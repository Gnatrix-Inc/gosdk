package gnatrix

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/Gnatrix-Inc/gosdk/internal/wire"
)

// ErrStreamClosed is returned by QueryStream.Next when the stream was
// abandoned via Close before reaching a natural terminal (QUERY_END or
// pre-execution rejection). The background drainer continues consuming
// frames so the underlying session stays available for the next query.
var ErrStreamClosed = errors.New("gnatrix: query stream closed")

// QueryStream is the streaming result handle returned by Client.Query.
// Callers must drain Next to io.EOF (or any non-nil error) or call
// Close to release the in-flight slot on the *Client.
type QueryStream struct {
	client *Client
	state  *queryState

	// closeCh wakes Next when Close fires. iterDoneCh wakes the drainer
	// when Next observes a terminal frame on its own. Both close
	// channels are guarded by once-flags so multiple terminals (e.g.
	// Close racing the iterator) collapse to a single signal.
	closeCh      chan struct{}
	closeOnce    sync.Once
	iterDoneCh   chan struct{}
	iterDoneOnce sync.Once

	result *QueryResult
}

// Query issues a QUERY_REQUEST and returns a streaming handle. The
// queryID is generated internally as a monotonic counter starting at 1
// and is surfaced only via QueryResult.QueryID for log correlation.
//
// Returns ErrQueryInFlight (without writing any bytes to the wire) if
// another query is already active on this Client. The server enforces
// one in-flight query per session (ERROR 2019); the SDK enforces the
// same client-side to keep the contract explicit.
func (c *Client) Query(ctx context.Context, queryText string, opts QueryOptions) (*QueryStream, error) {
	indexName := opts.IndexName
	if indexName == "" {
		indexName = "default"
	}

	// rows is unbuffered: backpressure on a slow iterator propagates as
	// TCP backpressure on the socket (per feedback_streaming_only.md).
	// end and reject are buffered(1) because each carries at most one
	// terminal event — buffering avoids the dispatcher blocking on a
	// terminal delivery before the iterator (or drainer) gets to it.
	state := &queryState{
		rows:   make(chan wire.QueryRowMsg),
		end:    make(chan wire.QueryEndMsg, 1),
		reject: make(chan *QueryRejectError, 1),
	}

	if err := c.tryClaimQuery(state); err != nil {
		return nil, err
	}

	// Slot claimed. Allocate the queryID only now so a rejected Query
	// (ErrQueryInFlight) does not burn a counter value. The dispatcher
	// matches frames by state.queryID, so this assignment must happen
	// before the server could possibly reply — which it cannot until
	// the QUERY_REQUEST is written below.
	state.queryID = c.queryCounter.Add(1)

	req := wire.QueryRequestMsg{
		QueryID:            state.queryID,
		IndexName:          indexName,
		QueryText:          queryText,
		Limit:              opts.Limit,
		Cursor:             opts.Cursor,
		ProgressIntervalMs: opts.ProgressIntervalMs,
	}
	if opts.TimeRange != nil {
		req.HasTimeRange = true
		req.EarliestNs = opts.TimeRange.EarliestNs
		req.LatestNs = opts.TimeRange.LatestNs
	}

	c.mu.Lock()
	if err := c.terminalErr(); err != nil {
		c.mu.Unlock()
		c.clearActiveQuery()
		return nil, fmt.Errorf("gnatrix: query: %w", err)
	}
	if _, err := c.conn.Write(wire.EncodeQueryRequest(req)); err != nil {
		c.mu.Unlock()
		c.clearActiveQuery()
		return nil, fmt.Errorf("gnatrix: query write: %w", err)
	}
	c.mu.Unlock()

	// ctx is honored for the Query call itself by the eventual write
	// path (the conn does not currently respect ctx for writes, but the
	// caller can wrap the *Client externally). The stream's own
	// lifetime is independent of this ctx; per-Next ctx controls
	// per-row blocking.
	_ = ctx

	return &QueryStream{
		client:     c,
		state:      state,
		closeCh:    make(chan struct{}),
		iterDoneCh: make(chan struct{}),
	}, nil
}

// Next returns the next streamed row. Terminal conditions:
//
//   - io.EOF                  — QUERY_END status=0 (clean completion). Result() is valid.
//   - *QueryError             — QUERY_END with status != 0. Result() is valid (totals captured).
//   - *QueryRejectError       — server emitted ERROR with code in 2011..2019 (pre-execution). Result() returns nil.
//   - ErrStreamClosed         — Close was called before the stream terminated naturally.
//   - ctx.Err()               — the per-Next ctx was canceled or deadlined.
//   - wrapped terminalErr     — the read loop died (session-fatal); session is unusable.
//
// After any non-nil error from Next, the iterator is finished; further
// calls return the same kind of terminal (io.EOF or ErrStreamClosed).
func (s *QueryStream) Next(ctx context.Context) (Row, error) {
	// Fast-paths for already-terminal stream states. Both checks read
	// closed channels via non-blocking select.
	select {
	case <-s.closeCh:
		return nil, ErrStreamClosed
	default:
	}
	select {
	case <-s.iterDoneCh:
		return nil, io.EOF
	default:
	}

	select {
	case rowMsg := <-s.state.rows:
		var row Row
		if err := json.Unmarshal([]byte(rowMsg.RowJSON), &row); err != nil {
			// Server sent malformed JSON inside an otherwise valid
			// frame. The wire stream is still consistent; the iterator
			// surfaces the error to the caller, but does not clear the
			// slot. The caller is expected to call Close() to abandon
			// cleanly and free the slot.
			return nil, fmt.Errorf("gnatrix: row %d JSON: %w", rowMsg.RowSeq, err)
		}
		return row, nil

	case endMsg := <-s.state.end:
		s.result = buildQueryResult(endMsg)
		s.client.clearActiveQuery()
		s.markIterDone()
		if endMsg.Status != wire.QueryStatusOK {
			return nil, buildQueryError(endMsg)
		}
		return nil, io.EOF

	case rej := <-s.state.reject:
		s.client.clearActiveQuery()
		s.markIterDone()
		return nil, rej

	case <-s.closeCh:
		return nil, ErrStreamClosed

	case <-ctx.Done():
		return nil, ctx.Err()

	case <-s.client.readDone:
		s.client.clearActiveQuery()
		s.markIterDone()
		if cause := s.client.terminalErr(); cause != nil {
			return nil, fmt.Errorf("gnatrix: query stream: %w", cause)
		}
		return nil, errors.New("gnatrix: query stream: client closed")
	}
}

// Result returns the totals from QUERY_END. Valid only after Next has
// returned a terminal (io.EOF or any error). Panics if called before
// the stream is drained — that is a programmer error, not a runtime
// fault.
//
// Returns nil when the stream terminated via a pre-execution rejection
// (no QUERY_END was emitted, so there are no totals to report).
func (s *QueryStream) Result() *QueryResult {
	select {
	case <-s.iterDoneCh:
		return s.result
	default:
	}
	select {
	case <-s.closeCh:
		// Close was called and the drainer may or may not have observed
		// a terminal yet. Result is not guaranteed; treat the same as
		// "not drained".
	default:
	}
	panic("gnatrix: QueryStream.Result called before stream drained")
}

// Close abandons the stream and releases the in-flight slot on the
// *Client. Returns immediately; a background drainer consumes any
// frames the dispatcher still delivers for this query until QUERY_END
// or a pre-execution ERROR arrives, then clears the slot.
//
// Close is idempotent. If the stream has already terminated naturally
// (Next returned io.EOF or an error), Close is effectively a no-op —
// the drainer exits immediately via iterDoneCh.
func (s *QueryStream) Close() error {
	s.closeOnce.Do(func() {
		close(s.closeCh)
		go s.drain()
	})
	return nil
}

// drain consumes pending frames after Close. Exits when it observes a
// terminal (END/reject), or when the iterator beat it to the terminal
// (iterDoneCh), or when the session itself terminated (readDone).
// rows are discarded; the slot is cleared once the terminal is seen.
func (s *QueryStream) drain() {
	for {
		select {
		case <-s.state.rows:
			// discard
		case <-s.state.end:
			s.client.clearActiveQuery()
			return
		case <-s.state.reject:
			s.client.clearActiveQuery()
			return
		case <-s.iterDoneCh:
			// Iterator finished before drain needed to act. Slot was
			// already cleared by the iterator.
			return
		case <-s.client.readDone:
			s.client.clearActiveQuery()
			return
		}
	}
}

func (s *QueryStream) markIterDone() {
	s.iterDoneOnce.Do(func() {
		close(s.iterDoneCh)
	})
}

// buildQueryResult converts a wire QUERY_END message into the public
// QueryResult struct. Called when a terminal END frame is observed
// (regardless of status code — totals are useful even on failure).
func buildQueryResult(msg wire.QueryEndMsg) *QueryResult {
	return &QueryResult{
		QueryID:       msg.QueryID,
		RowsReturned:  msg.RowsReturned,
		EventsScanned: msg.EventsScanned,
		EventsMatched: msg.EventsMatched,
		ElapsedMs:     msg.ElapsedMs,
		Truncated:     msg.Truncated,
		NextCursor:    msg.NextCursor,
	}
}

// buildQueryError wraps a non-OK QUERY_END into the public QueryError
// type, parsing the EngineCode prefix when Status=2 (engine_error).
// The Status enum here is intentionally a uint32 to mirror the wire
// representation in QueryEndMsg.Status (uint64) narrowed to the small
// known status set.
func buildQueryError(msg wire.QueryEndMsg) *QueryError {
	e := &QueryError{
		Status:  uint32(msg.Status),
		Message: msg.Message,
		QueryID: msg.QueryID,
	}
	if msg.Status == wire.QueryStatusEngineError {
		e.EngineCode = parseEngineCode(msg.Message)
	}
	return e
}

// parseEngineCode extracts the leading numeric prefix from a QUERY_END
// engine_error message in the form "NNNN: msg" per wire-protocolv2.md
// §QUERY_END. Returns 0 if the format does not match or the number is
// out of uint32 range.
func parseEngineCode(msg string) uint32 {
	i := strings.IndexByte(msg, ':')
	if i <= 0 {
		return 0
	}
	n, err := strconv.Atoi(msg[:i])
	if err != nil || n < 0 {
		return 0
	}
	return uint32(n)
}
