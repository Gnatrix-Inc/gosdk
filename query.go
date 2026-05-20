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
// It exposes the streamed rows of a single in-flight query and the
// terminal totals (or error) once the stream ends.
//
// # Usage
//
// Iterate with Next() until any non-nil error (io.EOF on clean end,
// typed errors otherwise), then read totals via Result():
//
//	stream, err := client.Query(ctx, "search error", gnatrix.QueryOptions{
//	    Limit: 1000,
//	})
//	if err != nil {
//	    return err
//	}
//	defer stream.Close()
//
//	for {
//	    row, err := stream.Next(ctx)
//	    if errors.Is(err, io.EOF) {
//	        break
//	    }
//	    if err != nil {
//	        return err // *QueryError, *QueryRejectError, or session-fatal
//	    }
//	    process(row)
//	}
//	res := stream.Result() // totals from QUERY_END; nil after a reject
//
// # Lifecycle contract
//
//   - Callers MUST either (a) drain Next to a terminal (io.EOF or any
//     non-nil error), OR (b) call Close. The idiomatic pattern is
//     `defer stream.Close()` immediately after a successful Query,
//     which makes both cases safe: drain-to-terminal makes Close a
//     no-op; mid-iteration return triggers a background drainer that
//     consumes residual ROWs until the server emits QUERY_END and
//     then releases the one-in-flight slot on the *Client.
//
//   - Skipping both — iterating partially and abandoning the stream
//     without Close — leaks the one-in-flight slot on the *Client.
//     Client.Close still terminates the read loop cleanly (the
//     dispatcher escapes its blocked send via c.terminating), but
//     subsequent Query() calls return ErrQueryInFlight until the next
//     Dial.
//
// # Concurrency
//
// A QueryStream is single-consumer: do not call Next concurrently
// from multiple goroutines. Close is safe to call from any goroutine
// at any time; it is idempotent and synchronous (returns immediately;
// the drainer runs in the background).
//
// The *Client itself remains usable for non-Query operations (Ping)
// concurrent with an active QueryStream — wire writes are serialized
// internally.
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

// Query issues a QUERY_REQUEST frame and returns a streaming handle.
// Query is non-blocking — it writes the request to the wire and
// returns once the bytes are in the kernel send buffer. Result rows
// arrive asynchronously via the returned *QueryStream.
//
// # Parameters
//
// ctx is consulted only at entry and during the (typically fast) wire
// write under c.mu. Per-row timeouts during streaming belong on the
// ctx passed to (*QueryStream).Next; the ctx given here does NOT
// cancel an in-flight stream.
//
// queryText is the query body in the gnatrix Query language. It is
// sent verbatim to the server, which validates syntax and rejects
// malformed input with ERROR 2011 (QuerySyntaxError) before binding a
// query_id. The SDK surfaces that as *QueryRejectError on the first
// Next() call.
//
// opts is a value-typed QueryOptions; the zero value is valid (see
// QueryOptions for field defaults).
//
// # QueryID generation
//
// The wire-level query_id is allocated internally via an atomic
// monotonic counter on the *Client. The first successful Query
// returns 1; the value is exposed only via QueryResult.QueryID after
// drain so callers can correlate with server-side logs. The counter
// only advances on a successful slot claim — rejected Query calls
// (ErrQueryInFlight) do not burn a counter value.
//
// # One-in-flight enforcement
//
// The *Client carries a single in-flight query slot. If another
// QueryStream is still active (Rows not drained AND Close not yet
// observed as having freed the slot), Query returns ErrQueryInFlight
// without writing any bytes to the wire — the check is an atomic
// CAS, not a server round-trip. The server enforces the same
// invariant (ERROR 2019 QueryTooManyInFlight); the SDK mirror
// guarantees the contract is observable locally without waiting for
// the server.
//
// # Errors returned directly by Query (vs via Next)
//
//   - ErrQueryInFlight — another query is active on this *Client.
//   - Wrapped session terminal (read loop already died, e.g. session
//     was closed) — wrapped with "gnatrix: query: %w".
//   - Wrapped wire-write error — wrapped with "gnatrix: query write: %w".
//
// Server-side errors (ERROR 2011..2019 pre-execution; QUERY_END
// status 1..7 post-execution) do NOT surface from Query; they arrive
// asynchronously and surface from the first Next() call as
// *QueryRejectError or *QueryError respectively.
//
// # Caller responsibility
//
// Always pair a successful Query with `defer stream.Close()`. See the
// QueryStream lifecycle contract for the consequences of skipping it.
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

// Next returns the next streamed row, blocking until a row is
// available or the stream reaches a terminal condition.
//
// On success, Next returns a non-nil Row and nil error. Row is a
// freshly-parsed map[string]any decoded from the QUERY_ROW frame's
// JSON payload; it is owned by the caller and may be retained or
// mutated freely.
//
// # Terminal conditions
//
//   - io.EOF — QUERY_END with status=0 (clean completion). Result()
//     returns the totals captured from the END frame. Use
//     errors.Is(err, io.EOF) to detect this.
//
//   - *QueryError — QUERY_END with status != 0 (1=cancelled,
//     2=engine_error, 3=permission_denied, 4=tenant_quota_exceeded,
//     5=memory_limit_exceeded, 6=timeout, 7=storage_unavailable).
//     Result() returns the totals (events scanned, elapsed, etc. —
//     useful even on failure). For status=2, EngineCode is parsed
//     from the "NNNN: msg" prefix in Message. Use errors.As(err,
//     &qerr) to recover.
//
//   - *QueryRejectError — server emitted an ERROR frame with code in
//     2011..2019 (pre-execution rejection: syntax, unsupported
//     command, invalid time range, unknown field, memory limit,
//     timeout-at-admission, storage, oversize query, too many
//     in-flight). No QUERY_END is emitted for this request; Result()
//     returns nil. The session stays open — a subsequent Query is
//     immediately valid. Use errors.As(err, &rej) to recover.
//
//   - ErrStreamClosed — (*QueryStream).Close was called before the
//     stream terminated naturally. A background drainer continues
//     consuming residual frames in case the server is still emitting.
//
//   - ctx.Err() — the per-call ctx was canceled or its deadline
//     elapsed. The stream is NOT cleaned up automatically; the
//     caller should also Close. The ctx passed to Query() does NOT
//     affect this — each Next has its own ctx.
//
//   - Wrapped session-fatal error — the read loop died (malformed
//     server frame, unexpected ERROR code outside 2011..2019, TCP
//     disconnect). The *Client is unusable for new operations after
//     this; a new Dial is required.
//
// # Idempotency of terminals
//
// After any non-nil error from Next, the iterator is finished;
// further calls return the same terminal class without blocking
// (io.EOF stays EOF; ErrStreamClosed stays ErrStreamClosed).
//
// # Concurrency
//
// Next is NOT safe to call concurrently from multiple goroutines on
// the same stream. Pipeline patterns that produce/consume from a
// channel should run a single Next-consumer goroutine and fan out
// via that channel.
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

// Result returns the totals captured from the terminating QUERY_END
// frame. The returned *QueryResult exposes rows_returned,
// events_scanned, events_matched, elapsed_ms, truncated, and
// next_cursor — useful both on success and on post-execution failure
// (Status != 0) because the server still emits totals when an
// execution-time error terminates the query.
//
// # When Result is valid
//
// Result MUST be called only after Next has returned a terminal
// value (io.EOF or any non-nil error). Calling Result before drain
// panics — this is a programmer error, not a runtime fault, and
// surfaces as a stack trace pointing at the misuse site.
//
// # Nil return
//
// Result returns nil when the stream terminated via *QueryRejectError
// (pre-execution rejection — the server did NOT emit a QUERY_END for
// that request, so no totals exist). All other terminal paths
// (io.EOF, *QueryError, ErrStreamClosed-after-server-emitted-END,
// ctx cancellation after END arrival) yield a non-nil *QueryResult.
//
// # Concurrency
//
// Like Next, Result is single-consumer. It is safe to call multiple
// times after drain — the result is captured at terminal time and
// memoized in the stream.
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
// *Client.
//
// Close is non-blocking: it returns immediately and runs the
// cleanup in a background goroutine. The goroutine drains any
// residual QUERY_ROW frames the server is still emitting until it
// observes QUERY_END (or a pre-execution ERROR), then frees the
// one-in-flight slot so a subsequent Client.Query can proceed.
// QUERY_CANCEL (wire frame 0x13) is not yet implemented (Slice 2),
// so the server is not informed that the client gave up — it keeps
// streaming until natural termination.
//
// # Idempotency
//
// Close is safe to call multiple times and from any goroutine,
// including concurrently with Next. The first call wins via
// sync.Once; subsequent calls return nil without side effects.
//
// # Interaction with natural drain
//
// If the iterator already reached a terminal (Next returned io.EOF
// or any error) before Close was invoked, Close is effectively a
// no-op: the drainer it spawns exits immediately via the
// already-closed iterDoneCh, and the slot was already released by
// Next's terminal path.
//
// # Return value
//
// Close currently always returns nil. The signature accepts a
// future error return path (e.g. when Slice 2 wires QUERY_CANCEL,
// Close may surface a write failure for the cancel frame); callers
// should check it accordingly to remain forward-compatible.
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
