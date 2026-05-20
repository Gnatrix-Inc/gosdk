package gnatrix

import (
	"bytes"
	"fmt"
	"time"

	"github.com/Gnatrix-Inc/gosdk/internal/wire"
)

// queryState is the dispatcher-facing handle for a single in-flight
// query. Exactly one queryState exists on a *Client between Query() and
// the consumption of QUERY_END (or stream abandonment via Close).
//
// Channels are unbuffered on purpose: a slow reader on the iterator
// side propagates as TCP backpressure on the socket, which is the
// SDK's only throughput control by design.
type queryState struct {
	queryID uint64
	rows    chan wire.QueryRowMsg
	end     chan wire.QueryEndMsg
	// reject receives pre-execution server rejections (ERROR frames
	// with code in 2011..2019). Mutually exclusive with end per
	// wire-protocolv2.md §"Frame ordering": for any accepted
	// QUERY_REQUEST, the server emits exactly one QUERY_END or
	// exactly one ERROR — never both.
	reject chan *QueryRejectError
}

// readLoop is the long-running reader goroutine started after WELCOME.
// It is the sole owner of c.conn.Read; every other operation
// (Ping, future Query) writes to the conn and registers a waiter for
// the matching response frame.
//
// The loop exits when ReadHeader or ReadPayload returns an error —
// typically because Close closed the conn. The terminating error is
// recorded in c.readErr and c.readDone is closed so blocked waiters
// can unblock.
func (c *Client) readLoop() {
	defer close(c.readDone)
	for {
		hdr, err := wire.ReadHeader(c.conn)
		if err != nil {
			c.setReadErr(err)
			return
		}
		payload, err := wire.ReadPayload(c.conn, hdr.PayloadLen)
		if err != nil {
			c.setReadErr(fmt.Errorf("gnatrix: read payload: %w", err))
			return
		}
		c.dispatch(hdr, payload)
	}
}

// dispatch routes a fully-read frame to the appropriate waiter. Some
// failure modes (malformed payloads, unexpected ERROR codes, ERROR
// without an active query) trigger sessionFatal — the connection is
// closed, readErr is recorded, and any waiter unblocks via readDone.
//
// dispatch checks terminalErr at entry: once sessionFatal has fired,
// any frames the read loop still picks up from the TCP/TLS receive
// buffer are silently ignored. Unknown frame types are dropped for
// forward compatibility with newer servers.
func (c *Client) dispatch(hdr wire.Header, payload []byte) {
	if c.terminalErr() != nil {
		return
	}
	switch hdr.Type {
	case wire.FramePong:
		if hdr.PayloadLen == 0 {
			c.deliverPong(time.Now())
		}
	case wire.FrameQueryRow:
		msg, err := wire.DecodeQueryRow(bytes.NewReader(payload))
		if err != nil {
			c.sessionFatal(fmt.Errorf("gnatrix: malformed QUERY_ROW: %w", err))
			return
		}
		c.deliverQueryRow(msg)
	case wire.FrameQueryEnd:
		msg, err := wire.DecodeQueryEnd(bytes.NewReader(payload))
		if err != nil {
			c.sessionFatal(fmt.Errorf("gnatrix: malformed QUERY_END: %w", err))
			return
		}
		c.deliverQueryEnd(msg)
	case wire.FrameQueryProgress:
		// Progress callbacks are deferred to Slice 2; the value is
		// discarded. A malformed PROGRESS still indicates a corrupt
		// stream and is treated as session-fatal for consistency with
		// the other QUERY_* frames.
		if _, err := wire.DecodeQueryProgress(bytes.NewReader(payload)); err != nil {
			c.sessionFatal(fmt.Errorf("gnatrix: malformed QUERY_PROGRESS: %w", err))
		}
	case wire.FrameError:
		msg, err := wire.DecodeError(bytes.NewReader(payload))
		if err != nil {
			c.sessionFatal(fmt.Errorf("gnatrix: malformed ERROR: %w", err))
			return
		}
		c.deliverError(msg)
	}
}

// tryClaimQuery installs q as the in-flight query slot iff no other
// query is currently active. Returns ErrQueryInFlight if a slot is
// already claimed. Called by Client.Query (Slice 1.3) before writing
// the QUERY_REQUEST frame — when this returns an error, no bytes hit
// the wire. The check and the set are atomic under waiterMu so
// concurrent Query() calls cannot both succeed.
func (c *Client) tryClaimQuery(q *queryState) error {
	c.waiterMu.Lock()
	defer c.waiterMu.Unlock()
	if c.activeQuery != nil {
		return ErrQueryInFlight
	}
	c.activeQuery = q
	return nil
}

// clearActiveQuery releases the in-flight slot. Called by Client.Query's
// stream consumer (Slice 1.3) after observing QUERY_END or abandoning
// the stream via Close.
func (c *Client) clearActiveQuery() {
	c.waiterMu.Lock()
	c.activeQuery = nil
	c.waiterMu.Unlock()
}

// currentQuery returns the in-flight query slot, or nil if none.
func (c *Client) currentQuery() *queryState {
	c.waiterMu.Lock()
	defer c.waiterMu.Unlock()
	return c.activeQuery
}

// deliverQueryRow sends msg to the in-flight query's rows channel.
// Drops the row if there is no active query or the queryID does not
// match (defensive — server should never send rows for a query the
// client did not request).
//
// The send is blocking: if the iterator caller is not draining, the
// dispatcher stalls here, TCP receive buffer fills, and the server
// applies natural backpressure. This is the SDK's only throughput
// control by design (see feedback_streaming_only.md).
func (c *Client) deliverQueryRow(msg wire.QueryRowMsg) {
	q := c.currentQuery()
	if q == nil || q.queryID != msg.QueryID {
		return
	}
	q.rows <- msg
}

// deliverQueryEnd sends msg to the in-flight query's end channel.
// Drops if there is no active query or the queryID does not match.
// The slot itself is not cleared here — the caller in Slice 1.3 is
// responsible for clearing after observing QUERY_END.
func (c *Client) deliverQueryEnd(msg wire.QueryEndMsg) {
	q := c.currentQuery()
	if q == nil || q.queryID != msg.QueryID {
		return
	}
	q.end <- msg
}

// deliverError routes an ERROR frame to the in-flight query as a
// pre-execution rejection (wire-protocolv2.md §QUERY_REQUEST states
// that codes 2011..2019 are emitted when the server rejects a query
// before binding a query_id). ERROR frames do not carry a query_id,
// so correlation relies on the one-in-flight invariant.
//
// Session-fatal when:
//   - no active query (a server-level ERROR with no query context is
//     either an unsolicited auth/rate/internal error or a protocol
//     violation — the session is unrecoverable)
//   - code outside the query-reject range [2011, 2019] (1xxx framing,
//     2001..2010 auth post-handshake, 3xxx rate-limit, 5xxx internal —
//     all indicate the session cannot continue)
func (c *Client) deliverError(msg wire.ErrorMsg) {
	q := c.currentQuery()
	if q == nil {
		c.sessionFatal(fmt.Errorf("gnatrix: unsolicited server error %d: %s", msg.Code, msg.Message))
		return
	}
	if msg.Code < 2011 || msg.Code > 2019 {
		c.sessionFatal(fmt.Errorf("gnatrix: session error %d: %s", msg.Code, msg.Message))
		return
	}
	q.reject <- &QueryRejectError{Code: msg.Code, Message: msg.Message}
}

// sessionFatal records a terminal cause for the read loop and closes
// the underlying conn so any waiter (Ping, future Query) unblocks via
// readDone. Both the cause and the close are idempotent:
//   - setReadErr stores only the first non-nil error, so the original
//     fault is preserved across cascading failures.
//   - The conn close goes through Client.closeOnce, so a subsequent
//     user-initiated Close is a no-op and a concurrent close from
//     either path does not double-close.
func (c *Client) sessionFatal(err error) {
	c.setReadErr(err)
	c.closeOnce.Do(func() {
		c.closeErr = c.conn.Close()
	})
}

// deliverPong wakes any goroutine waiting on a PONG. Non-blocking: if
// the waiter channel is full or nil, the PONG is dropped.
func (c *Client) deliverPong(t time.Time) {
	c.waiterMu.Lock()
	w := c.pingWaiter
	c.waiterMu.Unlock()
	if w == nil {
		return
	}
	select {
	case w <- t:
	default:
	}
}

// setReadErr records the first terminating error of the read loop.
// Subsequent calls are ignored so the original cause is preserved.
func (c *Client) setReadErr(err error) {
	c.waiterMu.Lock()
	if c.readErr == nil {
		c.readErr = err
	}
	c.waiterMu.Unlock()
}

// terminalErr returns the read loop's exit cause, or nil if the loop
// is still running.
func (c *Client) terminalErr() error {
	c.waiterMu.Lock()
	defer c.waiterMu.Unlock()
	return c.readErr
}
