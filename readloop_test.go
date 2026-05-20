package gnatrix

import (
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Gnatrix-Inc/gosdk/internal/wire"
)

// nopConn is a net.Conn stub for tests that exercise dispatch's
// session-fatal path. Close records that it was called; Read and
// Write are no-ops so the conn satisfies the interface but is never
// driven by I/O.
type nopConn struct {
	closed atomic.Bool
}

func (c *nopConn) Read(p []byte) (int, error)         { return 0, io.EOF }
func (c *nopConn) Write(p []byte) (int, error)        { return len(p), nil }
func (c *nopConn) Close() error                       { c.closed.Store(true); return nil }
func (c *nopConn) LocalAddr() net.Addr                { return nil }
func (c *nopConn) RemoteAddr() net.Addr               { return nil }
func (c *nopConn) SetDeadline(t time.Time) error      { return nil }
func (c *nopConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *nopConn) SetWriteDeadline(t time.Time) error { return nil }

// newDispatchTestClient builds a *Client wired to a nopConn so
// dispatch's sessionFatal path can run without panicking. It is not
// safe to call any I/O method on the returned client.
func newDispatchTestClient() *Client {
	return &Client{
		conn:        &nopConn{},
		readDone:    make(chan struct{}),
		terminating: make(chan struct{}),
	}
}

// encodedPayload returns the payload portion of a frame (the 8-byte
// header stripped).
func encodedPayload(frame []byte) []byte {
	return frame[8:]
}

// claimOrFail installs q via tryClaimQuery and fails the test if the
// claim is rejected. Used by tests that don't care about the CAS path
// because the client has no other in-flight query.
func claimOrFail(t *testing.T, c *Client, q *queryState) {
	t.Helper()
	if err := c.tryClaimQuery(q); err != nil {
		t.Fatalf("tryClaimQuery: unexpected error: %v", err)
	}
}

// newTestQueryState builds a queryState with all three channels
// initialized as buffered(1). Buffering keeps tests synchronous: a
// dispatch send completes immediately and the test inspects the
// channel without ceremony. Production uses unbuffered channels by
// design (see feedback_streaming_only.md).
func newTestQueryState(queryID uint64) *queryState {
	return &queryState{
		queryID: queryID,
		rows:    make(chan wire.QueryRowMsg, 1),
		end:     make(chan wire.QueryEndMsg, 1),
		reject:  make(chan *QueryRejectError, 1),
	}
}

func TestDispatch_QueryRow_RoutesToMatchingSlot(t *testing.T) {
	c := newDispatchTestClient()
	q := newTestQueryState(42)
	claimOrFail(t, c, q)

	payload := encodedPayload(wire.EncodeQueryRow(wire.QueryRowMsg{
		QueryID: 42,
		RowSeq:  0,
		RowJSON: `{"x":1}`,
	}))

	c.dispatch(wire.Header{Type: wire.FrameQueryRow, PayloadLen: uint32(len(payload))}, payload)

	select {
	case got := <-q.rows:
		if got.QueryID != 42 || got.RowSeq != 0 || got.RowJSON != `{"x":1}` {
			t.Errorf("unexpected row: %+v", got)
		}
	default:
		t.Fatal("row not delivered to slot")
	}
}

func TestDispatch_QueryRow_DroppedWhenNoActiveSlot(t *testing.T) {
	c := newDispatchTestClient()

	payload := encodedPayload(wire.EncodeQueryRow(wire.QueryRowMsg{QueryID: 42}))
	// No slot installed. Must not panic; nothing to verify on the
	// receiver side because there is no receiver. The contract is
	// "drops silently".
	c.dispatch(wire.Header{Type: wire.FrameQueryRow, PayloadLen: uint32(len(payload))}, payload)
}

func TestDispatch_QueryRow_DroppedWhenQueryIDMismatches(t *testing.T) {
	c := newDispatchTestClient()
	q := newTestQueryState(42)
	claimOrFail(t, c, q)

	// Row carries queryID=99 while the slot is for 42.
	payload := encodedPayload(wire.EncodeQueryRow(wire.QueryRowMsg{QueryID: 99}))
	c.dispatch(wire.Header{Type: wire.FrameQueryRow, PayloadLen: uint32(len(payload))}, payload)

	select {
	case msg := <-q.rows:
		t.Fatalf("row should have been dropped; got %+v", msg)
	default:
	}
}

func TestDispatch_QueryEnd_RoutesToMatchingSlot(t *testing.T) {
	c := newDispatchTestClient()
	q := newTestQueryState(42)
	claimOrFail(t, c, q)

	payload := encodedPayload(wire.EncodeQueryEnd(wire.QueryEndMsg{
		QueryID:      42,
		Status:       wire.QueryStatusOK,
		RowsReturned: 7,
	}))
	c.dispatch(wire.Header{Type: wire.FrameQueryEnd, PayloadLen: uint32(len(payload))}, payload)

	select {
	case got := <-q.end:
		if got.QueryID != 42 || got.Status != wire.QueryStatusOK || got.RowsReturned != 7 {
			t.Errorf("unexpected end: %+v", got)
		}
	default:
		t.Fatal("end not delivered to slot")
	}
}

func TestDispatch_QueryEnd_DroppedWhenQueryIDMismatches(t *testing.T) {
	c := newDispatchTestClient()
	q := newTestQueryState(42)
	claimOrFail(t, c, q)

	payload := encodedPayload(wire.EncodeQueryEnd(wire.QueryEndMsg{QueryID: 99}))
	c.dispatch(wire.Header{Type: wire.FrameQueryEnd, PayloadLen: uint32(len(payload))}, payload)

	select {
	case msg := <-q.end:
		t.Fatalf("end should have been dropped; got %+v", msg)
	default:
	}
}

// TestDispatch_QueryProgress_DecodedAndDiscarded verifies that a
// QUERY_PROGRESS for the active slot does not appear on either the
// rows or end channels. Progress callbacks are deferred to Slice 2.
func TestDispatch_QueryProgress_DecodedAndDiscarded(t *testing.T) {
	c := newDispatchTestClient()
	q := newTestQueryState(42)
	claimOrFail(t, c, q)

	payload := encodedPayload(wire.EncodeQueryProgress(wire.QueryProgressMsg{
		QueryID:       42,
		EventsScanned: 100,
	}))
	c.dispatch(wire.Header{Type: wire.FrameQueryProgress, PayloadLen: uint32(len(payload))}, payload)

	select {
	case msg := <-q.rows:
		t.Fatalf("progress leaked to rows: %+v", msg)
	case msg := <-q.end:
		t.Fatalf("progress leaked to end: %+v", msg)
	case <-time.After(20 * time.Millisecond):
	}
}

// TestDispatch_QueryRow_MalformedPayload_TriggersSessionFatal verifies
// that a garbage payload marks the session terminal and closes the
// conn, instead of silently dropping bytes that may have left the
// stream in an unknown state.
func TestDispatch_QueryRow_MalformedPayload_TriggersSessionFatal(t *testing.T) {
	c := newDispatchTestClient()
	q := newTestQueryState(42)
	claimOrFail(t, c, q)

	garbage := []byte{0xff, 0xff, 0xff}
	c.dispatch(wire.Header{Type: wire.FrameQueryRow, PayloadLen: uint32(len(garbage))}, garbage)

	select {
	case msg := <-q.rows:
		t.Fatalf("malformed row reached slot: %+v", msg)
	default:
	}
	assertSessionFatal(t, c, "malformed QUERY_ROW")
}

func TestTryClaimQuery_Lifecycle(t *testing.T) {
	c := newDispatchTestClient()

	if got := c.currentQuery(); got != nil {
		t.Errorf("initial currentQuery = %v; want nil", got)
	}

	q := &queryState{queryID: 1}
	if err := c.tryClaimQuery(q); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if got := c.currentQuery(); got != q {
		t.Errorf("after claim, currentQuery = %v; want %v", got, q)
	}

	c.clearActiveQuery()
	if got := c.currentQuery(); got != nil {
		t.Errorf("after clear, currentQuery = %v; want nil", got)
	}
}

// TestTryClaimQuery_RejectsWhenOccupied locks the core one-in-flight
// contract: a claim against a non-empty slot returns ErrQueryInFlight
// and leaves the original slot untouched.
func TestTryClaimQuery_RejectsWhenOccupied(t *testing.T) {
	c := newDispatchTestClient()

	first := &queryState{queryID: 1}
	if err := c.tryClaimQuery(first); err != nil {
		t.Fatalf("first claim: %v", err)
	}

	second := &queryState{queryID: 2}
	err := c.tryClaimQuery(second)
	if err != ErrQueryInFlight {
		t.Fatalf("second claim error = %v; want ErrQueryInFlight", err)
	}

	if got := c.currentQuery(); got != first {
		t.Errorf("currentQuery = %v; want %v (original)", got, first)
	}
}

// TestTryClaimQuery_AllowsReClaimAfterClear verifies that the slot is
// freed by clearActiveQuery and a subsequent claim succeeds.
func TestTryClaimQuery_AllowsReClaimAfterClear(t *testing.T) {
	c := newDispatchTestClient()

	q1 := &queryState{queryID: 1}
	if err := c.tryClaimQuery(q1); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	c.clearActiveQuery()

	q2 := &queryState{queryID: 2}
	if err := c.tryClaimQuery(q2); err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if got := c.currentQuery(); got != q2 {
		t.Errorf("currentQuery = %v; want %v", got, q2)
	}
}

// TestTryClaimQuery_ConcurrentClaims_OnlyOneWins fires N goroutines
// racing for the same empty slot and verifies that exactly one wins.
// This proves the CAS is atomic — without the mutex around the
// check-then-set, multiple goroutines could see activeQuery==nil and
// all install their slot, the last writer winning silently.
func TestTryClaimQuery_ConcurrentClaims_OnlyOneWins(t *testing.T) {
	const n = 20
	c := newDispatchTestClient()

	var (
		start    = make(chan struct{})
		wg       sync.WaitGroup
		mu       sync.Mutex
		wins     int
		inFlight int
	)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(id uint64) {
			defer wg.Done()
			<-start
			err := c.tryClaimQuery(&queryState{queryID: id})
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				wins++
			} else if err == ErrQueryInFlight {
				inFlight++
			} else {
				t.Errorf("unexpected error: %v", err)
			}
		}(uint64(i))
	}
	close(start)
	wg.Wait()

	if wins != 1 {
		t.Errorf("wins = %d; want exactly 1", wins)
	}
	if inFlight != n-1 {
		t.Errorf("ErrQueryInFlight count = %d; want %d", inFlight, n-1)
	}
	if c.currentQuery() == nil {
		t.Error("currentQuery is nil after a successful claim")
	}
}

// ---- ERROR routing (Slice 1.2.4) ---------------------------------------

func TestDispatch_Error_InRangeWithActiveQuery_RoutesToReject(t *testing.T) {
	c := newDispatchTestClient()
	q := newTestQueryState(42)
	claimOrFail(t, c, q)

	payload := encodedPayload(wire.EncodeError(wire.ErrorMsg{
		Code:    2014,
		Message: "unknown field 'usre'",
	}))
	c.dispatch(wire.Header{Type: wire.FrameError, PayloadLen: uint32(len(payload))}, payload)

	select {
	case rej := <-q.reject:
		if rej == nil {
			t.Fatal("reject channel got nil pointer")
		}
		if rej.Code != 2014 {
			t.Errorf("Code = %d; want 2014", rej.Code)
		}
		if rej.Message != "unknown field 'usre'" {
			t.Errorf("Message = %q; want %q", rej.Message, "unknown field 'usre'")
		}
	default:
		t.Fatal("reject channel got no value")
	}
}

// TestDispatch_Error_BoundaryCodes locks the inclusive endpoints of
// the query-reject range. A regression that uses < 2011 or > 2019
// would silently drop boundary errors.
func TestDispatch_Error_BoundaryCodes(t *testing.T) {
	for _, code := range []uint32{2011, 2019} {
		code := code
		t.Run(fmt.Sprintf("code_%d", code), func(t *testing.T) {
			c := newDispatchTestClient()
			q := newTestQueryState(42)
			claimOrFail(t, c, q)

			payload := encodedPayload(wire.EncodeError(wire.ErrorMsg{Code: code, Message: "x"}))
			c.dispatch(wire.Header{Type: wire.FrameError, PayloadLen: uint32(len(payload))}, payload)

			select {
			case rej := <-q.reject:
				if rej.Code != code {
					t.Errorf("Code = %d; want %d", rej.Code, code)
				}
			default:
				t.Fatalf("code %d not routed to reject", code)
			}
		})
	}
}

// TestDispatch_Error_OutOfRange_TriggersSessionFatal covers codes
// adjacent to the boundaries and well outside [2011, 2019]. None
// route to reject, and each marks the session terminal.
func TestDispatch_Error_OutOfRange_TriggersSessionFatal(t *testing.T) {
	for _, code := range []uint32{2010, 2020, 1001, 3001, 5001} {
		code := code
		t.Run(fmt.Sprintf("code_%d", code), func(t *testing.T) {
			c := newDispatchTestClient()
			q := newTestQueryState(42)
			claimOrFail(t, c, q)

			payload := encodedPayload(wire.EncodeError(wire.ErrorMsg{Code: code, Message: "x"}))
			c.dispatch(wire.Header{Type: wire.FrameError, PayloadLen: uint32(len(payload))}, payload)

			select {
			case rej := <-q.reject:
				t.Fatalf("out-of-range code %d leaked to reject as %+v", code, rej)
			default:
			}
			assertSessionFatal(t, c, fmt.Sprintf("session error %d", code))
		})
	}
}

// TestDispatch_Error_NoActiveQuery_TriggersSessionFatal verifies that
// an unsolicited ERROR — one received with no in-flight query —
// terminates the session rather than being silently dropped.
func TestDispatch_Error_NoActiveQuery_TriggersSessionFatal(t *testing.T) {
	c := newDispatchTestClient()

	payload := encodedPayload(wire.EncodeError(wire.ErrorMsg{Code: 2014, Message: "x"}))
	c.dispatch(wire.Header{Type: wire.FrameError, PayloadLen: uint32(len(payload))}, payload)

	assertSessionFatal(t, c, "unsolicited server error 2014")
}

func TestDispatch_Error_MalformedPayload_TriggersSessionFatal(t *testing.T) {
	c := newDispatchTestClient()
	q := newTestQueryState(42)
	claimOrFail(t, c, q)

	// Three 0xff bytes form an unterminated varint — DecodeError fails.
	garbage := []byte{0xff, 0xff, 0xff}
	c.dispatch(wire.Header{Type: wire.FrameError, PayloadLen: uint32(len(garbage))}, garbage)

	select {
	case rej := <-q.reject:
		t.Fatalf("malformed error reached reject: %+v", rej)
	default:
	}
	assertSessionFatal(t, c, "malformed ERROR")
}

// TestDispatch_QueryEnd_MalformedPayload_TriggersSessionFatal locks
// the symmetric contract for QUERY_END.
func TestDispatch_QueryEnd_MalformedPayload_TriggersSessionFatal(t *testing.T) {
	c := newDispatchTestClient()
	q := newTestQueryState(42)
	claimOrFail(t, c, q)

	garbage := []byte{0xff, 0xff, 0xff}
	c.dispatch(wire.Header{Type: wire.FrameQueryEnd, PayloadLen: uint32(len(garbage))}, garbage)

	select {
	case msg := <-q.end:
		t.Fatalf("malformed end reached slot: %+v", msg)
	default:
	}
	assertSessionFatal(t, c, "malformed QUERY_END")
}

// TestDispatch_QueryProgress_MalformedPayload_TriggersSessionFatal:
// progress is decoded only to validate; a corrupt PROGRESS still
// indicates a broken stream and terminates the session.
func TestDispatch_QueryProgress_MalformedPayload_TriggersSessionFatal(t *testing.T) {
	c := newDispatchTestClient()
	q := newTestQueryState(42)
	claimOrFail(t, c, q)

	garbage := []byte{0xff, 0xff, 0xff}
	c.dispatch(wire.Header{Type: wire.FrameQueryProgress, PayloadLen: uint32(len(garbage))}, garbage)

	assertSessionFatal(t, c, "malformed QUERY_PROGRESS")
}

// TestDispatch_PostFatal_IsNoOp verifies the early-out guard at the
// top of dispatch: once sessionFatal has fired, subsequent frames
// (which may have been buffered before conn.Close took effect) do
// not route anywhere.
func TestDispatch_PostFatal_IsNoOp(t *testing.T) {
	c := newDispatchTestClient()
	q := newTestQueryState(42)
	claimOrFail(t, c, q)

	// Trip the session into fatal state via an out-of-range ERROR.
	c.dispatch(wire.Header{Type: wire.FrameError, PayloadLen: 0xff},
		encodedPayload(wire.EncodeError(wire.ErrorMsg{Code: 5001, Message: "boom"})))
	assertSessionFatal(t, c, "5001")

	// A valid QUERY_ROW for the still-claimed slot must be ignored.
	rowPayload := encodedPayload(wire.EncodeQueryRow(wire.QueryRowMsg{QueryID: 42, RowJSON: "{}"}))
	c.dispatch(wire.Header{Type: wire.FrameQueryRow, PayloadLen: uint32(len(rowPayload))}, rowPayload)

	select {
	case msg := <-q.rows:
		t.Fatalf("row delivered after session-fatal: %+v", msg)
	default:
	}
}

// assertSessionFatal checks that sessionFatal recorded a terminal
// error containing the expected substring AND closed the conn.
func assertSessionFatal(t *testing.T, c *Client, wantSubstr string) {
	t.Helper()
	err := c.terminalErr()
	if err == nil {
		t.Fatal("terminalErr() = nil; want a session-fatal error")
	}
	if !contains(err.Error(), wantSubstr) {
		t.Errorf("terminalErr() = %q; want substring %q", err.Error(), wantSubstr)
	}
	nc, ok := c.conn.(*nopConn)
	if !ok {
		t.Fatalf("conn is not *nopConn (%T) — sessionFatal cannot be observed", c.conn)
	}
	if !nc.closed.Load() {
		t.Error("conn.Close was not called by sessionFatal")
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
