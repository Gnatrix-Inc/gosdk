package gnatrix

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Gnatrix-Inc/gosdk/internal/wire"
	"go.uber.org/goleak"
)

// queryFixture wires a *Client to one end of a net.Pipe and exposes
// the other end for byte-level injection by tests. The readLoop runs
// on the client conn so frames written to the server conn flow into
// the dispatcher as if they came from a real gnatrixquery peer.
//
// Per feedback_no_query_mocks.md, this is not a fake server: there is
// no per-frame handler logic, just raw bytes on the wire.
type queryFixture struct {
	t      *testing.T
	client *Client
	server net.Conn
}

func newQueryFixture(t *testing.T) *queryFixture {
	t.Helper()
	cConn, sConn := net.Pipe()
	c := &Client{
		conn:        cConn,
		readDone:    make(chan struct{}),
		terminating: make(chan struct{}),
	}
	go c.readLoop()
	t.Cleanup(func() {
		_ = c.Close()
		_ = sConn.Close()
	})
	return &queryFixture{t: t, client: c, server: sConn}
}

// readRequest consumes one QUERY_REQUEST frame off the server end of
// the pipe and returns the decoded message.
func (f *queryFixture) readRequest() wire.QueryRequestMsg {
	f.t.Helper()
	hdr, err := wire.ReadHeader(f.server)
	if err != nil {
		f.t.Fatalf("readRequest header: %v", err)
	}
	if hdr.Type != wire.FrameQueryRequest {
		f.t.Fatalf("readRequest type = %v; want FrameQueryRequest", hdr.Type)
	}
	payload, err := wire.ReadPayload(f.server, hdr.PayloadLen)
	if err != nil {
		f.t.Fatalf("readRequest payload: %v", err)
	}
	msg, err := wire.DecodeQueryRequest(bytes.NewReader(payload))
	if err != nil {
		f.t.Fatalf("readRequest decode: %v", err)
	}
	return msg
}

func (f *queryFixture) writeFrame(frame []byte) {
	f.t.Helper()
	if _, err := f.server.Write(frame); err != nil {
		f.t.Fatalf("writeFrame: %v", err)
	}
}

// issueQuery runs Client.Query in a goroutine (so the synchronous pipe
// write does not deadlock), drains the request from the server side,
// and returns the resulting stream plus the decoded request.
func (f *queryFixture) issueQuery(text string, opts QueryOptions) (*QueryStream, wire.QueryRequestMsg, error) {
	type result struct {
		stream *QueryStream
		err    error
	}
	ch := make(chan result, 1)
	go func() {
		s, err := f.client.Query(context.Background(), text, opts)
		ch <- result{s, err}
	}()
	req := f.readRequest()
	r := <-ch
	return r.stream, req, r.err
}

// ---- Happy path -------------------------------------------------------

func TestClientQuery_HappyPath_StreamsRowsAndCompletes(t *testing.T) {
	f := newQueryFixture(t)

	stream, req, err := f.issueQuery("search error", QueryOptions{IndexName: "logs-2026"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if req.QueryID != 1 {
		t.Errorf("req.QueryID = %d; want 1 (monotonic counter starts at 1)", req.QueryID)
	}
	if req.QueryText != "search error" {
		t.Errorf("req.QueryText = %q; want %q", req.QueryText, "search error")
	}
	if req.IndexName != "logs-2026" {
		t.Errorf("req.IndexName = %q; want %q", req.IndexName, "logs-2026")
	}

	// Producer runs in its own goroutine. The rows channel is
	// unbuffered, so each row write blocks the dispatcher on
	// `state.rows <- msg` until the test goroutine calls Next.
	// Writing multiple rows synchronously from the test goroutine
	// would deadlock since the second pipe write cannot complete
	// while readLoop is stuck inside the previous dispatch.
	go func() {
		f.writeFrame(wire.EncodeQueryRow(wire.QueryRowMsg{
			QueryID: req.QueryID, RowSeq: 0, RowJSON: `{"a":1}`,
		}))
		f.writeFrame(wire.EncodeQueryRow(wire.QueryRowMsg{
			QueryID: req.QueryID, RowSeq: 1, RowJSON: `{"a":2}`,
		}))
		f.writeFrame(wire.EncodeQueryEnd(wire.QueryEndMsg{
			QueryID:       req.QueryID,
			Status:        wire.QueryStatusOK,
			RowsReturned:  2,
			EventsScanned: 100,
			EventsMatched: 2,
			ElapsedMs:     50,
			NextCursor:    "next",
		}))
	}()

	ctx := context.Background()

	row1, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("Next #1: %v", err)
	}
	if row1["a"] != float64(1) {
		t.Errorf("row1[a] = %v; want 1", row1["a"])
	}

	row2, err := stream.Next(ctx)
	if err != nil {
		t.Fatalf("Next #2: %v", err)
	}
	if row2["a"] != float64(2) {
		t.Errorf("row2[a] = %v; want 2", row2["a"])
	}

	if _, err := stream.Next(ctx); !errors.Is(err, io.EOF) {
		t.Errorf("final Next: got %v; want io.EOF", err)
	}

	res := stream.Result()
	if res == nil {
		t.Fatal("Result() = nil; want non-nil after EOF")
	}
	if res.RowsReturned != 2 || res.EventsScanned != 100 || res.NextCursor != "next" {
		t.Errorf("Result = %+v", res)
	}
}

// IndexName default fills in "default" when caller leaves it empty.
func TestClientQuery_DefaultIndexName(t *testing.T) {
	f := newQueryFixture(t)
	_, req, err := f.issueQuery("q", QueryOptions{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if req.IndexName != "default" {
		t.Errorf("req.IndexName = %q; want \"default\"", req.IndexName)
	}
}

// TimeRange option flows through to the wire frame.
func TestClientQuery_TimeRangeFlowsToWire(t *testing.T) {
	f := newQueryFixture(t)
	tr := &TimeRange{EarliestNs: 1_000_000_000, LatestNs: 2_000_000_000}
	_, req, err := f.issueQuery("q", QueryOptions{TimeRange: tr})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if !req.HasTimeRange {
		t.Fatal("req.HasTimeRange = false; want true")
	}
	if req.EarliestNs != 1_000_000_000 || req.LatestNs != 2_000_000_000 {
		t.Errorf("time range = (%d, %d)", req.EarliestNs, req.LatestNs)
	}
}

// ---- Pre-execution rejection ------------------------------------------

func TestClientQuery_PreExecutionReject_AsQueryRejectError(t *testing.T) {
	f := newQueryFixture(t)
	stream, _, err := f.issueQuery("bad query", QueryOptions{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	f.writeFrame(wire.EncodeError(wire.ErrorMsg{
		Code: 2014, Message: "unknown field 'usre'",
	}))

	_, err = stream.Next(context.Background())
	var rej *QueryRejectError
	if !errors.As(err, &rej) {
		t.Fatalf("Next: got %T %v; want *QueryRejectError", err, err)
	}
	if rej.Code != 2014 || rej.Message != "unknown field 'usre'" {
		t.Errorf("reject = %+v", rej)
	}
	if stream.Result() != nil {
		t.Errorf("Result() after reject = %+v; want nil", stream.Result())
	}
}

// TestClientQuery_SyntaxErrorReject_KeepsSessionAlive specifically
// pins ERROR code 2011 (QuerySyntaxError per wire-protocolv2.md
// §ERROR) and verifies the invariant the spec calls out in §"Frame
// ordering" (pre-execution rejection):
//
//	C: QUERY_REQUEST
//	S: ERROR (2011 / 2012 / 2013 / 2018 / 2019)
//	← session stays open, no QUERY_END for this request
//
// The sibling TestClientQuery_PreExecutionReject_AsQueryRejectError
// covers the typed-error path with code 2014. This test layers two
// stronger assertions on top:
//
//  1. Code 2011 surfaces verbatim as *QueryRejectError{Code:2011}.
//  2. After the reject, a follow-up Query() claims the slot without
//     ErrQueryInFlight and runs to a clean io.EOF — the session was
//     not poisoned by the rejected request.
//
// QueryID monotonicity is also verified: the rejected query used
// queryID=1 (counter advanced on a successful claim), so the next
// successful query uses queryID=2, not 3 — proving the rejected
// attempt did not burn an extra counter value either.
func TestClientQuery_SyntaxErrorReject_KeepsSessionAlive(t *testing.T) {
	f := newQueryFixture(t)

	// First query: server replies with ERROR 2011 (syntax).
	stream1, req1, err := f.issueQuery("badly !! parsed", QueryOptions{})
	if err != nil {
		t.Fatalf("Query #1: %v", err)
	}
	if req1.QueryID != 1 {
		t.Errorf("req1.QueryID = %d; want 1 (counter starts at 1)", req1.QueryID)
	}

	f.writeFrame(wire.EncodeError(wire.ErrorMsg{
		Code:    2011,
		Message: "syntax error at column 7: unexpected '!!'",
	}))

	_, err = stream1.Next(context.Background())
	var rej *QueryRejectError
	if !errors.As(err, &rej) {
		t.Fatalf("Next: got %T %v; want *QueryRejectError", err, err)
	}
	if rej.Code != 2011 {
		t.Errorf("Code = %d; want 2011 (QuerySyntaxError)", rej.Code)
	}
	if rej.Message != "syntax error at column 7: unexpected '!!'" {
		t.Errorf("Message = %q", rej.Message)
	}
	if stream1.Result() != nil {
		t.Errorf("Result() after reject = %+v; want nil (no QUERY_END was emitted)", stream1.Result())
	}

	// Critical invariant: the session is still usable. A follow-up
	// Query must claim the slot (no ErrQueryInFlight, because the
	// rejected stream's Next() called clearActiveQuery) and complete
	// cleanly.
	stream2, req2, err := f.issueQuery("valid query", QueryOptions{})
	if err != nil {
		t.Fatalf("Query #2 (after syntax-error reject): %v", err)
	}
	if req2.QueryID != 2 {
		t.Errorf("req2.QueryID = %d; want 2 (counter advanced; rejected Query #1 used qid=1)", req2.QueryID)
	}
	f.writeFrame(wire.EncodeQueryEnd(wire.QueryEndMsg{
		QueryID: 2,
		Status:  wire.QueryStatusOK,
	}))
	if _, err := stream2.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("stream2 final Next: got %v; want io.EOF", err)
	}
}

// TestClientQuery_EmptyQueryText_SurfaceAsSyntaxReject documents the
// behavior when the caller passes an empty queryText. The SDK does
// NOT validate client-side (consistent with the "no duplicate
// pre-validation" rule in CLAUDE.md — the server is the source of
// truth for query syntax). Specifically:
//
//  1. Query() returns *QueryStream, nil — no synchronous error.
//  2. The wire frame carries QueryText="" verbatim (lenstr 0x00 in
//     that position). The test asserts req.QueryText == "" decoded
//     from the captured bytes, proving no client-side filtering.
//  3. The real server rejects the empty query with ERROR 2011
//     (QuerySyntaxError) before binding a query_id. The test
//     simulates that response.
//  4. stream.Next() returns *QueryRejectError{Code: 2011}.
//  5. stream.Result() returns nil (no QUERY_END was emitted).
//
// Sibling test TestClientQuery_SyntaxErrorReject_KeepsSessionAlive
// covers the session-alive invariant; this test focuses on
// confirming the empty-string-is-not-filtered behavior + the typical
// server surface.
func TestClientQuery_EmptyQueryText_SurfaceAsSyntaxReject(t *testing.T) {
	f := newQueryFixture(t)

	stream, req, err := f.issueQuery("", QueryOptions{})
	if err != nil {
		t.Fatalf("Query (empty queryText): unexpected error: %v", err)
	}
	if req.QueryText != "" {
		t.Errorf("req.QueryText = %q; want \"\" (SDK must not filter empty queries — server is the validator)", req.QueryText)
	}
	if req.QueryID != 1 {
		t.Errorf("req.QueryID = %d; want 1 (counter starts at 1, must advance even for empty queryText)", req.QueryID)
	}

	// Simulate the server's response: ERROR 2011 with a message that
	// reflects the empty-query rejection. The wire-protocol spec does
	// not pin the exact server message text — only the code.
	f.writeFrame(wire.EncodeError(wire.ErrorMsg{
		Code:    2011,
		Message: "empty query",
	}))

	_, err = stream.Next(context.Background())
	var rej *QueryRejectError
	if !errors.As(err, &rej) {
		t.Fatalf("Next: got %T %v; want *QueryRejectError", err, err)
	}
	if rej.Code != 2011 {
		t.Errorf("Code = %d; want 2011 (QuerySyntaxError)", rej.Code)
	}
	if rej.Message != "empty query" {
		t.Errorf("Message = %q; want %q", rej.Message, "empty query")
	}
	if stream.Result() != nil {
		t.Errorf("Result() after reject = %+v; want nil (no QUERY_END was emitted)", stream.Result())
	}
}

// ---- Post-execution errors --------------------------------------------

func TestClientQuery_PostExecutionTimeout_AsQueryError(t *testing.T) {
	f := newQueryFixture(t)
	stream, _, err := f.issueQuery("slow query", QueryOptions{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	f.writeFrame(wire.EncodeQueryEnd(wire.QueryEndMsg{
		QueryID:   1,
		Status:    wire.QueryStatusTimeout,
		ElapsedMs: 30000,
		Message:   "query timeout after 30s",
	}))

	_, err = stream.Next(context.Background())
	var qerr *QueryError
	if !errors.As(err, &qerr) {
		t.Fatalf("Next: got %T %v; want *QueryError", err, err)
	}
	if qerr.Status != uint32(wire.QueryStatusTimeout) {
		t.Errorf("Status = %d; want %d", qerr.Status, wire.QueryStatusTimeout)
	}
	if qerr.EngineCode != 0 {
		t.Errorf("EngineCode = %d; want 0 (only Status=2 parses code)", qerr.EngineCode)
	}
	res := stream.Result()
	if res == nil || res.ElapsedMs != 30000 {
		t.Errorf("Result = %+v; want ElapsedMs=30000", res)
	}
}

func TestClientQuery_EngineError_ParsesEngineCodeFromPrefix(t *testing.T) {
	f := newQueryFixture(t)
	stream, _, err := f.issueQuery("x", QueryOptions{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	f.writeFrame(wire.EncodeQueryEnd(wire.QueryEndMsg{
		QueryID: 1,
		Status:  wire.QueryStatusEngineError,
		Message: "2014: unknown field 'usre'",
	}))

	_, err = stream.Next(context.Background())
	var qerr *QueryError
	if !errors.As(err, &qerr) {
		t.Fatalf("Next: got %T %v; want *QueryError", err, err)
	}
	if qerr.EngineCode != 2014 {
		t.Errorf("EngineCode = %d; want 2014", qerr.EngineCode)
	}
}

// TestClientQuery_OversizedQueryText_ReturnsErrQueryTooLarge seals
// the wire-size pre-check in Client.Query: a queryText longer than
// the wire MaxPayload (65 536 bytes) is rejected synchronously,
// before any state allocation, slot claim, or counter increment.
// The *Client is left in a pristine state — a follow-up Query with
// a sane size must claim slot #1 (not #2), proving the rejected
// call did not burn a queryID value.
//
// Without this check, the SDK would emit a frame with payload_len >
// MaxPayload to the wire; the server rejects that as ERROR 1001
// InvalidFrame and closes the session, killing the *Client. This
// test verifies the rescue is in place.
func TestClientQuery_OversizedQueryText_ReturnsErrQueryTooLarge(t *testing.T) {
	f := newQueryFixture(t)

	// Exactly one byte over the limit. The constant 65536 is the wire
	// MaxPayload; queryText alone of size 65537 cannot fit in a
	// single frame regardless of the other fields' overhead.
	oversized := strings.Repeat("a", 65537)

	stream, err := f.client.Query(context.Background(), oversized, QueryOptions{})
	if stream != nil {
		t.Errorf("Query returned non-nil stream alongside the error; want nil")
	}
	if !errors.Is(err, ErrQueryTooLarge) {
		t.Fatalf("Query error = %v; want ErrQueryTooLarge", err)
	}

	// Side-effect check: slot must NOT be claimed, queryCounter must
	// NOT have advanced. A follow-up Query with a sane size must
	// proceed normally with queryID=1.
	if f.client.currentQuery() != nil {
		t.Error("currentQuery is non-nil after ErrQueryTooLarge — slot leaked")
	}

	stream2, req2, err := f.issueQuery("ok", QueryOptions{})
	if err != nil {
		t.Fatalf("follow-up Query: %v", err)
	}
	if req2.QueryID != 1 {
		t.Errorf("req2.QueryID = %d; want 1 (counter must not advance on rejected oversize call)", req2.QueryID)
	}
	f.writeFrame(wire.EncodeQueryEnd(wire.QueryEndMsg{QueryID: 1, Status: wire.QueryStatusOK}))
	if _, err := stream2.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("stream2 final Next: %v", err)
	}
}

// TestClient_NoGoroutineLeakAfter100Queries seals the lifecycle of
// the *Client and *QueryStream goroutines under repeated use: 100
// queries each drained to io.EOF, each Closed, then Client.Close —
// no goroutine spawned by the SDK survives.
//
// goleak.VerifyNone runs after t returns; it diffs the goroutine
// list against a baseline taken at test entry and fails if any
// extra goroutine remains. A regression that leaks the drainer
// (e.g. a drain() that does not observe iterDoneCh) would surface
// here as N=100 leaked goroutines with identical stack traces.
//
// The previous probe used runtime.NumGoroutine() which counts only
// — goleak additionally prints the stack of each leaked goroutine
// so the culprit is obvious.
func TestClient_NoGoroutineLeakAfter100Queries(t *testing.T) {
	defer goleak.VerifyNone(t)

	f := newQueryFixture(t)

	for i := 0; i < 100; i++ {
		stream, req, err := f.issueQuery("q", QueryOptions{})
		if err != nil {
			t.Fatalf("iter %d Query: %v", i, err)
		}
		f.writeFrame(wire.EncodeQueryEnd(wire.QueryEndMsg{
			QueryID: req.QueryID,
			Status:  wire.QueryStatusOK,
		}))
		if _, err := stream.Next(context.Background()); !errors.Is(err, io.EOF) {
			t.Fatalf("iter %d Next: %v", i, err)
		}
		if err := stream.Close(); err != nil {
			t.Fatalf("iter %d Close: %v", i, err)
		}
	}

	// Explicit Client.Close to terminate the read loop. The fixture's
	// t.Cleanup also calls Close (sync.Once-guarded), so the extra
	// call is a no-op but makes the test's intent clear.
	if err := f.client.Close(); err != nil {
		t.Fatalf("Client.Close: %v", err)
	}
}

// ---- One-in-flight enforcement ----------------------------------------

func TestClientQuery_SecondQueryWhileFirstInFlight_ReturnsErrQueryInFlight(t *testing.T) {
	f := newQueryFixture(t)

	stream1, _, err := f.issueQuery("first", QueryOptions{})
	if err != nil {
		t.Fatalf("Query #1: %v", err)
	}

	// Second Query call without finishing the first. Returns
	// ErrQueryInFlight before touching the wire — does not need to
	// run in a goroutine because no Write is issued.
	_, err = f.client.Query(context.Background(), "second", QueryOptions{})
	if !errors.Is(err, ErrQueryInFlight) {
		t.Fatalf("second Query: got %v; want ErrQueryInFlight", err)
	}

	// Finish the first.
	f.writeFrame(wire.EncodeQueryEnd(wire.QueryEndMsg{QueryID: 1, Status: wire.QueryStatusOK}))
	if _, err := stream1.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("stream1 final Next: %v", err)
	}

	// A subsequent Query now succeeds with QueryID=2.
	stream2, req2, err := f.issueQuery("third", QueryOptions{})
	if err != nil {
		t.Fatalf("Query #2: %v", err)
	}
	if req2.QueryID != 2 {
		t.Errorf("req2.QueryID = %d; want 2", req2.QueryID)
	}
	f.writeFrame(wire.EncodeQueryEnd(wire.QueryEndMsg{QueryID: 2, Status: wire.QueryStatusOK}))
	if _, err := stream2.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("stream2 final Next: %v", err)
	}
}

// TestClientQuery_SecondQueryWhileFirstInFlight_RejectsWithoutIO seals
// the contract that ErrQueryInFlight is returned **before** any bytes
// hit the wire. The check is a latency bound: a correct implementation
// returns purely from a mutex check (sub-millisecond on net.Pipe in
// memory); a regression that tries to Write before checking would
// either deadlock against the unread pipe or, with a deadline, return
// context.DeadlineExceeded instead of ErrQueryInFlight after a much
// longer wait. The 50ms cap is conservative — even under -race it
// gives ~3 orders of magnitude headroom over a real mutex check.
func TestClientQuery_SecondQueryWhileFirstInFlight_RejectsWithoutIO(t *testing.T) {
	f := newQueryFixture(t)

	stream1, _, err := f.issueQuery("first", QueryOptions{})
	if err != nil {
		t.Fatalf("Query #1: %v", err)
	}
	defer stream1.Close()

	// Give the rejected Query a generous ctx. We expect a return time
	// far below the deadline; the ctx is just a safety net so the test
	// cannot hang if the contract is broken in a way that blocks on I/O.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	start := time.Now()
	_, err = f.client.Query(ctx, "second", QueryOptions{})
	elapsed := time.Since(start)

	if !errors.Is(err, ErrQueryInFlight) {
		t.Fatalf("Query #2 error = %v (elapsed=%v); want ErrQueryInFlight", err, elapsed)
	}
	if elapsed > 50*time.Millisecond {
		t.Errorf("Query #2 took %v; expected <50ms (no I/O on rejection path)", elapsed)
	}
}

// TestClientQuery_ConcurrentCalls_OnlyOneSucceedsAtomically exercises
// the one-in-flight enforcement end-to-end through the public Query
// API, not just the internal tryClaimQuery helper. N goroutines all
// call Client.Query through a start barrier; exactly one must win and
// the rest receive ErrQueryInFlight. The wire-write side of the
// winning call is drained from the server end so the goroutine can
// complete. This is the public-API parallel to
// TestTryClaimQuery_ConcurrentClaims_OnlyOneWins in readloop_test.go.
func TestClientQuery_ConcurrentCalls_OnlyOneSucceedsAtomically(t *testing.T) {
	const n = 20
	f := newQueryFixture(t)

	start := make(chan struct{})
	var wg sync.WaitGroup
	var (
		mu            sync.Mutex
		wins          int
		inFlight      int
		otherErr      error
		winningStream *QueryStream
	)

	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			<-start
			s, err := f.client.Query(context.Background(), "race", QueryOptions{})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				wins++
				winningStream = s
			case errors.Is(err, ErrQueryInFlight):
				inFlight++
			default:
				otherErr = err
			}
		}()
	}
	close(start)

	// The winning goroutine writes a QUERY_REQUEST that we must drain
	// from the server end of the pipe; otherwise the pipe write blocks
	// and the goroutine never returns.
	req := f.readRequest()

	wg.Wait()

	if otherErr != nil {
		t.Fatalf("unexpected error from a racing Query: %v", otherErr)
	}
	if wins != 1 {
		t.Errorf("wins = %d; want exactly 1 (only one Query may claim the slot)", wins)
	}
	if inFlight != n-1 {
		t.Errorf("ErrQueryInFlight count = %d; want %d", inFlight, n-1)
	}
	if winningStream == nil {
		t.Fatal("winningStream is nil — no goroutine claimed the slot")
	}

	// Close out the winning stream so the slot is freed and no goroutine
	// (drainer, reader) leaks past the test.
	f.writeFrame(wire.EncodeQueryEnd(wire.QueryEndMsg{
		QueryID: req.QueryID,
		Status:  wire.QueryStatusOK,
	}))
	if _, err := winningStream.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Errorf("winning stream final Next: got %v; want io.EOF", err)
	}
}

// TestClientQuery_AbandonedIteratorUnblocksReadLoopOnClientClose
// validates the c.terminating escape in deliverQueryRow. A caller
// that iterates partially and then walks away (no defer stream.Close,
// no drain to terminal) leaves the read loop blocked on
// `q.rows <- msg` because the iterator's receiver is gone. Without
// the terminating branch, even Client.Close cannot rescue: closing
// the conn only unblocks pending Reads, not in-flight channel Sends.
// The read loop would be pinned forever (goroutine leak) and readDone
// would never close.
//
// With the fix, Client.Close synchronously closes c.terminating
// BEFORE the conn — so a dispatcher mid-send picks the second
// select branch immediately, returns, and the next loop iteration's
// ReadHeader fails on the closed conn → readLoop returns →
// readDone closes.
//
// The test is structured to FORCE the dispatcher into the blocking
// send before Close is called. A naive version (just write rows and
// hope) races: if Close beats the server-side row push, the SDK is
// at ReadHeader (fails normally on conn close) and the
// terminating-branch is never exercised. The test instead:
//
//  1. Caller consumes exactly one row.
//  2. Test synchronously pushes a second row via f.writeFrame —
//     net.Pipe is synchronous, so this returns only AFTER the SDK
//     has read all 20 bytes (header + payload). The dispatcher has
//     the msg in hand and is in or about to enter the blocking
//     send on q.rows.
//  3. A short sleep ensures the dispatcher has actually reached the
//     select. Without the fix, after this point the dispatcher is
//     pinned and conn.Close cannot rescue.
//  4. Client.Close must wake the dispatcher within a short window.
func TestClientQuery_AbandonedIteratorUnblocksReadLoopOnClientClose(t *testing.T) {
	// goleak: any goroutine that survives past Client.Close + readDone
	// observation is a leak. The producer goroutine writing rows
	// completes when its sConn.Write returns an error (peer closed)
	// or all bytes are read; the dispatcher escapes via c.terminating;
	// readLoop exits and closes readDone.
	defer goleak.VerifyNone(t)

	f := newQueryFixture(t)

	stream, req, err := f.issueQuery("x", QueryOptions{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	// Push row 1 from a goroutine so caller's Next can consume it.
	go func() {
		_, _ = f.server.Write(wire.EncodeQueryRow(wire.QueryRowMsg{
			QueryID: req.QueryID, RowSeq: 0, RowJSON: `{"i":1}`,
		}))
	}()

	if _, err := stream.Next(context.Background()); err != nil {
		t.Fatalf("Next #1: %v", err)
	}

	// Synchronously push row 2. Returns only after SDK has read all
	// 20 bytes — at that point the dispatcher has the msg and is
	// inside (or microseconds away from) deliverQueryRow's select.
	f.writeFrame(wire.EncodeQueryRow(wire.QueryRowMsg{
		QueryID: req.QueryID, RowSeq: 1, RowJSON: `{"i":2}`,
	}))

	// Give the dispatcher a moment to actually reach the select. With
	// the fix, this is excessive (the select is reached in
	// microseconds and the test still completes in <1ms after
	// Client.Close). Without the fix, the sleep makes the test
	// deterministic — the dispatcher is guaranteed to be blocked
	// when Close fires.
	time.Sleep(20 * time.Millisecond)

	// Caller "forgot" to Close. Client.Close must terminate the read
	// loop via the c.terminating signal.
	if err := f.client.Close(); err != nil {
		t.Fatalf("Client.Close: %v", err)
	}

	select {
	case <-f.client.readDone:
		// Read loop exited — fix is in place.
	case <-time.After(2 * time.Second):
		t.Fatal("readLoop did not exit within 2s after Client.Close — " +
			"deliverQueryRow is pinned on q.rows<-msg without escape")
	}
}

// ---- Close + abandonment ----------------------------------------------

func TestClientQuery_CloseAbandonsThenDrainerFreesSlotForNextQuery(t *testing.T) {
	f := newQueryFixture(t)
	stream, _, err := f.issueQuery("first", QueryOptions{})
	if err != nil {
		t.Fatalf("Query #1: %v", err)
	}

	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := stream.Next(context.Background()); !errors.Is(err, ErrStreamClosed) {
		t.Errorf("Next after Close: got %v; want ErrStreamClosed", err)
	}

	// The server completes the abandoned query. The background drainer
	// should observe this END and clear the slot.
	f.writeFrame(wire.EncodeQueryEnd(wire.QueryEndMsg{QueryID: 1, Status: wire.QueryStatusOK}))

	deadline := time.Now().Add(time.Second)
	for f.client.currentQuery() != nil {
		if time.Now().After(deadline) {
			t.Fatal("drainer did not clear slot within 1s after server sent END")
		}
		time.Sleep(time.Millisecond)
	}

	stream2, req2, err := f.issueQuery("second", QueryOptions{})
	if err != nil {
		t.Fatalf("Query #2 after drained abandon: %v", err)
	}
	if req2.QueryID != 2 {
		t.Errorf("req2.QueryID = %d; want 2", req2.QueryID)
	}
	f.writeFrame(wire.EncodeQueryEnd(wire.QueryEndMsg{QueryID: 2, Status: wire.QueryStatusOK}))
	if _, err := stream2.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("stream2 final Next: %v", err)
	}
}

// Close after natural termination is a no-op (idempotent).
func TestClientQuery_CloseAfterNaturalTermination_IsNoOp(t *testing.T) {
	f := newQueryFixture(t)
	stream, _, err := f.issueQuery("x", QueryOptions{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	f.writeFrame(wire.EncodeQueryEnd(wire.QueryEndMsg{QueryID: 1, Status: wire.QueryStatusOK}))
	if _, err := stream.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("Next final: %v", err)
	}

	// Close after EOF must not panic and must return nil.
	if err := stream.Close(); err != nil {
		t.Errorf("Close after EOF: got %v; want nil", err)
	}
	// Subsequent Close is idempotent.
	if err := stream.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

// ---- Result-before-drain panic ----------------------------------------

func TestQueryStream_Result_PanicsBeforeDrain(t *testing.T) {
	f := newQueryFixture(t)
	stream, _, err := f.issueQuery("x", QueryOptions{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	defer func() {
		if r := recover(); r == nil {
			t.Error("Result() did not panic before drain")
		}
	}()
	_ = stream.Result()
}

// ---- Context cancellation -----------------------------------------------

func TestQueryStream_Next_RespectsContextCancellation(t *testing.T) {
	f := newQueryFixture(t)
	stream, _, err := f.issueQuery("x", QueryOptions{})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := stream.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("Next: got %v; want context.Canceled", err)
	}
}

// ---- parseEngineCode unit tests ---------------------------------------

func TestParseEngineCode(t *testing.T) {
	tests := []struct {
		input string
		want  uint32
	}{
		{"2014: unknown field", 2014},
		{"5001: internal", 5001},
		{"0: zero is technically valid", 0},
		{"", 0},
		{"no colon here", 0},
		{": no number", 0},
		{"abc: not a number", 0},
		{"-5: negative", 0},
		{" 2014 : has whitespace", 0},
		{"99999999999999999999: overflow int", 0},
	}
	for _, tt := range tests {
		got := parseEngineCode(tt.input)
		if got != tt.want {
			t.Errorf("parseEngineCode(%q) = %d; want %d", tt.input, got, tt.want)
		}
	}
}
