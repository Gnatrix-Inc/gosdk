# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

This is the **Go SDK for gnatrix** — a thin client library that dials a gnatrixquery server over TCP+TLS and authenticates with a `gnx_...` token scoped to a tenant. The module path is `github.com/Gnatrix-Inc/gosdk` (see `go.mod`, Go 1.22).

**Slice 0** (handshake + Ping) and **Slice 1** (queries) are both feature-complete. Slice 1 was split into:

- **Slice 1.1** — codecs in `internal/wire/` for QUERY_REQUEST (0x10), QUERY_ROW (0x11), QUERY_END (0x12), QUERY_PROGRESS (0x14), with byte-stable golden hex fixtures.
- **Slice 1.2** — session state machine: long-running reader goroutine that owns `conn.Read`, dispatches frames by type, enforces one-in-flight query client-side, routes ERROR 2011..2019 to active streams as `*QueryRejectError`, marks the session terminal on any other ERROR or malformed payload. Split internally into 1.2.1..1.2.6.
- **Slice 1.3** — public Query API: `Client.Query`, `QueryStream.Next/Result/Close`, `parseEngineCode` for the `"NNNN: msg"` prefix in QUERY_END.message when status=2.

Test counts (all green, `-race` clean): 43 root + 3 transport + 24 wire-codec. `go build ./...` clean.

**Deferred (not in Slice 1):**

- QUERY_CANCEL (0x13) — Slice 2.
- Progress callbacks / surfacing QUERY_PROGRESS to callers — Slice 2. The dispatcher decodes and discards PROGRESS today.
- Other auth methods (mTLS, password, superuser_peer) — out since Slice 0.
- Retry / reconnect, connection pooling — out since Slice 0; a failed Dial or dropped connection surfaces the error and the caller decides.
- Compression, OpenTelemetry, Prometheus — Slice 3; no hooks exposed.
- Unix socket transport — operator-only, not an SDK concern.
- Multiplexing multiple queries per session — wire carries `query_id` everywhere but the server enforces one-in-flight and the client mirrors that with `ErrQueryInFlight` before touching the wire (see `feedback_no_query_multiplexing.md` in user memory).
- `cmd/example` and `cmd/gnatrix-go-smoke` binaries — removed during library-cleanup; the SDK ships as a library only.

## Go toolchain — important

`/usr/bin/go` on this machine is **gccgo 1.18**, which is too old for this module (`go 1.22`). Use the real upstream toolchain at `/usr/lib/go-1.22/bin/go`. The `Makefile` honors `GO=...`:

```sh
make build GO=/usr/lib/go-1.22/bin/go
make test  GO=/usr/lib/go-1.22/bin/go
```

Other targets: `vet`, `fmt`, `tidy`, `clean`. Single test: `GO=/usr/lib/go-1.22/bin/go go test -run TestName ./...`. `-race` is clean for the full suite. There is no `make run` target — the SDK has no binaries.

## Layout

```
gnatrix.go                public package: Config, Client, Session, Dial, Ping, Close, query type declarations
gnatrix_test.go           Slice 0 end-to-end test with an in-memory fake gnatrixquery server
readloop.go               session state machine: reader goroutine, dispatch, sessionFatal,
                          queryState slot + claim/clear, deliverPong/Row/End/Error
readloop_test.go          dispatch-level unit tests (Slice 1.2): routing, CAS claim,
                          ERROR routing, session-fatal triggers; uses nopConn stub
query.go                  public Query API: Client.Query, QueryStream.{Next,Result,Close},
                          drain, parseEngineCode, ErrStreamClosed
query_test.go             end-to-end query tests (Slice 1.3) using net.Pipe byte injection
internal/transport/       TLS 1.3 dial (DialTLS)
internal/wire/            binary frame codec — one file per frame plus shared primitives
  frame.go                  Magic, Version, MaxPayload, FrameType, Header, AppendHeader/ReadHeader/ReadPayload
  varint.go                 AppendVarint / ReadVarint (LEB128)
  string.go                 AppendLenStr / AppendLenBytes + Read counterparts
  hello.go welcome.go error.go ping.go query.go ...
  testdata/                 golden hex fixtures: hello, welcome, query_request_{no,with}_timerange,
                            query_row, query_end_{ok,timeout}, query_progress
```

## Public API surface

**Slice 0 — connection lifecycle:**

- `Config` — `Addr` ("host:port"), `Token` (raw `gnx_...`), `TenantSlug`, optional `*tls.Config` (defaults to system roots with `ServerName=host`), `CACertPath` (mutually exclusive with `TLSConfig`), `DialTimeout` (5s default), `HandshakeTimeout` (10s default, applies to each of the TLS and gnatrix handshake steps), `ClientVersion` (`"gnatrix-go/0.0.1"` default).
- `Client` — opaque, all internals unexported. Created via `Dial(ctx, cfg)`. Internal `sync.Mutex` serializes wire **writes** so concurrent operations do not interleave bytes; reads are owned exclusively by `readLoop` (see `readloop.go`). `Close` is `sync.Once`-guarded; subsequent calls return the cached value.
- `Session` — populated by `Dial` from the WELCOME frame and returned **by value** (snapshot, not a pointer): `SessionID uint64`, `TenantID [16]byte`, `UserID [16]byte`, `Permissions []string`, `ExpiresAt time.Time`, `IssuedToken` (empty for api_token auth). The 16-byte UUIDs are kept as raw `[16]byte` rather than parsed into a `uuid.UUID` (no extra dep).
- Methods: `Dial`, `(*Client).Session`, `(*Client).Ping`, `(*Client).Close`. `Ping` registers a single-slot waiter under `waiterMu`, writes PING under `mu`, then `select`s on the waiter / `ctx.Done` / `readDone` / a 10s fallback timer. It no longer sets `conn.SetDeadline` (the read loop owns the conn).
- `AuthError{Code uint32, Message string}` — server ERROR frames during handshake with code in **2001..2010** are wrapped in `*AuthError` so callers can `errors.As` them.

**Slice 1 — query API:**

- `(*Client).Query(ctx, queryText string, opts QueryOptions) (*QueryStream, error)` — generates a monotonic `queryID` via `atomic.Uint64` (first value `1`), claims the one-in-flight slot via internal CAS, writes QUERY_REQUEST under `mu`. Returns `ErrQueryInFlight` (without touching the wire) if another query is already active. The atomic increment runs only on a successful claim, so a rejected Query does not burn a counter value.
- `QueryOptions{IndexName, Limit, Cursor, TimeRange *TimeRange, ProgressIntervalMs uint32}`. `IndexName` empty defaults to `"default"`. `TimeRange == nil` means the server applies its default window.
- `TimeRange{EarliestNs, LatestNs int64}` — int64 ns since the Unix epoch. The wire layer reinterprets to/from the two's-complement varint per spec.
- `Row = map[string]any` — type alias; the iterator parses `row_json` via `encoding/json`.
- `(*QueryStream).Next(ctx) (Row, error)` — terminal conditions: `io.EOF` (QUERY_END status=0), `*QueryError` (status≠0, totals captured), `*QueryRejectError` (server ERROR in 2011..2019), `ErrStreamClosed`, `ctx.Err()`, wrapped session-fatal error.
- `(*QueryStream).Result() *QueryResult` — valid only after Next returned a terminal. Panics otherwise (programmer error). Returns nil when the stream terminated via reject (no QUERY_END was received).
- `(*QueryStream).Close() error` — idempotent. Closes `closeCh` (wakes Next with `ErrStreamClosed`) and spawns a background drainer that consumes residual ROWs / END / reject until the slot can be released. The drainer also exits via `iterDoneCh` if Next reached a terminal first, or via `c.readDone` if the session died.
- `QueryResult{QueryID, RowsReturned, EventsScanned, EventsMatched, ElapsedMs, Truncated, NextCursor}` — totals from QUERY_END.
- `QueryError{Status, Message, EngineCode, QueryID}` — non-OK QUERY_END statuses 1..7. `EngineCode` is parsed from the `"NNNN: msg"` prefix when `Status == 2` (engine_error). Parsing uses `strings.IndexByte(':')` + `strconv.Atoi`, never regex.
- `QueryRejectError{Code, Message}` — server ERROR with code in 2011..2019 received while a query is in-flight.
- `ErrQueryInFlight` — returned by `Query` when another query is active on the same `*Client`.
- `ErrStreamClosed` — returned by `Next` after `Close`.

These signatures are locked. Internals (frame routing, claim CAS, drainer goroutine) belong in `readloop.go` / `query.go` / `internal/`.

## Contract properties verified by tests

The current suite locks down these behaviors. When refactoring, do not regress them:

- `Dial` with a cancelled or deadline-elapsed `ctx` returns within **50 ms**; the returned error satisfies `errors.Is(err, context.Canceled)` or `errors.Is(err, context.DeadlineExceeded)` respectively (`TestDial_Context*`). The fast-fail comes from `net.Dialer.DialContext` short-circuiting; we just preserve the cause with `%w` wrapping in `transport.DialTLS`.
- `ctx` cancellation **mid-handshake** (after TLS is up, while the SDK is blocked in ReadHeader waiting for WELCOME) is propagated to the in-flight conn I/O via a watcher goroutine that collapses the conn deadline. Dial returns within ~1 s of the cancellation with `errors.Is(err, context.Canceled)`, never waits out `HandshakeTimeout` (`TestDial_ContextCancelledMidHandshake_FastFailsWithCtxErr`). The fake server's `stallAfterHello` option provides the unresponsive peer this test needs. The wiring lives in `handshake()` (`gnatrix.go` — watcher goroutine + `handshakeStageErr` helper); a missing or broken watcher would let Dial hang for the full HandshakeTimeout (5 s in the test) and surface `i/o timeout` instead.
- `Dial` with a valid api_token returns a `*Client` whose `Session().SessionID > 0` and whose `Permissions` carry the server-issued strings (`TestDial_HappyPath_SessionPopulated`, `TestDial_HelloAPIToken_ReturnsWelcome`). The latter also locks the on-wire shape of HELLO for `auth_method=1` (literal `1`, not the named constant).
- Server ERROR codes in **2001..2010** surface as `*AuthError` recoverable via `errors.As`, and both `Code` and `Message` are propagated intact; codes outside that range surface as plain `fmt.Errorf` (`TestDial_InvalidToken_ReturnsAuthError2002`, `TestDial_UnknownTenant_ReturnsAuthError2001`, `TestDial_ServerError2010_ReturnsAuthError` — the last pins the inclusive upper bound 2010 via the fake server's `forceErrorCode` hook in `fakeServerOpts`).
- `Client.Ping` performs a real PING→PONG round-trip; a second consecutive Ping succeeds, verifying that the waiter slot is released and `mu` does not stay held (`TestClient_Ping_RoundTrip`).
- `Client.mu` (writes only) actually serializes concurrent `Ping` calls: `TestClient_Ping_ConcurrentCallsAreSerialized` fires N=10 goroutines through a start barrier against a fake server whose `pingResponseDelay` option processes each PING in its own goroutine (so the server imposes no ordering). With the mutex, total wall time ≥ (N-1) × delay ≈ 500 ms; without it, wall collapses to ≈ delay. The hook lives in `fakeServerOpts.pingResponseDelay` and `servePings`. The model changed in Slice 1.2.1 — reads moved to `readLoop` — but the serialization invariant is preserved because Ping holds `mu` for the whole write+wait window and the single `pingWaiter` slot ensures one PONG handoff at a time.
- `Client.Close` is idempotent (`sync.Once`-guarded — subsequent calls return the cached value), `Ping` after `Close` returns an error without panicking, and a third `Close` after a failed `Ping` is still a no-op (`TestClient_Close_IsCleanAndIdempotent`). The TLS layer sends `close_notify` then FIN; there is no wire-level GOODBYE frame.
- TLS 1.3 minimum is enforced: a server forcing TLS 1.2 fails the handshake at the SDK side even if the caller's `tls.Config` would allow it (`internal/transport.TestDialTLS_RejectsTLS12Server`).
- A server that drops the TLS handshake (e.g. accepts the TCP connection and closes it immediately) makes `Dial` fast-fail within `HandshakeTimeout + 1s` with a wrapped error recoverable as one of `tls.AlertError`, `*net.OpError`, or `io.EOF` via `errors.As`/`errors.Is` — never an opaque string-only error, never a hang (`TestDial_TLSHandshakeDropped_FastFailsWithWrappedError`). The three accept-list types reflect the canonical Go outcomes for "handshake interrupted": fatal alert, OS-level reset, or clean EOF mid-handshake respectively.
- LEB128 varint round-trip is locked down at the codec layer: the boundary values 0, 1, 127, 128, 16383, 16384, 2³²−1, 2⁶³−1 encode to their canonical byte sequences (`TestVarint_BoundaryValues`), and 10 000 PCG-seeded random `uint64` values round-trip exactly with no excess bytes (`TestVarint_RandomRoundTrip`). Both tests live in `internal/wire/varint_test.go`.
- HELLO encoder output for `auth_method=1` is byte-stable against the golden fixture `internal/wire/testdata/hello_apitoken.hex` — 8-byte header + 37-byte payload, 45 bytes total (`TestEncodeHello_APIToken_MatchesGolden`). `DecodeHello` recovers every field from the same fixture (`TestDecodeHello_APIToken_RoundTripsGolden`). Fixture inputs: `tenant_slug="acme"`, `credential="gnx_test123"`, `has_email=0`, `client_capabilities=0`, `client_version="gnatrix-go/0.0.1"`.
- WELCOME encoder/decoder are byte-stable against `internal/wire/testdata/welcome_apitoken.hex` — 8-byte header + 67-byte payload, 75 bytes total (`TestEncodeWelcome_APIToken_MatchesGolden`, `TestDecodeWelcome_APIToken_RoundTripsGolden`). Fixture: `SessionID=42`, `ServerCapabilities=0`, `SessionExpiresAtMs=1_700_000_000_000`, `UserID=11111111-2222-3333-4444-555555555555` raw bytes, `TenantID=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee` raw bytes, `Permissions=["query:read","query:write"]`, `IssuedToken=""`. The decode test asserts UUID byte preservation with `bytes.Equal` on the raw `[16]byte` arrays — this catches any future regression that introduces a string round-trip (e.g. parsing into `uuid.UUID` and back) in the codec.

### Slice 1.1 — query codecs

- QUERY_REQUEST encoder is byte-stable against two fixtures: `query_request_no_timerange.hex` (28-byte payload, `has_time_range=0`, default `progress_interval_ms=0`) and `query_request_with_timerange.hex` (37-byte payload, `has_time_range=1`, ns timestamps). Both encode/decode pairs lock the v2 layout including the int64↔uint64 reinterpretation for ns timestamps. `TestQueryRequest_NegativeTimestamps_RoundTrip` additionally verifies that negative ns values round-trip without bit corruption (a regression that uses `int(0)` as the sentinel instead of `int64(0)` would silently truncate). `TestQueryRequest_HasTimeRangeInvalidValue` rejects any `has_time_range` byte other than 0 or 1.
- QUERY_ROW encoder/decoder are byte-stable against `query_row.hex` (31-byte payload, `QueryID=42`, `RowSeq=5`, `RowJSON={"_time":"2026-05-20","x":1}`).
- QUERY_END encoder is byte-stable against `query_end_ok.hex` (12-byte payload, status=0 with totals) and `query_end_timeout.hex` (42-byte payload, status=6, non-empty `next_cursor` and `message`). The v2 layout inserts `events_scanned`, `events_matched`, `elapsed_ms`, and a u8 `truncated` between `rows_returned` and `next_cursor`; reordering or omitting any of these is what the fixtures catch. `TestQueryEnd_TruncatedInvalidValue` rejects `truncated` bytes other than 0/1.
- QUERY_PROGRESS encoder/decoder are byte-stable against `query_progress.hex` (9-byte payload: `QueryID`, `events_scanned`, `events_matched`, `segments_done`, `segments_total`, `elapsed_ms`).

### Slice 1.2 — dispatcher / state machine

- `Client.tryClaimQuery` is atomic: `TestTryClaimQuery_ConcurrentClaims_OnlyOneWins` fires N=20 goroutines through a start barrier; exactly one returns `nil` and the other 19 return `ErrQueryInFlight`. This proves the check-and-set inside the mutex is a real CAS — a missing lock would let multiple goroutines see `activeQuery==nil` and silently overwrite each other.
- Dispatch routes QUERY_ROW / QUERY_END only to the slot whose `queryID` matches the incoming frame; mismatches and absent-slot cases drop silently (`TestDispatch_QueryRow_DroppedWhenQueryIDMismatches`, `TestDispatch_QueryEnd_DroppedWhenQueryIDMismatches`, `TestDispatch_QueryRow_DroppedWhenNoActiveSlot`). Forgiveness is intentional: server-side cleanup races can produce stale frames that should not crash the session.
- QUERY_PROGRESS is decoded (so the stream stays synchronized) and discarded — never reaches a slot channel (`TestDispatch_QueryProgress_DecodedAndDiscarded`). Slice 2 will expose progress without changing this default.
- ERROR routing has explicit boundary discipline. With an active slot, codes **2011..2019 inclusive** become `*QueryRejectError` on the slot's `reject` channel (`TestDispatch_Error_BoundaryCodes` pins 2011 and 2019). Any other code — including 2010 and 2020 just outside the band, 1xxx framing, 3001 rate-limit, 5001 internal — triggers `sessionFatal` (`TestDispatch_Error_OutOfRange_TriggersSessionFatal`). ERROR with no active slot also triggers `sessionFatal` (`TestDispatch_Error_NoActiveQuery_TriggersSessionFatal`) — an unsolicited server error has no recovery path.
- A malformed payload (decode failure) for any QUERY_* or ERROR frame triggers `sessionFatal` (`TestDispatch_*_MalformedPayload_TriggersSessionFatal`). The wire stream is presumed corrupt; the session is closed via `closeOnce` and `readErr` is recorded so subsequent Ping/Query calls surface a meaningful cause.
- After `sessionFatal` fires, `dispatch` is a no-op for any further frames the read loop happens to have already buffered (`TestDispatch_PostFatal_IsNoOp`). The guard is a `terminalErr() != nil` check at the top of dispatch; without it, an in-flight TCP buffer could deliver one more row after the conn close was initiated.
- `Client.conn` is held as `net.Conn` (not `*tls.Conn`) so tests can substitute a `nopConn` stub that records `Close()` via `atomic.Bool`. Production unchanged: `transport.DialTLS` returns `*tls.Conn` which satisfies the interface.

### Slice 1.3 — query end-to-end

- Tests use `net.Pipe` + raw byte injection per `feedback_no_query_mocks.md`. The fixture (`queryFixture` in `query_test.go`) wires one pipe end to `Client.conn` (with the real `readLoop` running) and exposes the other end for the test to write pre-encoded frames using our own `EncodeQuery*` helpers. There is no per-frame handler logic on the test side.
- Happy path (`TestClientQuery_HappyPath_StreamsRowsAndCompletes`) issues a Query, asserts the on-wire `QueryID == 1` (monotonic counter starts at 1, only incremented on a successful claim), streams two rows from a producer goroutine, observes `io.EOF` after QUERY_END, and reads totals via `Result()`. The producer must be a goroutine because the rows channel is unbuffered — synchronous writes would deadlock on the second row while the dispatcher is stuck inside the first `state.rows <- msg`.
- `QueryOptions.IndexName == ""` defaults to `"default"` on the wire (`TestClientQuery_DefaultIndexName`).
- `QueryOptions.TimeRange != nil` sets `has_time_range=1` and propagates both ns timestamps verbatim (`TestClientQuery_TimeRangeFlowsToWire`).
- Pre-execution rejection: an ERROR with code 2014 sent after Query arrives at the iterator as `*QueryRejectError{Code:2014, Message:"..."}`, and `Result()` returns nil (no QUERY_END was emitted) (`TestClientQuery_PreExecutionReject_AsQueryRejectError`).
- Post-execution error: QUERY_END with `Status=6` (timeout) surfaces as `*QueryError`, and `Result()` returns valid totals (`ElapsedMs=30000`) because the END frame was received even though the query failed (`TestClientQuery_PostExecutionTimeout_AsQueryError`).
- Engine error parsing: QUERY_END with `Status=2` and `Message="2014: unknown field 'usre'"` produces `*QueryError{EngineCode:2014}`. The parse is `strings.IndexByte(':')` + `strconv.Atoi`, never regex (`TestClientQuery_EngineError_ParsesEngineCodeFromPrefix`, `TestParseEngineCode` table-test with 10 boundary inputs including garbage / negative / whitespace / overflow).
- One-in-flight: a second `Query()` while the first is active returns `ErrQueryInFlight` **without** writing any bytes to the wire (`TestClientQuery_SecondQueryWhileFirstInFlight_ReturnsErrQueryInFlight`). The test does not need a goroutine for the rejected call because no `conn.Write` is issued. After the first query finishes, the third call succeeds with `QueryID=2` — the rejected call did not burn a counter value.
- Close abandonment: `Close()` returns immediately, subsequent `Next()` returns `ErrStreamClosed`, and a background drainer consumes the eventual QUERY_END from the server and frees the slot. A follow-up `Query()` then succeeds (`TestClientQuery_CloseAbandonsThenDrainerFreesSlotForNextQuery`). The test polls `c.currentQuery()` with a 1s deadline to assert the drainer cleared the slot.
- Close after natural termination is a no-op (`TestClientQuery_CloseAfterNaturalTermination_IsNoOp`). Idempotent across multiple calls.
- `Result()` panics if called before the stream terminated (`TestQueryStream_Result_PanicsBeforeDrain`).
- `Next` respects a per-call ctx: a pre-canceled context surfaces as `context.Canceled` from `Next` (`TestQueryStream_Next_RespectsContextCancellation`).

## Encapsulation boundary

Framing, varint encoding, TLS dial mechanics, session lifecycle, and the read loop / dispatcher are all hidden:

- Framing (`Header`, `FrameType`, `Magic`, `Version`, `MaxPayload`, `AppendHeader`/`ReadHeader`/`ReadPayload`) lives in `internal/wire/frame.go`. Go's `internal/` rule blocks external imports.
- Varint / lenstr / lenbytes encoding lives in `internal/wire/varint.go` and `string.go`. Same `internal/` lock.
- Per-frame codecs (`HelloMsg`, `WelcomeMsg`, `ErrorMsg`, `QueryRequestMsg`, `QueryRowMsg`, `QueryEndMsg`, `QueryProgressMsg`, …) live in `internal/wire/` and are not part of the public surface; the SDK exposes only the parsed value types (`Session`, `Row`, `QueryResult`, etc.).
- TLS dial mechanics live in `internal/transport/`. `Config.TLSConfig *tls.Config` is the **only** deliberate extension point — callers can inject CAs, ServerName, etc., but the `MinVersion=TLS13` pin in `transport.buildTLSConfig` cannot be disabled from outside.
- The read loop, dispatcher, query slot, claim CAS, and `sessionFatal` are unexported (`readloop.go`). `Client.conn` is `net.Conn` for testability but is itself unexported and there is no public getter.
- Session lifecycle is entirely behind `Dial` (create) → `Session()` (snapshot read) → `Query()` (claim) → `Close()` (idempotent teardown). There is no getter for the underlying conn and no method to re-handshake.

When adding to the public surface, preserve this boundary — protocol details (frame types, byte layouts, varint encoding, dispatcher routing) must not leak through.

## Wire-protocol invariants

The on-wire format is authoritatively specified in `/usr/local/src/gnatrix/server_client/docs/wire-protocol.md`. The Slice-1 update lives at `server_client/docs/schema/wire-protocolv2.md` (same version byte `0x01`; layers on `QUERY_PROGRESS` (0x14), new error codes 2011..2019 for query-level session errors, extended QUERY_REQUEST/QUERY_END fields, and the explicit "one in-flight query per session" invariant). The SDK now speaks v2 in full: QUERY_REQUEST carries `has_time_range` / `earliest_ns` / `latest_ns` / `progress_interval_ms`; QUERY_END carries `events_scanned` / `events_matched` / `elapsed_ms` / `truncated`; QUERY_PROGRESS is decoded (and discarded pending Slice 2); ERROR codes 2011..2019 are surfaced as `*QueryRejectError`. Key invariants enforced by this SDK today:

- TLS 1.3 minimum. `transport.DialTLS` pins `tls.Config.MinVersion = VersionTLS13` even if the caller's config would allow less; the wire spec makes TLS 1.3 a hard requirement and the SDK refuses to downgrade.
- Frame header: magic `0x47` ('G'), version `0x01`, type byte, flags `0x00`, payload length u32 LE. Max payload 65536 bytes.
- Default port 7777 (configurable via `Config.Addr`).
- Auth token prefix is `gnx_`; the server stores `BLAKE3-256(token_bytes)`.
- HELLO carries `client_version` (default `"gnatrix-go/0.0.1"`); WELCOME yields the populated `Session`.

## Wire package conventions (`internal/wire/`)

When adding or modifying frame codecs, follow the conventions established by `frame.go` and `hello.go`:

- One file per frame type, named after the frame (`hello.go`, `welcome.go`, ...). Exceptions: all `QUERY_*` frames live in `query.go`; PING and PONG share `ping.go`.
- Struct names are `XxxMsg` (e.g. `HelloMsg`, `WelcomeMsg`).
- Encoders are package-level functions `EncodeXxx(msg XxxMsg) []byte` that return a **complete on-wire frame** (8-byte header + payload), not just the payload. They build the payload via `AppendVarint`/`AppendLenStr`/`AppendLenBytes` and prepend the header via `AppendHeader(nil, FrameXxx, uint32(len(p)))`.
- Decoders are package-level functions `DecodeXxx(r io.Reader) (XxxMsg, error)`. They consume the **payload only** — the caller is expected to have already read the header via `ReadHeader` and to pass a bounded reader (typically `bytes.NewReader(payload)` after `ReadPayload`).
- Primitive helpers (`AppendVarint`, `ReadVarint`, `AppendLenStr`, `ReadLenStr`, `AppendLenBytes`, `ReadLenBytes`) are exported. Read* variants take `io.Reader`.
- Frame type constants are named `FrameXxx` and live exclusively in `frame.go`. Do not duplicate them in per-frame files.
- Reserved frames (AUTH_CHALLENGE, AUTH_RESPONSE, QUERY_CANCEL) have type-byte allocations in `frame.go` and a stub file with a comment explaining the reservation; they have no codec yet.
- Byte-stable encoders get a golden-hex test under `testdata/<frame>_<variant>.hex`, checked both ways: `Encode → bytes.Equal(golden)` and `Decode(golden) → expected struct`. `hello_test.go` is the reference. The `loadHexFixture` helper strips ASCII whitespace before `hex.DecodeString`, so fixtures can use spaces and newlines to group bytes by field.

## Related repos

The wider gnatrix system lives at `/usr/local/src/gnatrix/` — notably `gnatrix-gateway`, `engine`/`engine-2.0`/`engine-3.0`, `gnx-api-backend`, `server_client`, `terminal-client`. The server side of the protocol this SDK speaks lives in those repos; consult them (especially `gnatrix-gateway` and `server_client`) when implementing the wire format or matching auth-error codes. `server_client/docs/wire-protocol.md` is the authoritative spec.

The two server-side C++ files this SDK shadows most directly:

- `/usr/local/src/gnatrix/server_client/include/gnatrix/net/frame_codec.hpp` — frame header layout, `encode_varint`/`decode_varint`, and the per-frame `encode(...)` / `decode(...)` declarations. The naming conventions in `internal/wire/` (`EncodeXxx` returning a full frame, payload-only `DecodeXxx`) mirror this header; the existing `EncodeHello` doc-comment already cross-references `frame_codec.cpp`. When adding or modifying a codec, diff your byte layout against this header.
- `/usr/local/src/gnatrix/server_client/src/net/handshake.cpp` — server-side HELLO/WELCOME/ERROR orchestration: how the server validates `auth_method=1`, populates the WELCOME fields, and chooses ERROR codes in the 2001..2010 range. The boundary cases in `gnatrix_test.go` (unknown tenant → 2001, invalid token → 2002, upper bound 2010) are anchored to the branches in this file. Re-read it before broadening the AuthError range or wiring new auth methods.

The runnable server this SDK is designed to dial against:

- `/usr/local/src/gnatrix/server_client/build/bin/gnatrixquery` — compiled `gnatrixquery` binary. Start this locally to exercise the SDK end-to-end against a real peer (rather than the in-test fake server / `net.Pipe` byte injection used in unit tests). Sibling binaries in the same directory: `server`, `gnatrix-migrate`, `gnatrix_password`, `gnatrix_unit_tests`, `gnatrix_integration_tests`, plus a `run` launcher.

A downstream consumer of this SDK lives at `/usr/local/src/gnatrix/gnatrix-tests/gosdk/` — a sample Go REST API (go.mod declares `go 1.25.0`, uses `replace github.com/Gnatrix-Inc/gosdk => ../../gnatrix-gosdk`) that exposes `/api/v1/gnatrix/{session,ping}` via `internal/starter/`. With Slice 1.3 shipped, this is also the natural place to add a query endpoint to test the SDK against the real `gnatrixquery` server end-to-end.
