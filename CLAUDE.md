# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

This is the **Go SDK for gnatrix** — a thin client library that dials a gnatrixquery server over TCP+TLS and authenticates with a `gnx_...` token scoped to a tenant. The module path is `github.com/gnatrix/gnatrix-gosdk` (see `go.mod`, Go 1.22).

The repo is delivering the **Slice 0** ticket. Slice 0 must cover:

1. Project skeleton
2. Frame codec (`internal/wire/`)
3. TLS transport (`internal/transport/`)
4. HELLO / WELCOME handshake with `auth_method=1` (api_token) only
5. PING / PONG keepalive
6. `Ping()` method on the public `*Client`

Slice 0 explicitly **excludes**:

- Query executions (QUERY_REQUEST / ROW / END / CANCEL / PROGRESS)
- Other auth methods (mTLS, password, superuser_peer)
- Retry / reconnect logic — a failed Dial or dropped connection surfaces the error; the caller decides
- Connection pooling — each `Dial` returns a single dedicated `*Client`
- Compression, OpenTelemetry tracing, Prometheus metrics — deferred to **Slice 3**; the SDK exposes no hooks for any of them today
- Unix socket transport — not relevant for SDK users (peer auth is operator-only)

Current state: all six items above are implemented and tested (12 root + 3 transport + 9 wire-codec tests, all green). Slice 0 is feature-complete on the SDK surface. `go build ./...` is clean. Query codecs exist in `internal/wire/query.go` as inert scaffolding — they are **not** exposed from `*Client`, and they reflect the v1 wire spec minus the dropped `query_kind` field (see Slice-1 alignment note in the file's doc-comment). Both `cmd/` binaries are now real: `cmd/example/` is a flag-driven happy-path demo (Dial → Session → Ping → Close); `cmd/gnatrix-go-smoke/` is a non-interactive CI smoke (Dial → Ping × 3 → Close, exits 0/1).

## Go toolchain — important

`/usr/bin/go` on this machine is **gccgo 1.18**, which is too old for this module (`go 1.22`). Use the real upstream toolchain at `/usr/lib/go-1.22/bin/go`. The `Makefile` honors `GO=...`, so:

```sh
make build GO=/usr/lib/go-1.22/bin/go
make test  GO=/usr/lib/go-1.22/bin/go
make run   GO=/usr/lib/go-1.22/bin/go        # runs cmd/example
```

Other targets: `vet`, `fmt`, `tidy`, `clean`. Single test: `GO=/usr/lib/go-1.22/bin/go go test -run TestName ./...`.

Both binaries take the same required flags: `-addr host:port`, `-token gnx_...`, `-tenant slug`, plus an optional `-timeout` (10s default). They print a one-line usage and exit 2 when a required flag is missing. `go build ./...` produces an `example` and a `gnatrix-go-smoke` binary in the working directory — add them to `.gitignore` if you build at the repo root.

## Layout

```
gnatrix.go                public package: Config, Client, Session, AuthError, Dial, Ping, Close
gnatrix_test.go           end-to-end test with an in-memory fake gnatrixquery server
internal/transport/       TLS 1.3 dial (DialTLS)
internal/wire/            binary frame codec — one file per frame plus shared primitives
  frame.go                  Magic, Version, MaxPayload, FrameType, Header, AppendHeader/ReadHeader
  varint.go                 AppendVarint / ReadVarint (LEB128)
  string.go                 AppendLenStr/AppendLenBytes + Read counterparts
  hello.go welcome.go error.go ping.go query.go ...
  testdata/                 golden hex fixtures for byte-stable frame encoders
cmd/example/              flag-driven runnable demo (Dial → Session → Ping → Close)
cmd/gnatrix-go-smoke/     non-interactive CI smoke (Dial → Ping × 3 → Close; exits 0/1)
```

## Public API surface

- `Config` — `Addr` ("host:port"), `Token` (raw `gnx_...`), `TenantSlug`, optional `*tls.Config` (defaults to system roots with `ServerName=host`), `DialTimeout` (5s default), `HandshakeTimeout` (10s default, applies to each of the TLS and gnatrix handshake steps), `ClientVersion` (`"gnatrix-go/0.0.1"` default).
- `Client` — opaque, all internals unexported. Created via `Dial(ctx, cfg)`. An internal `sync.Mutex` serializes wire round-trips so concurrent `Ping` calls are safe; `Close` is `sync.Once`-guarded so duplicate Close calls are no-ops.
- `Session` — populated by `Dial` from the WELCOME frame and returned **by value** (snapshot, not a pointer): `SessionID uint64`, `TenantID [16]byte`, `UserID [16]byte`, `Permissions []string`, `ExpiresAt time.Time`, `IssuedToken` (empty for api_token auth). The 16-byte UUIDs are kept as raw `[16]byte` rather than parsed into a `uuid.UUID` (no extra dep for Slice 0).
- Methods: `Dial`, `(*Client).Session`, `(*Client).Ping`, `(*Client).Close`. `Ping` honors `ctx.Deadline()` and otherwise caps the round-trip at a hardcoded 10s fallback.
- `AuthError{Code uint32, Message string}` — server ERROR frames with code in the **2001..2010** range are wrapped in `*AuthError` so callers can `errors.As` them. Other server errors come back as plain `fmt.Errorf`.

These signatures are locked. Internals (wire protocol, framing, handshake orchestration) belong in unexported files or under `internal/`.

## Contract properties verified by tests

The current suite locks down these behaviors. When refactoring, do not regress them:

- `Dial` with a cancelled or deadline-elapsed `ctx` returns within **50 ms**; the returned error satisfies `errors.Is(err, context.Canceled)` or `errors.Is(err, context.DeadlineExceeded)` respectively (`TestDial_Context*`). The fast-fail comes from `net.Dialer.DialContext` short-circuiting; we just preserve the cause with `%w` wrapping in `transport.DialTLS`.
- `ctx` cancellation **mid-handshake** (after TLS is up, while the SDK is blocked in ReadHeader waiting for WELCOME) is propagated to the in-flight conn I/O via a watcher goroutine that collapses the conn deadline. Dial returns within ~1 s of the cancellation with `errors.Is(err, context.Canceled)`, never waits out `HandshakeTimeout` (`TestDial_ContextCancelledMidHandshake_FastFailsWithCtxErr`). The fake server's `stallAfterHello` option provides the unresponsive peer this test needs. The wiring lives in `handshake()` (`gnatrix.go` — watcher goroutine + `handshakeStageErr` helper); a missing or broken watcher would let Dial hang for the full HandshakeTimeout (5 s in the test) and surface `i/o timeout` instead.
- `Dial` with a valid api_token returns a `*Client` whose `Session().SessionID > 0` and whose `Permissions` carry the server-issued strings (`TestDial_HappyPath_SessionPopulated`, `TestDial_HelloAPIToken_ReturnsWelcome`). The latter also locks the on-wire shape of HELLO for `auth_method=1` (literal `1`, not the named constant).
- Server ERROR codes in **2001..2010** surface as `*AuthError` recoverable via `errors.As`, and both `Code` and `Message` are propagated intact; codes outside that range surface as plain `fmt.Errorf` (`TestDial_InvalidToken_ReturnsAuthError2002`, `TestDial_UnknownTenant_ReturnsAuthError2001`, `TestDial_ServerError2010_ReturnsAuthError` — the last pins the inclusive upper bound 2010 via the fake server's `forceErrorCode` hook in `fakeServerOpts`).
- `Client.Ping` performs a real PING→PONG round-trip; a second consecutive Ping succeeds, verifying that the `sync.Mutex` is released and the conn deadline is reset (`TestClient_Ping_RoundTrip`).
- `Client.mu` actually serializes concurrent `Ping` calls: `TestClient_Ping_ConcurrentCallsAreSerialized` fires N=10 goroutines through a start barrier against a fake server whose `pingResponseDelay` option processes each PING in its own goroutine (so the server imposes no ordering). With the mutex, total wall time ≥ (N-1) × delay ≈ 500 ms; without it, wall collapses to ≈ delay. This is the only test that proves the contract — `tls.Conn` internally synchronizes Read/Write so memory races aren't the failure mode being checked, PING/PONG disassociation is. The hook lives in `fakeServerOpts.pingResponseDelay` and `servePings`.
- `Client.Close` is idempotent (`sync.Once`-guarded — subsequent calls return the cached value), `Ping` after `Close` returns an error without panicking, and a third `Close` after a failed `Ping` is still a no-op (`TestClient_Close_IsCleanAndIdempotent`). The TLS layer sends `close_notify` then FIN; there is no wire-level GOODBYE frame.
- TLS 1.3 minimum is enforced: a server forcing TLS 1.2 fails the handshake at the SDK side even if the caller's `tls.Config` would allow it (`internal/transport.TestDialTLS_RejectsTLS12Server`).
- A server that drops the TLS handshake (e.g. accepts the TCP connection and closes it immediately) makes `Dial` fast-fail within `HandshakeTimeout + 1s` with a wrapped error recoverable as one of `tls.AlertError`, `*net.OpError`, or `io.EOF` via `errors.As`/`errors.Is` — never an opaque string-only error, never a hang (`TestDial_TLSHandshakeDropped_FastFailsWithWrappedError`). The three accept-list types reflect the canonical Go outcomes for "handshake interrupted": fatal alert, OS-level reset, or clean EOF mid-handshake respectively.
- LEB128 varint round-trip is locked down at the codec layer: the boundary values 0, 1, 127, 128, 16383, 16384, 2³²−1, 2⁶³−1 encode to their canonical byte sequences (`TestVarint_BoundaryValues`), and 10 000 PCG-seeded random `uint64` values round-trip exactly with no excess bytes (`TestVarint_RandomRoundTrip`). Both tests live in `internal/wire/varint_test.go`.
- HELLO encoder output for `auth_method=1` is byte-stable against the golden fixture `internal/wire/testdata/hello_apitoken.hex` — 8-byte header + 37-byte payload, 45 bytes total (`TestEncodeHello_APIToken_MatchesGolden`). `DecodeHello` recovers every field from the same fixture (`TestDecodeHello_APIToken_RoundTripsGolden`). Fixture inputs: `tenant_slug="acme"`, `credential="gnx_test123"`, `has_email=0`, `client_capabilities=0`, `client_version="gnatrix-go/0.0.1"`.
- WELCOME encoder/decoder are byte-stable against `internal/wire/testdata/welcome_apitoken.hex` — 8-byte header + 67-byte payload, 75 bytes total (`TestEncodeWelcome_APIToken_MatchesGolden`, `TestDecodeWelcome_APIToken_RoundTripsGolden`). Fixture: `SessionID=42`, `ServerCapabilities=0`, `SessionExpiresAtMs=1_700_000_000_000`, `UserID=11111111-2222-3333-4444-555555555555` raw bytes, `TenantID=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee` raw bytes, `Permissions=["query:read","query:write"]`, `IssuedToken=""`. The decode test asserts UUID byte preservation with `bytes.Equal` on the raw `[16]byte` arrays — this catches any future regression that introduces a string round-trip (e.g. parsing into `uuid.UUID` and back) in the codec.

## Encapsulation boundary

Framing, varint encoding, TLS dial mechanics, and session lifecycle are all hidden:

- Framing (`Header`, `FrameType`, `Magic`, `Version`, `MaxPayload`, `AppendHeader`/`ReadHeader`/`ReadPayload`) lives in `internal/wire/frame.go`. Go's `internal/` rule blocks external imports.
- Varint / lenstr / lenbytes encoding lives in `internal/wire/varint.go` and `string.go`. Same `internal/` lock.
- TLS dial mechanics live in `internal/transport/`. `Config.TLSConfig *tls.Config` is the **only** deliberate extension point — callers can inject CAs, ServerName, etc., but the `MinVersion=TLS13` pin in `transport.buildTLSConfig` cannot be disabled from outside.
- Session lifecycle is entirely behind `Dial` (create) → `Session()` (snapshot read) → `Close()` (idempotent teardown). There is no getter for the underlying `*tls.Conn` and no method to re-handshake.

When adding to the public surface, preserve this boundary — protocol details (frame types, byte layouts, varint encoding) must not leak through.

## Wire-protocol invariants

The on-wire format is authoritatively specified in `/usr/local/src/gnatrix/server_client/docs/wire-protocol.md`. The Slice-1 update lives at `server_client/docs/schema/wire-protocolv2.md` (same version byte `0x01`; layers on `QUERY_PROGRESS` (0x14), new error codes 2011..2019 for query-level session errors, extended QUERY_REQUEST/QUERY_END fields, and the explicit "one in-flight query per session" invariant). Slice 0 only speaks the v1 frames; v2 is informational here. Key invariants enforced by this SDK today:

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
- Reserved frames (AUTH_CHALLENGE, AUTH_RESPONSE, QUERY_CANCEL, QUERY_PROGRESS) have type-byte allocations in `frame.go` and a stub file with a comment explaining the reservation; they have no codec yet.
- Byte-stable encoders get a golden-hex test under `testdata/<frame>_<variant>.hex`, checked both ways: `Encode → bytes.Equal(golden)` and `Decode(golden) → expected struct`. `hello_test.go` is the reference. The `loadHexFixture` helper strips ASCII whitespace before `hex.DecodeString`, so fixtures can use spaces and newlines to group bytes by field.

## Related repos

The wider gnatrix system lives at `/usr/local/src/gnatrix/` — notably `gnatrix-gateway`, `engine`/`engine-2.0`/`engine-3.0`, `gnx-api-backend`, `server_client`, `terminal-client`. The server side of the protocol this SDK speaks lives in those repos; consult them (especially `gnatrix-gateway` and `server_client`) when implementing the wire format or matching auth-error codes. `server_client/docs/wire-protocol.md` is the authoritative spec.

The two server-side C++ files this SDK shadows most directly:

- `/usr/local/src/gnatrix/server_client/include/gnatrix/net/frame_codec.hpp` — frame header layout, `encode_varint`/`decode_varint`, and the per-frame `encode(...)` / `decode(...)` declarations. The naming conventions in `internal/wire/` (`EncodeXxx` returning a full frame, payload-only `DecodeXxx`) mirror this header; the existing `EncodeHello` doc-comment already cross-references `frame_codec.cpp`. When adding or modifying a codec, diff your byte layout against this header.
- `/usr/local/src/gnatrix/server_client/src/net/handshake.cpp` — server-side HELLO/WELCOME/ERROR orchestration: how the server validates `auth_method=1`, populates the WELCOME fields, and chooses ERROR codes in the 2001..2010 range. The boundary cases in `gnatrix_test.go` (unknown tenant → 2001, invalid token → 2002, upper bound 2010) are anchored to the branches in this file. Re-read it before broadening the AuthError range or wiring new auth methods.

The runnable server this SDK is designed to dial against:

- `/usr/local/src/gnatrix/server_client/build/bin/gnatrixquery` — compiled `gnatrixquery` binary. Start this locally and point `cmd/example` or `cmd/gnatrix-go-smoke` at it to exercise the SDK end-to-end against a real peer (rather than the in-test fake server). Sibling binaries in the same directory: `server`, `gnatrix-migrate`, `gnatrix_password`, `gnatrix_unit_tests`, `gnatrix_integration_tests`, plus a `run` launcher.
