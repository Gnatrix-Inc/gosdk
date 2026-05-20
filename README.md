# gnatrix-gosdk

Go SDK for the [gnatrix](https://github.com/gnatrix) query engine. Dials a
server over TLS 1.3, authenticates with a `gnx_...` API token scoped to a
tenant, and exposes a thin client for keepalive (`Ping`) and streaming
queries (`Query` → `QueryStream`).

Requires Go 1.22+. Zero external dependencies (stdlib only).

**TL;DR for the query API:** see [QUERY_QUICKSTART.md](./QUERY_QUICKSTART.md)
— copy-paste-friendly onboarding (Dial → Query → Next → Result) with
error handling, TimeRange, pagination, and the lifecycle rules.

## Install

```
go get github.com/Gnatrix-Inc/gosdk
```

## Quickstart

```go
package main

import (
    "context"
    "errors"
    "fmt"
    "io"

    gnatrix "github.com/Gnatrix-Inc/gosdk"
)

func main() {
    ctx := context.Background()

    c, err := gnatrix.Dial(ctx, gnatrix.Config{
        Addr:       "localhost:7777",
        Token:      "gnx_...",
        TenantSlug: "acme",
    })
    if err != nil {
        panic(err)
    }
    defer c.Close()

    rtt, _ := c.Ping(ctx)
    fmt.Printf("session %d rtt %v\n", c.Session().SessionID, rtt)

    // Streaming query. The iterator yields one row at a time; io.EOF
    // signals a clean QUERY_END status=0.
    stream, err := c.Query(ctx, "search error", gnatrix.QueryOptions{
        IndexName: "logs-2026",
        Limit:     100,
    })
    if err != nil {
        panic(err)
    }
    defer stream.Close()

    for {
        row, err := stream.Next(ctx)
        if errors.Is(err, io.EOF) {
            break
        }
        if err != nil {
            panic(err)
        }
        fmt.Println(row)
    }

    res := stream.Result()
    fmt.Printf("rows=%d events_scanned=%d elapsed_ms=%d\n",
        res.RowsReturned, res.EventsScanned, res.ElapsedMs)
}
```

## Query API

The SDK is **streaming-only** by design — there is no `QueryAll()` /
`Collect()` convenience. Callers that want every row in memory write the
loop themselves and accumulate into their own slice. The unbuffered rows
channel inside the SDK propagates a slow consumer as TCP backpressure on
the socket; that is the only throughput control.

**Always `defer stream.Close()`** right after a successful `Query()`.
The streaming iterator claims an internal one-in-flight slot on the
`*Client`; that slot is released either by draining `Next()` to a
terminal (`io.EOF` or a typed error) **or** by `Close()`. Calling
both is harmless — `Close()` after a natural drain is a no-op. But
abandoning the stream mid-iteration without `Close()` leaks the slot:
every subsequent `Query()` on the same `*Client` returns
`ErrQueryInFlight` until the connection is torn down.

### `Client.Query(ctx, queryText, opts) (*QueryStream, error)`

Issues a `QUERY_REQUEST` and returns a stream. Generates a monotonic
`queryID` (starts at 1) atomically and only on a successful slot claim —
a rejected `Query` does not burn a counter value.

```go
opts := gnatrix.QueryOptions{
    IndexName:          "logs-2026", // empty defaults to "default"
    Limit:              500,         // 0 = no cap
    Cursor:             "",          // pagination cursor from a previous result
    TimeRange:          &gnatrix.TimeRange{
        EarliestNs: 1_700_000_000_000_000_000,
        LatestNs:   1_700_000_010_000_000_000,
    },
    ProgressIntervalMs: 250, // 0 = server default
}
stream, err := c.Query(ctx, "search error | top 10 host", opts)
```

Returns `ErrQueryInFlight` (without touching the wire) if another query
is already active on the same `*Client`. The server enforces one
in-flight query per session; the SDK mirrors that locally so the second
call fails before any bytes are written.

### `(*QueryStream).Next(ctx) (Row, error)`

Returns the next streamed row. Terminal conditions:

- `io.EOF` — `QUERY_END` with `status=0`. `Result()` returns valid totals.
- `*QueryError` — `QUERY_END` with `status≠0`. `Result()` still returns
  totals (events scanned, elapsed, etc. — useful even on failure).
- `*QueryRejectError` — server `ERROR` with code in 2011..2019 (pre-
  execution rejection). `Result()` returns nil.
- `ErrStreamClosed` — `Close()` was called before a natural terminal.
- `ctx.Err()` — the per-call context was canceled or deadlined.
- Wrapped session-fatal error — the read loop died (malformed frame,
  ERROR outside 2011..2019, TCP disconnect, etc.); session is unusable.

After any non-nil error from `Next`, the iterator is finished; further
calls return the same kind of terminal.

### `(*QueryStream).Result() *QueryResult`

Totals from `QUERY_END`. **Panics if called before the stream is
drained** — that is a programmer error, not a runtime fault. Returns
`nil` when the stream terminated via a pre-execution rejection (no
`QUERY_END` was emitted).

### `(*QueryStream).Close() error`

Idempotent. Marks the stream abandoned and returns immediately; a
background goroutine drains any frames the dispatcher still delivers
for this query until `QUERY_END` (or a pre-execution `ERROR`) arrives,
then releases the in-flight slot so a subsequent `Query()` can proceed.

## Error handling

Three typed errors are recoverable via `errors.As`:

```go
var authErr *gnatrix.AuthError
if errors.As(err, &authErr) {
    // Dial-time auth/session failure (code 2001..2010).
    // authErr.Code, authErr.Message
}

var rej *gnatrix.QueryRejectError
if errors.As(err, &rej) {
    // Pre-execution rejection from Next (code 2011..2019).
    // rej.Code, rej.Message
}

var qerr *gnatrix.QueryError
if errors.As(err, &qerr) {
    // Post-execution failure from Next (status 1..7).
    // qerr.Status, qerr.Message, qerr.EngineCode, qerr.QueryID
}
```

Sentinel errors are matched with `errors.Is`:

- `gnatrix.ErrQueryInFlight` — second `Query()` while one is active.
- `gnatrix.ErrStreamClosed` — `Next()` after `Close()`.

Context cancellation is honored at every wait point: pre-Dial, mid-
handshake, mid-`Ping`, and mid-`Next`. The SDK returns within ~1 s of
cancellation with `errors.Is(err, context.Canceled)`.

## Status — Slice 1 complete

**In:**

- TCP + TLS 1.3 transport with mandatory TLS 1.3 floor.
- `HELLO`/`WELCOME` handshake with `auth_method=1` (api_token) and
  typed `AuthError` for codes 2001..2010.
- `PING`/`PONG` keepalive via single-slot waiter on a long-running
  read loop.
- `QUERY_REQUEST` / `QUERY_ROW` / `QUERY_END` / `QUERY_PROGRESS`
  codecs aligned with wire-protocol v2 (extended request fields,
  reordered end fields, new status codes 5/6/7).
- Session state machine: dispatcher routes by frame type and
  `query_id`, enforces one in-flight query client-side, surfaces
  `*QueryRejectError` for ERROR 2011..2019, marks the session
  terminal on any other ERROR or malformed payload.
- Public `Query` / `QueryStream.{Next,Result,Close}` with streaming
  iteration, `engine_code` parsing from the `"NNNN: msg"` prefix in
  `QUERY_END.message` when `status=2`.

**Out (deferred):**

- `QUERY_CANCEL` (0x13) wire frame — Slice 2.
- Surfacing `QUERY_PROGRESS` to callers — Slice 2. The dispatcher
  decodes and discards progress today.
- Other auth methods (mTLS, password, superuser_peer).
- Retry / reconnect logic, connection pooling.
- Compression, OpenTelemetry tracing, Prometheus metrics — Slice 3.
- Unix socket transport (operator-only).
- Multiplexing multiple queries per session.
