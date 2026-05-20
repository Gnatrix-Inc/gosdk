# gnatrix-gosdk

Go SDK for the [gnatrix](https://github.com/gnatrix) query engine. Dials a
server over TLS 1.3, authenticates with a `gnx_...` API token scoped to a
tenant, and exposes a thin client with `Ping` / `Session` / `Close`.

Requires Go 1.22+. Zero external dependencies (stdlib only).

## Install

```
go get github.com/gnatrix/gnatrix-gosdk
```

## Quickstart

```go
package main
import ("context"; "fmt"; "github.com/gnatrix/gnatrix-gosdk")

func main() {
    c, err := gnatrix.Dial(context.Background(), gnatrix.Config{Addr: "localhost:7777", Token: "gnx_demo", TenantSlug: "acme"})
    if err != nil { panic(err) }
    defer c.Close()
    rtt, _ := c.Ping(context.Background())
    fmt.Printf("session %d rtt %v\n", c.Session().SessionID, rtt)
}
```

Save as `main.go` and run with `go run .`. See [`cmd/example/`](./cmd/example/)
for a flag-driven variant and [`cmd/gnatrix-go-smoke/`](./cmd/gnatrix-go-smoke/)
for a non-interactive CI smoke binary.

## Error handling

Server ERROR frames with code in **2001..2010** (auth/session range) come
back as `*gnatrix.AuthError` recoverable via `errors.As`:

```go
var authErr *gnatrix.AuthError
if errors.As(err, &authErr) {
    // authErr.Code, authErr.Message
}
```

Context cancellation is honored both pre-Dial and mid-handshake; the SDK
returns within ~1 s of cancellation with `errors.Is(err, context.Canceled)`.

## Status — Slice 0

In: TCP + TLS 1.3 transport, HELLO/WELCOME handshake with `auth_method=1`
(api_token), PING/PONG keepalive.

Out (deferred): queries (`QUERY_*` frames), additional auth methods (mTLS,
password, superuser_peer), retry/reconnect, connection pooling,
compression, OpenTelemetry tracing, Prometheus metrics, Unix socket
transport.
