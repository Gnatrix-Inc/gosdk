// Package gnatrix is the Go SDK for gnatrixquery.
//
// Slice 0 surface: Dial opens a TLS 1.3 connection, performs the
// HELLO/WELCOME handshake with an api_token credential, and returns a
// *Client whose Session() reflects the server-issued session. Ping keeps
// the connection live; Close tears it down.
package gnatrix

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Gnatrix-Inc/gosdk/internal/transport"
	"github.com/Gnatrix-Inc/gosdk/internal/wire"
)

const defaultClientVersion = "gnatrix-go/0.0.1"

// Config configures a Dial call.
type Config struct {
	// Addr is the server address in "host:port" form.
	Addr string

	// Token is the raw "gnx_..." API token (auth_method=1).
	Token string

	// TenantSlug names the tenant to log into.
	TenantSlug string

	// CACertPath is the path to a PEM-encoded CA certificate file used to
	// verify the server's TLS certificate. When set, only this CA is trusted
	// (system roots are not included). Mutually exclusive with TLSConfig.
	CACertPath string

	// TLSConfig optionally overrides the default TLS settings. When nil,
	// the SDK uses system root CAs with ServerName set to the host of Addr.
	// MinVersion is always pinned to TLS 1.3 (wire-protocol.md §Transport).
	// Mutually exclusive with CACertPath.
	TLSConfig *tls.Config

	// DialTimeout caps the TCP dial. Defaults to 5s when zero.
	DialTimeout time.Duration

	// HandshakeTimeout caps each of the TLS handshake and the gnatrix
	// HELLO/WELCOME exchange. Defaults to 10s when zero.
	HandshakeTimeout time.Duration

	// ClientVersion is the version tag sent in HELLO. Defaults to
	// "gnatrix-go/0.0.1" when empty.
	ClientVersion string
}

// Client is a live, authenticated connection to a gnatrixquery server.
type Client struct {
	// conn is held as net.Conn so tests can substitute a stub. In
	// production this is always *tls.Conn from transport.DialTLS; the
	// TLS-specific API is not used after handshake.
	conn    net.Conn
	session Session

	// mu serializes wire writes on conn so concurrent Ping and (future)
	// Query callers do not interleave their frame bytes. Reads are owned
	// exclusively by readLoop and do not take this lock.
	mu sync.Mutex

	// waiterMu guards pingWaiter, activeQuery, and readErr. Held briefly
	// by the read loop and by Ping/Query; never held across I/O.
	waiterMu    sync.Mutex
	pingWaiter  chan time.Time
	activeQuery *queryState
	readErr     error

	// readDone is closed when readLoop exits. Used by waiters to fail
	// fast when the read loop itself has terminated (e.g. after the
	// conn was closed and the next ReadHeader failed).
	readDone chan struct{}

	// terminating is closed synchronously by Client.Close BEFORE the
	// conn is closed. Distinct from readDone because Close cannot
	// rely on readDone to fire promptly: if readLoop is blocked
	// inside a dispatch send (e.g. q.rows <- msg with the iterator
	// abandoned), conn.Close will not unblock it — closing the conn
	// only wakes pending Reads, not in-flight channel Sends. The
	// dispatcher's select includes <-c.terminating so it can escape
	// regardless of where it is.
	terminating chan struct{}

	// queryCounter generates QueryID values for QUERY_REQUEST frames.
	// Monotonic, starts at 1 (the first Add(1) returns 1). Invisible to
	// the caller; surfaced in QueryResult.QueryID for log correlation.
	queryCounter atomic.Uint64

	closeOnce sync.Once
	closeErr  error
}

// Session is the server-issued session, populated by Dial from the WELCOME
// frame. Its value is stable for the lifetime of the *Client.
type Session struct {
	SessionID   uint64
	TenantID    [16]byte
	UserID      [16]byte
	Permissions []string
	ExpiresAt   time.Time
	IssuedToken string // empty for api_token auth
}

// AuthError is returned by Dial when the server replies with an ERROR
// frame whose code is in the 2001..2010 auth/session range. Callers can
// use errors.As to recover it.
type AuthError struct {
	Code    uint32
	Message string
}

func (e *AuthError) Error() string {
	return fmt.Sprintf("gnatrix: auth error %d: %s", e.Code, e.Message)
}

// ---- Query API (Slice 1) -----------------------------------------------
//
// Type declarations live here; method bodies for Client.Query and
// QueryStream live in query.go.

// QueryOptions configures a Client.Query call.
//
// All fields are optional. Zero-value semantics:
//
//	IndexName == ""             → server applies "default"
//	Limit == 0                  → no cap; server streams every match
//	Cursor == ""                → fresh query (no pagination resume)
//	TimeRange == nil            → server's default window (15m ending now,
//	                              configurable per tenant)
//	ProgressIntervalMs == 0     → server's default progress cadence (250ms)
//
// TimeRange is the only cross-field invariant: when non-nil, both
// EarliestNs and LatestNs must be set (the SDK trusts the caller; the
// server rejects latest < earliest with ERROR 2013 QueryInvalidTimeRange).
// To paginate, copy QueryResult.NextCursor from a prior successful
// query into Cursor on the next call.
//
// QueryOptions is value-typed; an empty zero value is a valid call.
type QueryOptions struct {
	// IndexName names the target index, e.g. "logs-2026". Empty defaults
	// to "default" at the Query call site.
	IndexName string

	// Limit is the maximum number of rows the client wants. 0 means no cap.
	Limit uint64

	// Cursor is an opaque pagination cursor from a previous QueryResult.
	// Empty when starting a fresh query.
	Cursor string

	// TimeRange constrains the query to a half-open ns window. Nil means
	// the server applies its default window.
	TimeRange *TimeRange

	// ProgressIntervalMs requests QUERY_PROGRESS cadence in milliseconds.
	// 0 means use the server default (250 ms).
	ProgressIntervalMs uint32
}

// TimeRange is a half-open [EarliestNs, LatestNs) window in
// nanoseconds since the Unix epoch.
//
// Both fields are int64. The wire layer reinterprets the bit pattern
// as uint64 via two's-complement (matching the server's
// std::bit_cast<uint64_t>) and emits it as an unsigned LEB128 varint
// — so any int64 including math.MinInt64 round-trips losslessly.
// Relative-time expressions ("-15m", "now") are NOT understood by the
// SDK or server; resolve them caller-side before populating the
// fields.
//
// Use time.Time.UnixNano() to derive the values:
//
//	tr := &gnatrix.TimeRange{
//	    EarliestNs: time.Now().Add(-1 * time.Hour).UnixNano(),
//	    LatestNs:   time.Now().UnixNano(),
//	}
//
// The server rejects latest < earliest with ERROR 2013
// (QueryInvalidTimeRange) before binding a query_id; the SDK surfaces
// that as *QueryRejectError on the first Next() call.
type TimeRange struct {
	EarliestNs int64
	LatestNs   int64
}

// Row is one streamed query result, decoded from the canonical JSON
// payload of a QUERY_ROW frame.
type Row = map[string]any

// QueryResult carries the totals from the terminating QUERY_END frame.
// Available only after Rows() has been fully drained.
type QueryResult struct {
	QueryID       uint64
	RowsReturned  uint64
	EventsScanned uint64
	EventsMatched uint64
	ElapsedMs     uint64
	Truncated     bool
	NextCursor    string
}

// QueryError is returned for query-level failures reported via QUERY_END
// with a non-zero status. The session stays open after a QueryError;
// the caller may issue another Query.
//
// Status values (see wire-protocolv2.md §QUERY_END):
//
//	1 = cancelled
//	2 = engine_error              (EngineCode parsed from "NNNN: msg" prefix)
//	3 = permission_denied
//	4 = tenant_quota_exceeded
//	5 = memory_limit_exceeded
//	6 = timeout
//	7 = storage_unavailable
type QueryError struct {
	Status     uint32
	Message    string
	EngineCode uint32
	QueryID    uint64
}

func (e *QueryError) Error() string {
	return fmt.Sprintf("gnatrix: query error status=%d code=%d: %s", e.Status, e.EngineCode, e.Message)
}

// QueryRejectError is returned for pre-execution rejections delivered as
// an ERROR frame (codes 2011..2019). The session stays open; no QUERY_END
// is emitted for the rejected request.
type QueryRejectError struct {
	Code    uint32
	Message string
}

func (e *QueryRejectError) Error() string {
	return fmt.Sprintf("gnatrix: query rejected %d: %s", e.Code, e.Message)
}

// ErrQueryInFlight is returned by Client.Query when another query is
// already in flight on the same Client. The server enforces one
// in-flight query per session (ERROR 2019); the SDK enforces it locally
// so the second call fails before any bytes hit the wire.
var ErrQueryInFlight = errors.New("gnatrix: query already in flight on this client")

// ErrQueryTooLarge is returned by Client.Query when the queryText
// alone exceeds the wire frame payload limit (65 536 bytes).
//
// # What this catches
//
// The obvious footgun: queryText so large it cannot possibly fit in
// a single wire frame regardless of the other fields' overhead.
// Sending such a frame would be rejected by the server as ERROR
// 1001 InvalidFrame and would close the session — killing the
// *Client. Pre-rejecting client-side keeps the session alive.
//
// # What this does NOT catch
//
// A queryText shorter than MaxPayload but large enough that the
// encoded frame, after adding the overhead from QueryID, IndexName,
// Cursor and length-prefix varints, still exceeds MaxPayload. The
// check is a conservative `len(queryText) > MaxPayload` precisely
// to avoid duplicating the encoder's overhead math here; the gap
// (between roughly MaxPayload − ~20 bytes and MaxPayload itself) is
// uncovered. Frames in that band still go to the wire, the server
// rejects them at the frame layer, and the *Client terminates.
// Future tightening could pre-encode and measure (more allocation
// but exact) — deferred.
//
// # Tenant-level cap is server-side
//
// The server enforces the per-tenant max_query_text_bytes (which
// may be lower than MaxPayload) and surfaces overflow as
// *QueryRejectError with code 2018 (QueryTooLarge) via Next. That
// is a runtime quota check; ErrQueryTooLarge is purely the wire
// floor.
var ErrQueryTooLarge = errors.New("gnatrix: queryText exceeds wire frame size limit")

// Dial opens a TLS 1.3 connection to cfg.Addr, performs the HELLO/WELCOME
// handshake using cfg.Token as an api_token credential, and returns a
// ready *Client. Auth failures (server ERROR code in 2001..2010) are
// returned as *AuthError.
func Dial(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.Addr == "" {
		return nil, errors.New("gnatrix: Config.Addr is required")
	}
	if cfg.Token == "" {
		return nil, errors.New("gnatrix: Config.Token is required")
	}
	if cfg.TenantSlug == "" {
		return nil, errors.New("gnatrix: Config.TenantSlug is required")
	}

	if cfg.CACertPath != "" && cfg.TLSConfig != nil {
		return nil, errors.New("gnatrix: CACertPath and TLSConfig are mutually exclusive")
	}

	clientVersion := cfg.ClientVersion
	if clientVersion == "" {
		clientVersion = defaultClientVersion
	}

	tlsCfg := cfg.TLSConfig
	if cfg.CACertPath != "" {
		tc, err := tlsConfigFromCA(cfg.CACertPath)
		if err != nil {
			return nil, err
		}
		tlsCfg = tc
	}

	conn, err := transport.DialTLS(ctx, cfg.Addr, tlsCfg, cfg.DialTimeout, cfg.HandshakeTimeout)
	if err != nil {
		return nil, err
	}

	session, err := handshake(ctx, conn, cfg, clientVersion)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	c := &Client{
		conn:        conn,
		session:     session,
		readDone:    make(chan struct{}),
		terminating: make(chan struct{}),
	}
	go c.readLoop()
	return c, nil
}

// Session returns the session minted by the server during Dial.
func (c *Client) Session() Session {
	return c.session
}

// Ping sends a PING and waits for a PONG, returning the round-trip duration.
// Ping respects ctx.Deadline() and otherwise caps the wait at 10s.
func (c *Client) Ping(ctx context.Context) (time.Duration, error) {
	const fallbackTimeout = 10 * time.Second

	c.mu.Lock()
	defer c.mu.Unlock()

	// Fail fast if the read loop has already terminated (e.g. after Close).
	select {
	case <-c.readDone:
		return 0, pingTerminalErr(c.terminalErr())
	default:
	}

	waiter := make(chan time.Time, 1)
	c.waiterMu.Lock()
	c.pingWaiter = waiter
	c.waiterMu.Unlock()
	defer func() {
		c.waiterMu.Lock()
		c.pingWaiter = nil
		c.waiterMu.Unlock()
	}()

	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > fallbackTimeout {
		deadline = time.Now().Add(fallbackTimeout)
	}

	start := time.Now()
	if _, err := c.conn.Write(wire.EncodePing()); err != nil {
		return 0, fmt.Errorf("gnatrix: send ping: %w", err)
	}

	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()

	select {
	case <-waiter:
		return time.Since(start), nil
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-c.readDone:
		return 0, pingTerminalErr(c.terminalErr())
	case <-timer.C:
		return 0, fmt.Errorf("gnatrix: ping timeout after %s", time.Since(start))
	}
}

func pingTerminalErr(cause error) error {
	if cause == nil {
		return errors.New("gnatrix: ping on closed client")
	}
	return fmt.Errorf("gnatrix: ping: %w", cause)
}

// Close shuts down the underlying connection. Safe to call multiple
// times. Closes c.terminating BEFORE c.conn so any dispatcher
// currently blocked inside a channel send (e.g. an unbuffered
// q.rows<-msg when the iterator was abandoned without Close) can
// escape via the select branch on <-c.terminating. Without this
// ordering, the read loop would remain pinned and readDone would
// never close.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		close(c.terminating)
		c.closeErr = c.conn.Close()
	})
	return c.closeErr
}

// handshake runs the gnatrix-level HELLO/WELCOME exchange on an already-
// established TLS connection. It uses ctx's deadline when present and
// otherwise falls back to cfg.HandshakeTimeout.
func handshake(ctx context.Context, conn net.Conn, cfg Config, clientVersion string) (Session, error) {
	hsTimeout := cfg.HandshakeTimeout
	if hsTimeout <= 0 {
		hsTimeout = transport.DefaultHandshakeTimeout
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > hsTimeout {
		deadline = time.Now().Add(hsTimeout)
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return Session{}, fmt.Errorf("gnatrix: set deadline: %w", err)
	}
	defer conn.SetDeadline(time.Time{})

	// Wire ctx cancellation through to in-flight conn I/O. tls.Conn has no
	// ctx-aware Read/Write, so when ctx is cancelled we collapse the conn
	// deadline to a past instant; that aborts any blocked Read/Write with
	// os.ErrDeadlineExceeded, and the per-stage error wrapping below
	// substitutes ctx.Err() so callers can errors.Is it.
	handshakeDone := make(chan struct{})
	defer close(handshakeDone)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.SetDeadline(time.Unix(1, 0))
		case <-handshakeDone:
		}
	}()

	helloFrame := wire.EncodeHello(wire.HelloMsg{
		AuthMethod:    wire.AuthMethodAPIToken,
		TenantSlug:    cfg.TenantSlug,
		Credential:    []byte(cfg.Token),
		ClientVersion: clientVersion,
	})
	if _, err := conn.Write(helloFrame); err != nil {
		return Session{}, handshakeStageErr(ctx, "send hello", err)
	}

	hdr, err := wire.ReadHeader(conn)
	if err != nil {
		return Session{}, handshakeStageErr(ctx, "read handshake response", err)
	}
	payload, err := wire.ReadPayload(conn, hdr.PayloadLen)
	if err != nil {
		return Session{}, handshakeStageErr(ctx, "read handshake payload", err)
	}

	switch hdr.Type {
	case wire.FrameWelcome:
		welcome, err := wire.DecodeWelcome(bytes.NewReader(payload))
		if err != nil {
			return Session{}, fmt.Errorf("gnatrix: decode welcome: %w", err)
		}
		return Session{
			SessionID:   welcome.SessionID,
			UserID:      welcome.UserID,
			TenantID:    welcome.TenantID,
			Permissions: welcome.Permissions,
			ExpiresAt:   time.Unix(int64(welcome.SessionExpiresAtSec), 0),
			IssuedToken: welcome.IssuedToken,
		}, nil

	case wire.FrameError:
		errMsg, derr := wire.DecodeError(bytes.NewReader(payload))
		if derr != nil {
			return Session{}, fmt.Errorf("gnatrix: decode error frame: %w", derr)
		}
		// 2001..2010 are reserved for auth/session-level failures; surface
		// them as *AuthError so callers can errors.As them.
		if errMsg.Code >= 2001 && errMsg.Code <= 2010 {
			return Session{}, &AuthError{Code: errMsg.Code, Message: errMsg.Message}
		}
		return Session{}, fmt.Errorf("gnatrix: server error %d: %s", errMsg.Code, errMsg.Message)

	default:
		return Session{}, fmt.Errorf("gnatrix: unexpected frame 0x%02x after HELLO", byte(hdr.Type))
	}
}

// tlsConfigFromCA builds a tls.Config whose RootCAs pool contains only the
// PEM-encoded certificate(s) in the file at path.
func tlsConfigFromCA(path string) (*tls.Config, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("gnatrix: read CA cert %s: %w", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("gnatrix: no valid PEM certificates in %s", path)
	}
	return &tls.Config{RootCAs: pool}, nil
}

// handshakeStageErr substitutes ctx.Err() when ctx was cancelled mid-stage,
// so a deadline-collapse driven by the cancellation watcher surfaces as
// context.Canceled / context.DeadlineExceeded instead of a bare i/o timeout.
func handshakeStageErr(ctx context.Context, stage string, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return fmt.Errorf("gnatrix: %s: %w", stage, err)
}
