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
	"os"
	"sync"
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
	conn    *tls.Conn
	session Session

	// mu serializes wire round-trips on conn. Without it, two concurrent
	// Pings (or future post-handshake operations) would interleave their
	// frame writes and reads on the same TLS stream.
	mu sync.Mutex

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

	return &Client{conn: conn, session: session}, nil
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

	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > fallbackTimeout {
		deadline = time.Now().Add(fallbackTimeout)
	}
	if err := c.conn.SetDeadline(deadline); err != nil {
		return 0, fmt.Errorf("gnatrix: ping set deadline: %w", err)
	}
	defer c.conn.SetDeadline(time.Time{})

	start := time.Now()
	if _, err := c.conn.Write(wire.EncodePing()); err != nil {
		return 0, fmt.Errorf("gnatrix: send ping: %w", err)
	}

	hdr, err := wire.ReadHeader(c.conn)
	if err != nil {
		return 0, fmt.Errorf("gnatrix: read pong: %w", err)
	}
	if hdr.Type != wire.FramePong {
		return 0, fmt.Errorf("gnatrix: expected PONG (0x21), got 0x%02x", byte(hdr.Type))
	}
	if hdr.PayloadLen != 0 {
		return 0, fmt.Errorf("gnatrix: PONG has non-zero payload (%d bytes)", hdr.PayloadLen)
	}

	return time.Since(start), nil
}

// Close shuts down the underlying connection. Safe to call multiple times.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.conn.Close()
	})
	return c.closeErr
}

// handshake runs the gnatrix-level HELLO/WELCOME exchange on an already-
// established TLS connection. It uses ctx's deadline when present and
// otherwise falls back to cfg.HandshakeTimeout.
func handshake(ctx context.Context, conn *tls.Conn, cfg Config, clientVersion string) (Session, error) {
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
