// Package transport opens TLS-secured TCP connections to a gnatrixquery
// server. It is the underlying byte pipe that the wire codec then speaks
// over; it knows nothing about gnatrix frames.
package transport

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"
)

// Default timeouts (also applied if the caller passes a zero value).
const (
	DefaultDialTimeout      = 5 * time.Second
	DefaultHandshakeTimeout = 10 * time.Second
)

// DialTLS opens a TCP connection to addr, performs a TLS 1.3 handshake,
// and returns the established *tls.Conn.
//
// addr must be in "host:port" form (matching net.Dial).
//
// If tlsCfg is nil, a fresh tls.Config is built with the system root CAs
// and ServerName=host(addr). If tlsCfg is non-nil it is cloned (we never
// mutate the caller's config) and ServerName is filled in from host(addr)
// when empty.
//
// MinVersion is always forced to TLS 1.3: wire-protocol.md §Transport
// makes TLS 1.3 a hard requirement, so the SDK refuses to downgrade even
// if the caller's tls.Config would otherwise allow it.
//
// A zero dialTimeout or handshakeTimeout falls back to the package defaults.
func DialTLS(
	ctx context.Context,
	addr string,
	tlsCfg *tls.Config,
	dialTimeout time.Duration,
	handshakeTimeout time.Duration,
) (*tls.Conn, error) {
	if dialTimeout <= 0 {
		dialTimeout = DefaultDialTimeout
	}
	if handshakeTimeout <= 0 {
		handshakeTimeout = DefaultHandshakeTimeout
	}

	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("transport: invalid addr %q: %w", addr, err)
	}

	cfg := buildTLSConfig(tlsCfg, host)

	dialer := &net.Dialer{Timeout: dialTimeout}
	rawConn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("transport: dial tcp %s: %w", addr, err)
	}

	tlsConn := tls.Client(rawConn, cfg)

	hsCtx, cancel := context.WithTimeout(ctx, handshakeTimeout)
	defer cancel()

	if err := tlsConn.HandshakeContext(hsCtx); err != nil {
		_ = tlsConn.Close()
		return nil, fmt.Errorf("transport: tls handshake to %s: %w", addr, err)
	}

	return tlsConn, nil
}

// buildTLSConfig clones the caller's config (or builds a default) and
// pins MinVersion to TLS 1.3.
func buildTLSConfig(in *tls.Config, host string) *tls.Config {
	var cfg *tls.Config
	if in == nil {
		cfg = &tls.Config{ServerName: host}
	} else {
		cfg = in.Clone()
		if cfg.ServerName == "" {
			cfg.ServerName = host
		}
	}
	cfg.MinVersion = tls.VersionTLS13
	return cfg
}
