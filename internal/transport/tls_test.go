package transport

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"
)

func TestDialTLS_HandshakeIsTLS13(t *testing.T) {
	cert, leaf := newSelfSignedCert(t, "localhost")

	srv := newTLSServer(t, &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pool := x509.NewCertPool()
	pool.AddCert(leaf)

	conn, err := DialTLS(ctx, srv.addr(), &tls.Config{
		RootCAs:    pool,
		ServerName: "localhost",
	}, 2*time.Second, 2*time.Second)
	if err != nil {
		t.Fatalf("DialTLS: %v", err)
	}
	defer conn.Close()

	state := conn.ConnectionState()
	if state.Version != tls.VersionTLS13 {
		t.Errorf("negotiated TLS version = 0x%04x, want 0x%04x (TLS 1.3)",
			state.Version, tls.VersionTLS13)
	}
	if !state.HandshakeComplete {
		t.Error("HandshakeComplete = false")
	}
}

func TestDialTLS_RejectsTLS12Server(t *testing.T) {
	cert, leaf := newSelfSignedCert(t, "localhost")

	// Server caps at TLS 1.2 — the SDK must refuse to talk to it.
	srv := newTLSServer(t, &tls.Config{
		Certificates: []tls.Certificate{cert},
		MaxVersion:   tls.VersionTLS12,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pool := x509.NewCertPool()
	pool.AddCert(leaf)

	conn, err := DialTLS(ctx, srv.addr(), &tls.Config{
		RootCAs:    pool,
		ServerName: "localhost",
	}, 2*time.Second, 2*time.Second)
	if err == nil {
		conn.Close()
		t.Fatal("expected DialTLS to fail (server forced TLS 1.2), got nil error")
	}
	if !strings.Contains(err.Error(), "tls handshake") {
		t.Errorf("error should mention tls handshake, got: %v", err)
	}
}

func TestDialTLS_InvalidAddr(t *testing.T) {
	_, err := DialTLS(context.Background(), "not-a-valid-addr", nil, 1*time.Second, 1*time.Second)
	if err == nil {
		t.Fatal("expected error for invalid addr, got nil")
	}
	if !strings.Contains(err.Error(), "invalid addr") {
		t.Errorf("error should mention invalid addr, got: %v", err)
	}
}

// ---- test helpers ------------------------------------------------------

type testServer struct {
	listener net.Listener
}

func (s *testServer) addr() string { return s.listener.Addr().String() }

// newTLSServer starts an in-memory TLS listener that accepts and immediately
// closes one connection, then shuts down at test teardown.
func newTLSServer(t *testing.T, cfg *tls.Config) *testServer {
	t.Helper()
	l, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("tls.Listen: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	go func() {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		// Drive the handshake on the server side, then close.
		_ = conn.(*tls.Conn).HandshakeContext(context.Background())
		_ = conn.Close()
	}()

	return &testServer{listener: l}
}

// newSelfSignedCert generates an ed25519 self-signed cert valid for host
// (and 127.0.0.1). Returns the tls.Certificate for the server and the
// parsed *x509.Certificate so the client can pin it as a trusted root.
func newSelfSignedCert(t *testing.T, host string) (tls.Certificate, *x509.Certificate) {
	t.Helper()

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("rand.Int: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: host},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:              []string{host},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, priv)
	if err != nil {
		t.Fatalf("x509.CreateCertificate: %v", err)
	}

	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("x509.ParseCertificate: %v", err)
	}

	return tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  priv,
		Leaf:        leaf,
	}, leaf
}
