package gnatrix

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/gnatrix/gnatrix-gosdk/internal/wire"
)

const (
	testTenantSlug = "acme"
	testToken      = "gnx_test_0123456789abcdef0123456789abcdef"
)

func TestDial_HappyPath_SessionPopulated(t *testing.T) {
	fake := newFakeGnatrixServer(t, fakeServerOpts{
		validTenantSlug: testTenantSlug,
		validToken:      []byte(testToken),
		welcome: wire.WelcomeMsg{
			SessionID:          42,
			SessionExpiresAtMs: uint64(time.Now().Add(time.Hour).UnixMilli()),
			UserID:             [16]byte{0xaa},
			TenantID:           [16]byte{0xbb},
			Permissions:        []string{"query:read", "query:write"},
		},
	})

	client, err := dialFake(t, fake, testToken, testTenantSlug)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer client.Close()

	sess := client.Session()
	if sess.SessionID == 0 {
		t.Errorf("Session().SessionID = 0; want > 0")
	}
	if !slices.Contains(sess.Permissions, "query:read") {
		t.Errorf("Session().Permissions = %v; want to contain %q", sess.Permissions, "query:read")
	}
}

func TestDial_HelloAPIToken_ReturnsWelcome(t *testing.T) {
	fake := newFakeGnatrixServer(t, fakeServerOpts{
		validTenantSlug: testTenantSlug,
		validToken:      []byte(testToken),
		welcome: wire.WelcomeMsg{
			SessionID:          7,
			SessionExpiresAtMs: uint64(time.Now().Add(time.Hour).UnixMilli()),
		},
	})

	client, err := dialFake(t, fake, testToken, testTenantSlug)
	if err != nil {
		t.Fatalf("Dial returned error (expected WELCOME): %v", err)
	}
	defer client.Close()

	var hello wire.HelloMsg
	select {
	case hello = <-fake.helloReceived:
	case <-time.After(time.Second):
		t.Fatal("fake server did not capture a HELLO")
	}

	// Lock down the on-wire shape of HELLO for auth_method=1.
	if hello.AuthMethod != 1 {
		t.Errorf("HELLO.AuthMethod = %d; want 1 (api_token)", hello.AuthMethod)
	}
	if !bytes.Equal(hello.Credential, []byte(testToken)) {
		t.Errorf("HELLO.Credential = %q; want %q", hello.Credential, testToken)
	}
	if hello.TenantSlug != testTenantSlug {
		t.Errorf("HELLO.TenantSlug = %q; want %q", hello.TenantSlug, testTenantSlug)
	}
	if hello.Email != "" {
		t.Errorf("HELLO.Email = %q; want empty (api_token carries no email)", hello.Email)
	}
	if hello.ClientCapabilities != 0 {
		t.Errorf("HELLO.ClientCapabilities = %d; want 0 (reserved)", hello.ClientCapabilities)
	}
	if hello.ClientVersion == "" {
		t.Error("HELLO.ClientVersion is empty; want non-empty default")
	}
}

func TestDial_ContextAlreadyCancelled_FailsFastWithCtxErr(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	_, err := Dial(ctx, Config{
		Addr:       "127.0.0.1:1", // unreachable; ctx check should fire first
		Token:      testToken,
		TenantSlug: testTenantSlug,
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Dial succeeded with cancelled ctx; want error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("errors.Is(err, context.Canceled) = false; err = %v", err)
	}
	if elapsed > 50*time.Millisecond {
		t.Errorf("Dial took %v; want <= 50ms (transport must short-circuit on cancelled ctx)", elapsed)
	}
}

func TestDial_ContextDeadlineElapsed_FailsFastWithCtxErr(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
	defer cancel()

	start := time.Now()
	_, err := Dial(ctx, Config{
		Addr:       "127.0.0.1:1",
		Token:      testToken,
		TenantSlug: testTenantSlug,
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Dial succeeded with elapsed deadline; want error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("errors.Is(err, context.DeadlineExceeded) = false; err = %v", err)
	}
	if elapsed > 50*time.Millisecond {
		t.Errorf("Dial took %v; want <= 50ms", elapsed)
	}
}

func TestDial_ContextCancelledMidHandshake_FastFailsWithCtxErr(t *testing.T) {
	// Different from the ContextAlreadyCancelled tests: the ctx is alive
	// when Dial starts, so we get past TLS, past sending HELLO, and end
	// up blocked in ReadHeader waiting for a WELCOME that never comes.
	// Then a goroutine cancels the ctx. The SDK must propagate that to
	// the in-flight read and return promptly with context.Canceled —
	// not wait until HandshakeTimeout elapses.
	fake := newFakeGnatrixServer(t, fakeServerOpts{
		validTenantSlug: testTenantSlug,
		validToken:      []byte(testToken),
		stallAfterHello: true,
	})

	pool := x509.NewCertPool()
	pool.AddCert(fake.cert)

	const (
		cancelAfter      = 50 * time.Millisecond
		handshakeTimeout = 5 * time.Second // deliberately much larger than cancelAfter
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		time.Sleep(cancelAfter)
		cancel()
	}()

	start := time.Now()
	_, err := Dial(ctx, Config{
		Addr:             fake.addr(),
		Token:            testToken,
		TenantSlug:       testTenantSlug,
		TLSConfig:        &tls.Config{RootCAs: pool, ServerName: "localhost"},
		HandshakeTimeout: handshakeTimeout,
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Dial succeeded against a stalling server; want context.Canceled")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("errors.Is(err, context.Canceled) = false; err = %v", err)
	}

	// Must return within 1s of the cancellation firing, well under the
	// 5s HandshakeTimeout. If Dial waited out the conn deadline this
	// would be ~5s.
	maxElapsed := cancelAfter + time.Second
	if elapsed > maxElapsed {
		t.Errorf("Dial took %v; want <= %v — cancellation must interrupt the in-flight read, not wait for HandshakeTimeout",
			elapsed, maxElapsed)
	}
}

func TestClient_Close_IsCleanAndIdempotent(t *testing.T) {
	fake := newFakeGnatrixServer(t, fakeServerOpts{
		validTenantSlug: testTenantSlug,
		validToken:      []byte(testToken),
		welcome: wire.WelcomeMsg{
			SessionID:          1,
			SessionExpiresAtMs: uint64(time.Now().Add(time.Hour).UnixMilli()),
		},
	})

	client, err := dialFake(t, fake, testToken, testTenantSlug)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	// First Close: TLS close_notify + TCP FIN. Should succeed cleanly.
	if err := client.Close(); err != nil {
		t.Errorf("first Close returned %v; want nil", err)
	}

	// Second Close: sync.Once cached the result — must return the same value,
	// must not re-close, must not panic.
	if err := client.Close(); err != nil {
		t.Errorf("second Close returned %v; want nil (idempotent)", err)
	}

	// Operations after Close fail with an error, never panic.
	if _, err := client.Ping(context.Background()); err == nil {
		t.Error("Ping after Close returned nil error; want non-nil")
	}

	// A third Close after a failed Ping is still a no-op.
	if err := client.Close(); err != nil {
		t.Errorf("third Close returned %v; want nil", err)
	}
}

func TestClient_Ping_ConcurrentCallsAreSerialized(t *testing.T) {
	// Locks the sync.Mutex contract in CLAUDE.md ("internal sync.Mutex
	// serializes wire round-trips so concurrent Ping calls are safe").
	//
	// The challenge: tls.Conn.{Read,Write,SetDeadline} are already
	// internally synchronized, so memory races are not the failure mode —
	// PING/PONG disassociation is. To make that observable the fake
	// server processes each PING in its own goroutine with a fixed delay,
	// so the server itself imposes no ordering. The only remaining thing
	// that can force per-Ping wall time ≈ delay × N is the client mutex
	// holding send + read together for each call.
	//
	// Asserted lower bound: (N-1) × delay. A missing mutex would let the
	// client fire all N PINGs near-instantaneously, all PONGs would
	// return after ≈ delay, and wall would collapse to ≈ delay × 1.
	const (
		N           = 10
		serverDelay = 50 * time.Millisecond
	)

	fake := newFakeGnatrixServer(t, fakeServerOpts{
		validTenantSlug: testTenantSlug,
		validToken:      []byte(testToken),
		welcome: wire.WelcomeMsg{
			SessionID:          99,
			SessionExpiresAtMs: uint64(time.Now().Add(time.Hour).UnixMilli()),
		},
		pingResponseDelay: serverDelay,
	})

	client, err := dialFake(t, fake, testToken, testTenantSlug)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer client.Close()

	var (
		wg    sync.WaitGroup
		errs  = make([]error, N)
		start = make(chan struct{})
	)

	wallStart := time.Now()
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			_, errs[idx] = client.Ping(context.Background())
		}(i)
	}
	close(start)
	wg.Wait()
	wall := time.Since(wallStart)

	for i, err := range errs {
		if err != nil {
			t.Errorf("Ping[%d] returned %v; want nil", i, err)
		}
	}

	minExpected := time.Duration(N-1) * serverDelay
	if wall < minExpected {
		t.Errorf("wall time %v < %v ((N-1)=%d × %v) — client.mu did NOT serialize: server processed Pings in parallel",
			wall, minExpected, N-1, serverDelay)
	}
}

func TestClient_Ping_RoundTrip(t *testing.T) {
	fake := newFakeGnatrixServer(t, fakeServerOpts{
		validTenantSlug: testTenantSlug,
		validToken:      []byte(testToken),
		welcome: wire.WelcomeMsg{
			SessionID:          11,
			SessionExpiresAtMs: uint64(time.Now().Add(time.Hour).UnixMilli()),
		},
	})

	client, err := dialFake(t, fake, testToken, testTenantSlug)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer client.Close()

	rtt, err := client.Ping(context.Background())
	if err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if rtt <= 0 {
		t.Errorf("Ping rtt = %v; want > 0", rtt)
	}

	// A second Ping must also succeed — verifies the mutex released cleanly
	// and the conn deadline was reset.
	if _, err := client.Ping(context.Background()); err != nil {
		t.Fatalf("second Ping: %v", err)
	}
}

func TestDial_InvalidToken_ReturnsAuthError2002(t *testing.T) {
	fake := newFakeGnatrixServer(t, fakeServerOpts{
		validTenantSlug: testTenantSlug,
		validToken:      []byte(testToken),
	})

	_, err := dialFake(t, fake, "gnx_wrong_xxxxxxxxxxxxxxxxxxxxxxxxxx", testTenantSlug)
	if err == nil {
		t.Fatal("Dial succeeded with wrong token; want *AuthError")
	}

	var authErr *AuthError
	if !errors.As(err, &authErr) {
		t.Fatalf("Dial returned %T (%v); want *gnatrix.AuthError", err, err)
	}
	if authErr.Code != 2002 {
		t.Errorf("AuthError.Code = %d; want 2002 (InvalidCredentials)", authErr.Code)
	}
}

func TestDial_TLSHandshakeDropped_FastFailsWithWrappedError(t *testing.T) {
	// Listener that accepts the TCP connection and immediately closes it
	// without ever speaking TLS. The SDK's TLS client will send ClientHello
	// and then fail reading ServerHello — the error must surface as a
	// canonical net/tls type the caller can errors.As on, and Dial must
	// return promptly (HandshakeTimeout + 1s slack) — never hang.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		_ = conn.Close()
	}()

	const handshakeTimeout = 500 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	_, err = Dial(ctx, Config{
		Addr:             listener.Addr().String(),
		Token:            testToken,
		TenantSlug:       testTenantSlug,
		TLSConfig:        &tls.Config{ServerName: "localhost"},
		HandshakeTimeout: handshakeTimeout,
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Dial succeeded against a server that drops the TLS handshake; want error")
	}

	// The handshake can drop in three canonical shapes, all of which the
	// caller can pattern-match via errors.As/Is:
	//   - tls.AlertError    — server completes enough of the handshake to send a fatal alert
	//   - *net.OpError      — OS-level reset (ECONNRESET) before/during handshake
	//   - io.EOF            — server closes the TCP conn cleanly mid-handshake (this fake's path)
	// Any of the three is acceptable; what's *not* is an opaque string-only error.
	var alertErr tls.AlertError
	var opErr *net.OpError
	if !errors.As(err, &alertErr) && !errors.As(err, &opErr) && !errors.Is(err, io.EOF) {
		t.Errorf("err = %T (%v); want tls.AlertError, *net.OpError, or io.EOF recoverable via errors.As/Is", err, err)
	}

	if elapsed > handshakeTimeout+time.Second {
		t.Errorf("Dial took %v; want <= %v (HandshakeTimeout + 1s) — must not hang",
			elapsed, handshakeTimeout+time.Second)
	}

	<-serverDone
}

func TestDial_ServerError2010_ReturnsAuthError(t *testing.T) {
	// 2010 is the upper bound of the auth/session error range. The SDK maps
	// the whole 2001..2010 range to *AuthError, so this locks the inclusive
	// upper edge — a regression that changed the bound to `<` would slip
	// past the 2001/2002 tests.
	fake := newFakeGnatrixServer(t, fakeServerOpts{
		validTenantSlug:   testTenantSlug,
		validToken:        []byte(testToken),
		forceErrorCode:    2010,
		forceErrorMessage: "session policy violation",
	})

	_, err := dialFake(t, fake, testToken, testTenantSlug)
	if err == nil {
		t.Fatal("Dial succeeded; want *AuthError")
	}

	var authErr *AuthError
	if !errors.As(err, &authErr) {
		t.Fatalf("Dial returned %T (%v); want *gnatrix.AuthError", err, err)
	}
	if authErr.Code != 2010 {
		t.Errorf("AuthError.Code = %d; want 2010", authErr.Code)
	}
	if authErr.Message != "session policy violation" {
		t.Errorf("AuthError.Message = %q; want %q", authErr.Message, "session policy violation")
	}
}

func TestDial_UnknownTenant_ReturnsAuthError2001(t *testing.T) {
	fake := newFakeGnatrixServer(t, fakeServerOpts{
		validTenantSlug: testTenantSlug,
		validToken:      []byte(testToken),
	})

	_, err := dialFake(t, fake, testToken, "ghost-tenant")
	if err == nil {
		t.Fatal("Dial succeeded with unknown tenant; want *AuthError")
	}

	var authErr *AuthError
	if !errors.As(err, &authErr) {
		t.Fatalf("Dial returned %T (%v); want *gnatrix.AuthError", err, err)
	}
	if authErr.Code != 2001 {
		t.Errorf("AuthError.Code = %d; want 2001 (AuthRequired)", authErr.Code)
	}
}

// dialFake runs Dial against a fake gnatrixquery server with a 5s timeout
// and a TLS config that trusts the fake's self-signed cert.
func dialFake(t *testing.T, fake *fakeGnatrixServer, token, tenant string) (*Client, error) {
	t.Helper()

	pool := x509.NewCertPool()
	pool.AddCert(fake.cert)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	return Dial(ctx, Config{
		Addr:       fake.addr(),
		Token:      token,
		TenantSlug: tenant,
		TLSConfig:  &tls.Config{RootCAs: pool, ServerName: "localhost"},
	})
}

// ---- fake gnatrix server -----------------------------------------------

type fakeServerOpts struct {
	// validTenantSlug + validToken are matched against the HELLO. A mismatch
	// in tenant returns ERROR(2001); a mismatch in token (with matching
	// tenant) returns ERROR(2002); both matching returns WELCOME(welcome).
	validTenantSlug string
	validToken      []byte
	welcome         wire.WelcomeMsg

	// forceErrorCode, when non-zero, makes the server reply to HELLO with
	// ERROR(forceErrorCode, forceErrorMessage) regardless of credentials.
	// Used to exercise codes outside the 2001/2002 mismatch paths.
	forceErrorCode    uint32
	forceErrorMessage string

	// pingResponseDelay, when > 0, delays each PONG by this duration AND
	// processes PINGs in their own goroutines so the server can have N
	// PINGs in flight simultaneously. Used by the concurrent-Ping test to
	// expose any failure of client-side serialization: with this enabled
	// the server itself does not impose ordering, so a missing mutex on
	// the client would manifest as wall-time ≈ delay instead of N × delay.
	pingResponseDelay time.Duration

	// stallAfterHello, when true, makes the server read HELLO and then
	// block reading from the conn forever (until the client closes its
	// end). No WELCOME or ERROR is ever sent. Used to test that the SDK
	// honors ctx cancellation while parked in ReadHeader waiting for the
	// server reply.
	stallAfterHello bool
}

type fakeGnatrixServer struct {
	t        *testing.T
	listener net.Listener
	cert     *x509.Certificate

	// helloReceived carries the decoded HELLO so tests can inspect the
	// frame the SDK actually sent on the wire. Buffered (cap 1) so the
	// server goroutine never blocks on it.
	helloReceived chan wire.HelloMsg
}

func (s *fakeGnatrixServer) addr() string { return s.listener.Addr().String() }

func newFakeGnatrixServer(t *testing.T, opts fakeServerOpts) *fakeGnatrixServer {
	t.Helper()

	serverCert, leaf := newSelfSignedCert(t, "localhost")

	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		MinVersion:   tls.VersionTLS13,
	})
	if err != nil {
		t.Fatalf("tls.Listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	s := &fakeGnatrixServer{
		t:             t,
		listener:      listener,
		cert:          leaf,
		helloReceived: make(chan wire.HelloMsg, 1),
	}
	go s.acceptOne(opts)
	return s
}

func (s *fakeGnatrixServer) acceptOne(opts fakeServerOpts) {
	conn, err := s.listener.Accept()
	if err != nil {
		return
	}
	defer conn.Close()

	tlsConn, ok := conn.(*tls.Conn)
	if !ok {
		s.t.Errorf("fake server: accepted non-TLS connection")
		return
	}
	if err := tlsConn.HandshakeContext(context.Background()); err != nil {
		s.t.Errorf("fake server: tls handshake: %v", err)
		return
	}

	hdr, err := wire.ReadHeader(tlsConn)
	if err != nil {
		s.t.Errorf("fake server: read header: %v", err)
		return
	}
	if hdr.Type != wire.FrameHello {
		s.t.Errorf("fake server: expected HELLO (0x02), got 0x%02x", byte(hdr.Type))
		return
	}

	payload, err := wire.ReadPayload(tlsConn, hdr.PayloadLen)
	if err != nil {
		s.t.Errorf("fake server: read payload: %v", err)
		return
	}
	hello, err := wire.DecodeHello(bytes.NewReader(payload))
	if err != nil {
		s.t.Errorf("fake server: decode hello: %v", err)
		return
	}
	s.helloReceived <- hello

	if hello.AuthMethod != wire.AuthMethodAPIToken {
		s.t.Errorf("fake server: hello.AuthMethod = %d; want %d (api_token)",
			hello.AuthMethod, wire.AuthMethodAPIToken)
		return
	}

	switch {
	case opts.stallAfterHello:
		// Park until the client drops its end of the conn. The Read
		// returns when the client closes (Dial-error path) or when
		// the listener cleanup runs.
		_, _ = tlsConn.Read(make([]byte, 1))
	case opts.forceErrorCode != 0:
		_, _ = tlsConn.Write(wire.EncodeError(wire.ErrorMsg{
			Code:    opts.forceErrorCode,
			Message: opts.forceErrorMessage,
		}))
	case hello.TenantSlug != opts.validTenantSlug:
		_, _ = tlsConn.Write(wire.EncodeError(wire.ErrorMsg{
			Code:    wire.CodeAuthRequired,
			Message: "tenant unknown",
		}))
	case !bytes.Equal(hello.Credential, opts.validToken):
		_, _ = tlsConn.Write(wire.EncodeError(wire.ErrorMsg{
			Code:    wire.CodeInvalidCredentials,
			Message: "invalid credentials",
		}))
	default:
		if _, err := tlsConn.Write(wire.EncodeWelcome(opts.welcome)); err != nil {
			return
		}
		s.servePings(tlsConn, opts.pingResponseDelay)
	}
}

// servePings reads PING frames after a successful WELCOME and replies with
// PONG. Returns when the client closes the connection or sends anything
// other than a well-formed PING.
//
// When delay > 0 each PONG is dispatched in its own goroutine after sleeping
// for delay, so multiple PINGs can be in flight simultaneously. tls.Conn.Write
// is internally synchronized so the per-goroutine writes do not interleave on
// the wire.
func (s *fakeGnatrixServer) servePings(conn *tls.Conn, delay time.Duration) {
	for {
		hdr, err := wire.ReadHeader(conn)
		if err != nil {
			return
		}
		if hdr.Type != wire.FramePing || hdr.PayloadLen != 0 {
			return
		}
		if delay > 0 {
			go func() {
				time.Sleep(delay)
				_, _ = conn.Write(wire.EncodePong())
			}()
			continue
		}
		if _, err := conn.Write(wire.EncodePong()); err != nil {
			return
		}
	}
}

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
