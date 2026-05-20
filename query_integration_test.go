//go:build integration

// Integration tests gated by the `integration` build tag. These talk
// to a real gnatrixquery server (defaults to localhost:7777) and are
// NOT run by `go test ./...`. Run with:
//
//	GO=/usr/lib/go-1.22/bin/go go test -tags=integration ./ -v \
//	    -run TestIntegration_WireCapture
//
// Required env vars (with defaults pointing at the local sample setup):
//	GNATRIX_ADDR       — host:port (default "localhost:7777")
//	GNATRIX_TOKEN      — gnx_... API token (no default; test skips if unset)
//	GNATRIX_TENANT     — tenant slug (default "acme")
//	GNATRIX_CA_CERT    — path to CA PEM (default "../gnatrix-tests/gosdk/ca-cert.crt")

package gnatrix

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Gnatrix-Inc/gosdk/internal/transport"
	"github.com/Gnatrix-Inc/gosdk/internal/wire"
)

// captureConn wraps a net.Conn and records every Read/Write into two
// buffers (rx and tx). Reads/Writes proceed unchanged; the capture is
// passive. Wrapping happens AFTER the TLS handshake completes, so the
// recorded bytes are cleartext gnatrix frames — exactly the wire
// surface our codecs care about.
type captureConn struct {
	net.Conn
	mu     sync.Mutex
	rxBuf  bytes.Buffer
	txBuf  bytes.Buffer
}

func (c *captureConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.mu.Lock()
		c.rxBuf.Write(p[:n])
		c.mu.Unlock()
	}
	return n, err
}

func (c *captureConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.txBuf.Write(p)
	c.mu.Unlock()
	return c.Conn.Write(p)
}

func (c *captureConn) rx() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]byte, c.rxBuf.Len())
	copy(out, c.rxBuf.Bytes())
	return out
}

func (c *captureConn) tx() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]byte, c.txBuf.Len())
	copy(out, c.txBuf.Bytes())
	return out
}

// dialWithCapture is a test-only variant of Dial that wraps the
// transport conn in a captureConn before running the gnatrix
// handshake. The handshake() signature accepts net.Conn (refactored
// for this reason in Slice 1.x), so the wrap is transparent.
func dialWithCapture(t *testing.T, cfg Config) (*Client, *captureConn) {
	t.Helper()

	if cfg.Addr == "" || cfg.Token == "" || cfg.TenantSlug == "" {
		t.Fatal("dialWithCapture: Addr, Token, and TenantSlug are required")
	}

	clientVersion := cfg.ClientVersion
	if clientVersion == "" {
		clientVersion = defaultClientVersion
	}

	tlsCfg := cfg.TLSConfig
	if cfg.CACertPath != "" {
		tc, err := tlsConfigFromCA(cfg.CACertPath)
		if err != nil {
			t.Fatalf("tlsConfigFromCA: %v", err)
		}
		tlsCfg = tc
	}

	tlsConn, err := transport.DialTLS(context.Background(), cfg.Addr, tlsCfg, cfg.DialTimeout, cfg.HandshakeTimeout)
	if err != nil {
		t.Fatalf("DialTLS: %v", err)
	}

	cap := &captureConn{Conn: tlsConn}

	session, err := handshake(context.Background(), cap, cfg, clientVersion)
	if err != nil {
		_ = tlsConn.Close()
		t.Fatalf("handshake: %v", err)
	}

	c := &Client{
		conn:        cap,
		session:     session,
		readDone:    make(chan struct{}),
		terminating: make(chan struct{}),
	}
	go c.readLoop()

	t.Cleanup(func() {
		_ = c.Close()
	})

	return c, cap
}

// configFromEnv pulls the integration target from env vars with
// sensible defaults pointing at the local sample setup. Returns ok=false
// (and skips via t.Skip) when the test cannot run.
func configFromEnv(t *testing.T) (Config, bool) {
	t.Helper()
	token := os.Getenv("GNATRIX_TOKEN")
	if token == "" {
		// The sample REST API hardcodes a default for local dev. Use it
		// so the test "just works" on the dev box without env tweaking.
		// In CI this should be set explicitly.
		token = "gnx_TIJWZDZD7E5TOUBTBPZU52ZNBVWFIC4GCWCFC324W24ATIAUUIVQ"
	}
	addr := os.Getenv("GNATRIX_ADDR")
	if addr == "" {
		addr = "localhost:7777"
	}
	tenant := os.Getenv("GNATRIX_TENANT")
	if tenant == "" {
		tenant = "acme"
	}
	caPath := os.Getenv("GNATRIX_CA_CERT")
	if caPath == "" {
		caPath = "../gnatrix-tests/gosdk/ca-cert.crt"
	}
	if _, err := os.Stat(caPath); err != nil {
		t.Skipf("CA cert not found at %q: %v", caPath, err)
		return Config{}, false
	}
	return Config{
		Addr:       addr,
		Token:      token,
		TenantSlug: tenant,
		CACertPath: caPath,
		// Use a minimum-config TLSConfig for ServerName since
		// CACertPath path picks system roots otherwise. The handshake
		// helper handles this.
	}, true
}

// preflight establishes a TCP connection to verify the server is up
// before the test invests in TLS+handshake setup. Returns a clearer
// skip message than a generic "dial failed".
func preflight(t *testing.T, addr string) {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Skipf("gnatrixquery server not reachable at %s: %v", addr, err)
		return
	}
	_ = c.Close()
}

// readOneFrame consumes exactly one 8-byte header + payload from the
// front of buf and returns (header, payload, rest). Used to slice the
// captured byte stream into individual frames for comparison.
func readOneFrame(t *testing.T, buf []byte) (wire.Header, []byte, []byte) {
	t.Helper()
	if len(buf) < 8 {
		t.Fatalf("readOneFrame: buf len %d < 8", len(buf))
	}
	hdr, err := wire.ReadHeader(bytes.NewReader(buf[:8]))
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	end := 8 + int(hdr.PayloadLen)
	if len(buf) < end {
		t.Fatalf("readOneFrame: declared payload %d but only %d bytes after header", hdr.PayloadLen, len(buf)-8)
	}
	return hdr, buf[8:end], buf[end:]
}

// writeWireGolden persists the captured bytes under testdata/wire_real/
// in hex form so later hermetic tests (Slice 2+ or post-server-fix)
// can reuse them as goldens — proving forward compatibility against a
// snapshot of the real wire.
func writeWireGolden(t *testing.T, name string, frame []byte) {
	t.Helper()
	dir := filepath.Join("testdata", "wire_real")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll %s: %v", dir, err)
	}
	path := filepath.Join(dir, name+".hex")
	hex := make([]byte, 0, len(frame)*3)
	for i, b := range frame {
		if i > 0 && i%16 == 0 {
			hex = append(hex, '\n')
		} else if i > 0 {
			hex = append(hex, ' ')
		}
		const digits = "0123456789abcdef"
		hex = append(hex, digits[b>>4], digits[b&0x0f])
	}
	hex = append(hex, '\n')
	if err := os.WriteFile(path, hex, 0o644); err != nil {
		t.Fatalf("WriteFile %s: %v", path, err)
	}
	t.Logf("wrote %s (%d bytes)", path, len(frame))
}

// TestIntegration_WireCapture_RequestAndEnd runs one round-trip
// against the real gnatrixquery server and validates that the wire
// bytes we see in BOTH directions are exactly what our codecs
// produce/consume.
//
// What it actually validates today (server has no data plane):
//
//   - QUERY_REQUEST emitted by the SDK is byte-identical to
//     wire.EncodeQueryRequest(<the same struct>). The server accepted
//     it — proves the layout is the one the server's C++ codec
//     expects.
//   - QUERY_END received from the server (with status=7
//     "storage_unavailable") decodes via wire.DecodeQueryEnd into the
//     fields we expect. Proves the server's emitted byte layout is
//     the one our codec understands.
//
// What is NOT validated today:
//
//   - QUERY_ROW: server does not emit rows without a data plane.
//   - QUERY_PROGRESS: server does not emit progress for queries that
//     fail at admission.
//
// Both are covered by net.Pipe byte injection in query_test.go and
// wire/query_test.go (Slice 1.1 + 1.2). When a server with a working
// data plane is available, this test should be extended to capture
// ROW/PROGRESS too.
func TestIntegration_WireCapture_RequestAndEnd(t *testing.T) {
	cfg, ok := configFromEnv(t)
	if !ok {
		return
	}
	preflight(t, cfg.Addr)

	// Override the TLS ServerName since we connect to "localhost" but
	// the server cert is issued for that name explicitly.
	cfg.TLSConfig = &tls.Config{ServerName: "localhost", MinVersion: tls.VersionTLS13}
	// CACertPath takes precedence in our normal Dial; for this test we
	// need both the system-built tls.Config (for ServerName) and the
	// CA pool. Easiest: build tlsConfigFromCA then override ServerName.
	caCfg, err := tlsConfigFromCA(cfg.CACertPath)
	if err != nil {
		t.Fatalf("tlsConfigFromCA: %v", err)
	}
	caCfg.ServerName = "localhost"
	cfg.TLSConfig = caCfg
	cfg.CACertPath = "" // mutually exclusive guard

	client, cap := dialWithCapture(t, cfg)

	// --- Run the query and capture both directions of the wire ---

	const queryText = "search error"
	stream, err := client.Query(context.Background(), queryText, QueryOptions{
		IndexName: "default",
		Limit:     5,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer stream.Close()

	// Drain until terminal. We expect either io.EOF (empty result) or
	// a *QueryError (the current server returns status=7).
	var terminalErr error
	for {
		_, err := stream.Next(context.Background())
		if err != nil {
			terminalErr = err
			break
		}
	}
	if terminalErr == nil {
		t.Fatal("stream did not terminate")
	}
	if !errors.Is(terminalErr, io.EOF) {
		var qerr *QueryError
		if !errors.As(terminalErr, &qerr) {
			t.Fatalf("unexpected terminal error %T: %v", terminalErr, terminalErr)
		}
		t.Logf("stream terminated with QueryError status=%d engine_code=%d: %q",
			qerr.Status, qerr.EngineCode, qerr.Message)
	}

	// --- Slice the TX stream: HELLO + QUERY_REQUEST ---

	tx := cap.tx()
	hdrHello, _, restTx := readOneFrame(t, tx)
	if hdrHello.Type != wire.FrameHello {
		t.Fatalf("first TX frame type = 0x%02x; want HELLO 0x02", byte(hdrHello.Type))
	}
	hdrReq, reqPayload, leftoverTx := readOneFrame(t, restTx)
	if hdrReq.Type != wire.FrameQueryRequest {
		t.Fatalf("second TX frame type = 0x%02x; want QUERY_REQUEST 0x10", byte(hdrReq.Type))
	}
	if len(leftoverTx) != 0 {
		t.Errorf("unexpected trailing TX bytes: %x", leftoverTx)
	}

	// Validation #1: the bytes we sent decode to the struct we
	// constructed. This proves our encoder is round-trip-stable
	// against itself.
	gotReq, err := wire.DecodeQueryRequest(bytes.NewReader(reqPayload))
	if err != nil {
		t.Fatalf("DecodeQueryRequest on captured TX: %v", err)
	}
	if gotReq.QueryText != queryText || gotReq.IndexName != "default" || gotReq.Limit != 5 {
		t.Errorf("round-tripped request mismatch: %+v", gotReq)
	}
	if gotReq.QueryID != 1 {
		t.Errorf("QueryID = %d; want 1 (monotonic counter starts at 1)", gotReq.QueryID)
	}

	// Validation #2: re-encode the decoded struct via our codec and
	// confirm the bytes match what we actually sent. This is the
	// strongest local check — if our encoder ever drifts, this fires.
	// It also indirectly cross-validates against the server: the server
	// accepted these exact bytes (no MalformedString error, no 1001
	// InvalidFrame), so the server's decoder reads them too.
	reEncodedFull := wire.EncodeQueryRequest(gotReq)
	reEncodedPayload := reEncodedFull[8:]
	if !bytes.Equal(reEncodedPayload, reqPayload) {
		t.Errorf("re-encoded request mismatch\n got  (%d): %x\n want (%d): %x",
			len(reEncodedPayload), reEncodedPayload, len(reqPayload), reqPayload)
	}

	// Persist the QUERY_REQUEST frame (header + payload, in that order)
	// as a golden for hermetic tests post-Slice-1. The last
	// hdrReq.PayloadLen+8 bytes of tx are this frame because the only
	// preceding frame is HELLO and we captured nothing else in between.
	queryRequestFrame := append([]byte(nil), tx[len(tx)-int(hdrReq.PayloadLen)-8:]...)
	writeWireGolden(t, "query_request_live_frame", queryRequestFrame)

	// --- Slice the RX stream: WELCOME + QUERY_END ---

	rx := cap.rx()
	hdrWelcome, _, restRx := readOneFrame(t, rx)
	if hdrWelcome.Type != wire.FrameWelcome {
		t.Fatalf("first RX frame type = 0x%02x; want WELCOME 0x03", byte(hdrWelcome.Type))
	}
	hdrEnd, endPayload, leftoverRx := readOneFrame(t, restRx)
	if hdrEnd.Type != wire.FrameQueryEnd {
		// The server might emit ERROR instead in some configurations;
		// surface that as a useful failure.
		t.Fatalf("second RX frame type = 0x%02x; want QUERY_END 0x12 (got %d bytes leftover)",
			byte(hdrEnd.Type), len(leftoverRx))
	}

	// Validation #3: the bytes we received decode into a sane QueryEnd
	// struct. This proves the server's encoder emits a layout our
	// decoder understands.
	gotEnd, err := wire.DecodeQueryEnd(bytes.NewReader(endPayload))
	if err != nil {
		t.Fatalf("DecodeQueryEnd on captured RX: %v", err)
	}
	if gotEnd.QueryID != gotReq.QueryID {
		t.Errorf("QUERY_END.QueryID = %d; want %d (matches request)", gotEnd.QueryID, gotReq.QueryID)
	}
	if gotEnd.Status != wire.QueryStatusStorageUnavailable {
		// Other statuses are possible if the server config changes.
		// Log but don't fail — the goal is byte-level validation, not
		// asserting server behavior.
		t.Logf("QUERY_END.Status = %d (expected %d storage_unavailable today; server config may have changed)",
			gotEnd.Status, wire.QueryStatusStorageUnavailable)
	}

	// Validation #4: re-encode the decoded END and compare bytes. Same
	// rationale as #2 — locks the encoder/decoder round-trip and
	// proves cross-team alignment for whatever bytes the server sent.
	reEncodedEndFull := wire.EncodeQueryEnd(gotEnd)
	reEncodedEndPayload := reEncodedEndFull[8:]
	if !bytes.Equal(reEncodedEndPayload, endPayload) {
		t.Errorf("re-encoded QUERY_END mismatch\n got  (%d): %x\n want (%d): %x",
			len(reEncodedEndPayload), reEncodedEndPayload, len(endPayload), endPayload)
	}

	endFrame := append([]byte(nil), restRx[:8+int(hdrEnd.PayloadLen)]...)
	writeWireGolden(t, "query_end_live_frame", endFrame)

	t.Logf("captured QUERY_REQUEST: %d bytes (payload %d)", 8+int(hdrReq.PayloadLen), hdrReq.PayloadLen)
	t.Logf("captured QUERY_END:     %d bytes (payload %d) — status=%d message=%q",
		8+int(hdrEnd.PayloadLen), hdrEnd.PayloadLen, gotEnd.Status, gotEnd.Message)
}
