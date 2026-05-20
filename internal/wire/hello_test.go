package wire

import (
	"bytes"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEncodeHello_APIToken_MatchesGolden(t *testing.T) {
	msg := HelloMsg{
		AuthMethod:         AuthMethodAPIToken,
		TenantSlug:         "acme",
		Credential:         []byte("gnx_test123"),
		ClientCapabilities: 0,
		ClientVersion:      "gnatrix-go/0.0.1",
	}

	got := EncodeHello(msg)
	want := loadHexFixture(t, "hello_apitoken.hex")

	if !bytes.Equal(got, want) {
		t.Fatalf("HELLO frame mismatch\n got  (%d bytes): %x\n want (%d bytes): %x",
			len(got), got, len(want), want)
	}
}

func TestDecodeHello_APIToken_RoundTripsGolden(t *testing.T) {
	raw := loadHexFixture(t, "hello_apitoken.hex")

	hdr, err := ReadHeader(bytes.NewReader(raw[:8]))
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	if hdr.Type != FrameHello {
		t.Fatalf("header type = %v; want FrameHello", hdr.Type)
	}
	if int(hdr.PayloadLen) != len(raw)-8 {
		t.Fatalf("header PayloadLen = %d; want %d", hdr.PayloadLen, len(raw)-8)
	}

	msg, err := DecodeHello(bytes.NewReader(raw[8:]))
	if err != nil {
		t.Fatalf("DecodeHello: %v", err)
	}
	if msg.AuthMethod != AuthMethodAPIToken {
		t.Errorf("AuthMethod = %d; want %d", msg.AuthMethod, AuthMethodAPIToken)
	}
	if msg.TenantSlug != "acme" {
		t.Errorf("TenantSlug = %q; want %q", msg.TenantSlug, "acme")
	}
	if string(msg.Credential) != "gnx_test123" {
		t.Errorf("Credential = %q; want %q", msg.Credential, "gnx_test123")
	}
	if msg.Email != "" {
		t.Errorf("Email = %q; want empty", msg.Email)
	}
	if msg.ClientCapabilities != 0 {
		t.Errorf("ClientCapabilities = %d; want 0", msg.ClientCapabilities)
	}
	if msg.ClientVersion != "gnatrix-go/0.0.1" {
		t.Errorf("ClientVersion = %q; want %q", msg.ClientVersion, "gnatrix-go/0.0.1")
	}
}

func loadHexFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	stripped := strings.Map(func(r rune) rune {
		switch r {
		case ' ', '\t', '\n', '\r':
			return -1
		}
		return r
	}, string(raw))
	b, err := hex.DecodeString(stripped)
	if err != nil {
		t.Fatalf("decode hex fixture %s: %v", name, err)
	}
	return b
}
