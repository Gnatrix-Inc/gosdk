package wire

import (
	"bytes"
	"testing"
)

// Fixture values for welcome_apitoken.hex. Shared by encode/decode tests so a
// drift in either direction is caught.
var welcomeAPITokenGolden = WelcomeMsg{
	SessionID:          42,
	ServerCapabilities: 0,
	SessionExpiresAtSec: 1_700_000_000,
	UserID:             [16]byte{0x11, 0x11, 0x11, 0x11, 0x22, 0x22, 0x33, 0x33, 0x44, 0x44, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55},
	TenantID:           [16]byte{0xaa, 0xaa, 0xaa, 0xaa, 0xbb, 0xbb, 0xcc, 0xcc, 0xdd, 0xdd, 0xee, 0xee, 0xee, 0xee, 0xee, 0xee},
	Permissions:        []string{"query:read", "query:write"},
	IssuedToken:        "",
}

func TestEncodeWelcome_APIToken_MatchesGolden(t *testing.T) {
	got := EncodeWelcome(welcomeAPITokenGolden)
	want := loadHexFixture(t, "welcome_apitoken.hex")

	if !bytes.Equal(got, want) {
		t.Fatalf("WELCOME frame mismatch\n got  (%d bytes): %x\n want (%d bytes): %x",
			len(got), got, len(want), want)
	}
}

func TestDecodeWelcome_APIToken_RoundTripsGolden(t *testing.T) {
	raw := loadHexFixture(t, "welcome_apitoken.hex")

	hdr, err := ReadHeader(bytes.NewReader(raw[:8]))
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	if hdr.Type != FrameWelcome {
		t.Fatalf("header type = %v; want FrameWelcome", hdr.Type)
	}
	if int(hdr.PayloadLen) != len(raw)-8 {
		t.Fatalf("header PayloadLen = %d; want %d", hdr.PayloadLen, len(raw)-8)
	}

	msg, err := DecodeWelcome(bytes.NewReader(raw[8:]))
	if err != nil {
		t.Fatalf("DecodeWelcome: %v", err)
	}

	want := welcomeAPITokenGolden
	if msg.SessionID != want.SessionID {
		t.Errorf("SessionID = %d; want %d", msg.SessionID, want.SessionID)
	}
	if msg.ServerCapabilities != want.ServerCapabilities {
		t.Errorf("ServerCapabilities = %d; want %d", msg.ServerCapabilities, want.ServerCapabilities)
	}
	if msg.SessionExpiresAtSec != want.SessionExpiresAtSec {
		t.Errorf("SessionExpiresAtSec = %d; want %d", msg.SessionExpiresAtSec, want.SessionExpiresAtSec)
	}

	// UUID byte-preservation: bytes.Equal on the raw 16-byte arrays —
	// catches any future change that introduces a string-roundtrip in the
	// codec (e.g. parsing into uuid.UUID and back), which would normalize
	// or reformat the bytes.
	if !bytes.Equal(msg.UserID[:], want.UserID[:]) {
		t.Errorf("UserID = % x; want % x", msg.UserID, want.UserID)
	}
	if !bytes.Equal(msg.TenantID[:], want.TenantID[:]) {
		t.Errorf("TenantID = % x; want % x", msg.TenantID, want.TenantID)
	}

	if len(msg.Permissions) != len(want.Permissions) {
		t.Fatalf("len(Permissions) = %d; want %d", len(msg.Permissions), len(want.Permissions))
	}
	for i, p := range want.Permissions {
		if msg.Permissions[i] != p {
			t.Errorf("Permissions[%d] = %q; want %q", i, msg.Permissions[i], p)
		}
	}
	if msg.IssuedToken != want.IssuedToken {
		t.Errorf("IssuedToken = %q; want %q", msg.IssuedToken, want.IssuedToken)
	}
}
