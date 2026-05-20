package wire

import (
	"fmt"
	"io"
)

// WelcomeMsg is the decoded form of a WELCOME (0x03) frame payload
// (wire-protocol.md §WELCOME).
type WelcomeMsg struct {
	// SessionID is an opaque 64-bit session identifier minted by the server.
	SessionID uint64

	// ServerCapabilities is a bitmask; currently reserved (0).
	ServerCapabilities uint64

	// SessionExpiresAtMs is the session expiry as unix epoch milliseconds.
	SessionExpiresAtMs uint64

	// UserID is the 16-byte raw UUID of the authenticated user.
	UserID [16]byte

	// TenantID is the 16-byte raw UUID of the tenant.
	TenantID [16]byte

	// Permissions granted to this session, e.g. "query:read".
	Permissions []string

	// IssuedToken is the freshly issued API token after a password
	// handshake; empty for token-method auth (auth_method=1).
	IssuedToken string
}

// EncodeWelcome encodes msg into a complete on-wire WELCOME frame
// (8-byte header + payload).
func EncodeWelcome(msg WelcomeMsg) []byte {
	var p []byte
	p = AppendVarint(p, msg.SessionID)
	p = AppendVarint(p, msg.ServerCapabilities)
	p = AppendVarint(p, msg.SessionExpiresAtMs)
	p = AppendLenBytes(p, msg.UserID[:])
	p = AppendLenBytes(p, msg.TenantID[:])
	p = AppendVarint(p, uint64(len(msg.Permissions)))
	for _, perm := range msg.Permissions {
		p = AppendLenStr(p, perm)
	}
	p = AppendLenStr(p, msg.IssuedToken)

	frame := AppendHeader(nil, FrameWelcome, uint32(len(p)))
	return append(frame, p...)
}

// DecodeWelcome reads a WELCOME payload from r and returns the decoded
// message. The caller is expected to have already consumed the 8-byte
// header and to pass a reader bounded to the payload length.
func DecodeWelcome(r io.Reader) (WelcomeMsg, error) {
	var m WelcomeMsg
	var err error

	if m.SessionID, err = ReadVarint(r); err != nil {
		return m, fmt.Errorf("wire: welcome session_id: %w", err)
	}
	if m.ServerCapabilities, err = ReadVarint(r); err != nil {
		return m, fmt.Errorf("wire: welcome server_capabilities: %w", err)
	}
	if m.SessionExpiresAtMs, err = ReadVarint(r); err != nil {
		return m, fmt.Errorf("wire: welcome session_expires_at_ms: %w", err)
	}

	uid, err := ReadLenBytes(r)
	if err != nil {
		return m, fmt.Errorf("wire: welcome user_id: %w", err)
	}
	if len(uid) != 16 {
		return m, fmt.Errorf("wire: welcome user_id must be 16 bytes, got %d", len(uid))
	}
	copy(m.UserID[:], uid)

	tid, err := ReadLenBytes(r)
	if err != nil {
		return m, fmt.Errorf("wire: welcome tenant_id: %w", err)
	}
	if len(tid) != 16 {
		return m, fmt.Errorf("wire: welcome tenant_id must be 16 bytes, got %d", len(tid))
	}
	copy(m.TenantID[:], tid)

	count, err := ReadVarint(r)
	if err != nil {
		return m, fmt.Errorf("wire: welcome permissions_count: %w", err)
	}
	if count > MaxPayload {
		return m, fmt.Errorf("wire: welcome permissions_count %d exceeds bound", count)
	}
	m.Permissions = make([]string, 0, count)
	for i := uint64(0); i < count; i++ {
		perm, err := ReadLenStr(r)
		if err != nil {
			return m, fmt.Errorf("wire: welcome permissions[%d]: %w", i, err)
		}
		m.Permissions = append(m.Permissions, perm)
	}

	if m.IssuedToken, err = ReadLenStr(r); err != nil {
		return m, fmt.Errorf("wire: welcome issued_token: %w", err)
	}

	return m, nil
}
