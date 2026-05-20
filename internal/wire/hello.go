package wire

import (
	"fmt"
	"io"
)

// AuthMethod values for the HELLO frame (wire-protocol.md §HELLO).
const (
	AuthMethodAPIToken     uint8 = 1 // raw "gnx_..." bytes
	AuthMethodMTLS         uint8 = 2 // reserved; credential empty
	AuthMethodPassword     uint8 = 3 // raw password bytes + email
	AuthMethodSuperuserPeer uint8 = 4 // Unix socket only
)

// HelloMsg is the decoded form of a HELLO (0x02) frame payload.
// Call EncodeHello to turn it into a complete on-wire frame.
type HelloMsg struct {
	// AuthMethod is one of the AuthMethod* constants above.
	AuthMethod uint8

	// TenantSlug identifies the tenant; ignored when AuthMethod=4.
	TenantSlug string

	// Credential is the raw credential bytes.
	//   method=1: raw "gnx_..." token bytes
	//   method=3: raw UTF-8 password bytes
	//   method=4: empty (peer creds are the credential)
	Credential []byte

	// Email is required for method=3 (password). Empty otherwise.
	Email string

	// ClientCapabilities is a bitmask; send 0 (reserved).
	ClientCapabilities uint64

	// ClientVersion is a human-readable version tag, e.g. "gnatrix-go/0.0.1".
	ClientVersion string
}

// EncodeHello encodes msg into a complete on-wire HELLO frame
// (8-byte header + payload).  Matches FrameCodec::encode(HelloFrame&)
// in frame_codec.cpp.
func EncodeHello(msg HelloMsg) []byte {
	// Build payload first to know its length.
	var p []byte
	p = AppendVarint(p, uint64(msg.AuthMethod))
	p = AppendLenStr(p, msg.TenantSlug)
	p = AppendLenBytes(p, msg.Credential)

	if msg.Email != "" {
		p = append(p, 0x01) // has_email = true
		p = AppendLenStr(p, msg.Email)
	} else {
		p = append(p, 0x00) // has_email = false
	}

	p = AppendVarint(p, msg.ClientCapabilities)
	p = AppendLenStr(p, msg.ClientVersion)

	// Prepend 8-byte header.
	frame := AppendHeader(nil, FrameHello, uint32(len(p)))
	return append(frame, p...)
}

// DecodeHello reads a HELLO payload from r and returns the decoded message.
// The caller is expected to have already consumed the 8-byte header and to
// pass a reader bounded to the payload length.
func DecodeHello(r io.Reader) (HelloMsg, error) {
	var m HelloMsg

	authMethod, err := ReadVarint(r)
	if err != nil {
		return m, fmt.Errorf("wire: hello auth_method: %w", err)
	}
	if authMethod > 255 {
		return m, fmt.Errorf("wire: hello auth_method %d exceeds uint8", authMethod)
	}
	m.AuthMethod = uint8(authMethod)

	if m.TenantSlug, err = ReadLenStr(r); err != nil {
		return m, fmt.Errorf("wire: hello tenant_slug: %w", err)
	}
	if m.Credential, err = ReadLenBytes(r); err != nil {
		return m, fmt.Errorf("wire: hello credential: %w", err)
	}

	// has_email is a raw u8, not a varint (wire-protocol.md §HELLO).
	var hasEmail [1]byte
	if _, err := io.ReadFull(r, hasEmail[:]); err != nil {
		return m, fmt.Errorf("wire: hello has_email: %w", err)
	}
	switch hasEmail[0] {
	case 0:
		// no email
	case 1:
		if m.Email, err = ReadLenStr(r); err != nil {
			return m, fmt.Errorf("wire: hello email: %w", err)
		}
	default:
		return m, fmt.Errorf("wire: hello has_email invalid value %d", hasEmail[0])
	}

	if m.ClientCapabilities, err = ReadVarint(r); err != nil {
		return m, fmt.Errorf("wire: hello client_capabilities: %w", err)
	}
	if m.ClientVersion, err = ReadLenStr(r); err != nil {
		return m, fmt.Errorf("wire: hello client_version: %w", err)
	}
	return m, nil
}
