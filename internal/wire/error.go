package wire

import (
	"fmt"
	"io"
)

// Wire-level error codes (wire-protocol.md §ERROR).
//
// Ranges:
//   1xxx — framing-level
//   2001–2010 — auth / session-level (mapped to *gnatrix.AuthError)
//   2011–2019 — query slice-1 session-level
//   3xxx — rate-limit family
//   5xxx — server-side internal / unexpected
const (
	CodeInvalidFrame       uint32 = 1001
	CodeUnsupportedVersion uint32 = 1002

	CodeAuthRequired       uint32 = 2001
	CodeInvalidCredentials uint32 = 2002
	CodeAccountLocked      uint32 = 2003
	CodeTokenRevoked       uint32 = 2004
	CodeTokenExpired       uint32 = 2005
	CodeTenantNotFound     uint32 = 2006
	CodeMfaRequired        uint32 = 2007

	CodeRateLimited   uint32 = 3001
	CodeInternalError uint32 = 5001
)

// ErrorMsg is the decoded form of an ERROR (0x05) frame payload.
type ErrorMsg struct {
	// Code is one of the Code* constants above.
	Code uint32

	// Message is a human-readable diagnostic, safe to surface to the user.
	Message string
}

// EncodeError encodes msg into a complete on-wire ERROR frame.
func EncodeError(msg ErrorMsg) []byte {
	var p []byte
	p = AppendVarint(p, uint64(msg.Code))
	p = AppendLenStr(p, msg.Message)

	frame := AppendHeader(nil, FrameError, uint32(len(p)))
	return append(frame, p...)
}

// DecodeError reads an ERROR payload from r and returns the decoded
// message. The caller is expected to have already consumed the 8-byte
// header.
func DecodeError(r io.Reader) (ErrorMsg, error) {
	var m ErrorMsg
	code, err := ReadVarint(r)
	if err != nil {
		return m, fmt.Errorf("wire: error code: %w", err)
	}
	m.Code = uint32(code)
	if m.Message, err = ReadLenStr(r); err != nil {
		return m, fmt.Errorf("wire: error message: %w", err)
	}
	return m, nil
}
