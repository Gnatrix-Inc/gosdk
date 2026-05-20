// Package wire implements the Gnatrix binary wire protocol v1.
//
// Spec: terminal-client/docs/wire-protocol.md (authoritative).
// This package is internal; only the root gnatrix package may import it.
package wire

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Transport constants (wire-protocol.md §Transport, §Frame header).
const (
	Magic      = 0x47 // 'G'
	Version    = 0x01
	MaxPayload = 65536
)

// FrameType is the single-byte frame-type field at header offset 2.
type FrameType uint8

const (
	FrameHello         FrameType = 0x02
	FrameWelcome       FrameType = 0x03
	FrameAuthChallenge FrameType = 0x04 // reserved
	FrameError         FrameType = 0x05
	FrameAuthResponse  FrameType = 0x06 // reserved
	// 0x07 retired (BOOTSTRAP_HELLO) — do not reuse.
	FrameQueryRequest  FrameType = 0x10
	FrameQueryRow      FrameType = 0x11
	FrameQueryEnd      FrameType = 0x12
	FrameQueryCancel   FrameType = 0x13
	FrameQueryProgress FrameType = 0x14
	FramePing          FrameType = 0x20
	FramePong          FrameType = 0x21
)

func (t FrameType) String() string {
	switch t {
	case FrameHello:
		return "HELLO"
	case FrameWelcome:
		return "WELCOME"
	case FrameError:
		return "ERROR"
	case FrameQueryRequest:
		return "QUERY_REQUEST"
	case FrameQueryRow:
		return "QUERY_ROW"
	case FrameQueryEnd:
		return "QUERY_END"
	case FrameQueryCancel:
		return "QUERY_CANCEL"
	case FrameQueryProgress:
		return "QUERY_PROGRESS"
	case FramePing:
		return "PING"
	case FramePong:
		return "PONG"
	default:
		return fmt.Sprintf("UNKNOWN(0x%02x)", uint8(t))
	}
}

// Header is the decoded 8-byte frame header.
type Header struct {
	Magic      uint8
	Version    uint8
	Type       FrameType
	Flags      uint8
	PayloadLen uint32
}

// ReadHeader reads exactly 8 bytes from r and fills a Header.
// Returns an error if magic or version do not match.
func ReadHeader(r io.Reader) (Header, error) {
	var buf [8]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return Header{}, fmt.Errorf("wire: read header: %w", err)
	}
	if buf[0] != Magic {
		return Header{}, fmt.Errorf("wire: invalid magic 0x%02x (want 0x%02x)", buf[0], Magic)
	}
	if buf[1] != Version {
		return Header{}, fmt.Errorf("wire: unsupported version 0x%02x (want 0x%02x)", buf[1], Version)
	}
	h := Header{
		Magic:      buf[0],
		Version:    buf[1],
		Type:       FrameType(buf[2]),
		Flags:      buf[3],
		PayloadLen: binary.LittleEndian.Uint32(buf[4:8]),
	}
	if h.PayloadLen > MaxPayload {
		return Header{}, fmt.Errorf("wire: payload length %d exceeds max %d", h.PayloadLen, MaxPayload)
	}
	return h, nil
}

// AppendHeader appends an 8-byte frame header to dst.
func AppendHeader(dst []byte, t FrameType, payloadLen uint32) []byte {
	dst = append(dst, Magic, Version, uint8(t), 0x00)
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], payloadLen)
	return append(dst, lenBuf[:]...)
}

// ReadPayload reads exactly n bytes from r. n must be <= MaxPayload.
func ReadPayload(r io.Reader, n uint32) ([]byte, error) {
	if n == 0 {
		return nil, nil
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("wire: read payload (%d bytes): %w", n, err)
	}
	return buf, nil
}
