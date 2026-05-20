package wire

import (
	"fmt"
	"io"
)

// AppendVarint encodes v as unsigned LEB128 and appends the result to dst.
// Matches encode_varint() in frame_codec.cpp.
func AppendVarint(dst []byte, v uint64) []byte {
	for {
		b := uint8(v & 0x7F)
		v >>= 7
		if v != 0 {
			b |= 0x80
		}
		dst = append(dst, b)
		if v == 0 {
			break
		}
	}
	return dst
}

// ReadVarint reads one unsigned LEB128 varint from r.
// Matches decode_varint() in frame_codec.cpp.
// Returns an error on overflow (shift >= 64) or EOF mid-varint.
func ReadVarint(r io.Reader) (uint64, error) {
	const maxShift = 63
	var (
		result uint64
		shift  uint
		buf    [1]byte
	)
	for {
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return 0, fmt.Errorf("wire: read varint: %w", err)
		}
		b := buf[0]
		result |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			return result, nil
		}
		shift += 7
		if shift > maxShift {
			return 0, fmt.Errorf("wire: varint overflow")
		}
	}
}
