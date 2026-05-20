package wire

import (
	"errors"
	"fmt"
	"io"
)

// ErrTruncated is returned by ReadLenStr / ReadLenBytes when the length
// prefix declares more bytes than the underlying reader can supply.
// Match with errors.Is(err, ErrTruncated).
var ErrTruncated = errors.New("wire: declared length exceeds remaining buffer")

// AppendLenStr encodes s as a lenstr (varint(len) + UTF-8 bytes) and
// appends the result to dst.  Matches encode_string() in frame_codec.cpp.
func AppendLenStr(dst []byte, s string) []byte {
	dst = AppendVarint(dst, uint64(len(s)))
	return append(dst, s...)
}

// AppendLenBytes encodes b as a lenbytes (varint(len) + raw bytes) and
// appends the result to dst.
func AppendLenBytes(dst []byte, b []byte) []byte {
	dst = AppendVarint(dst, uint64(len(b)))
	return append(dst, b...)
}

// ReadLenStr reads a lenstr from r: first a varint length, then that many
// UTF-8 bytes.  Matches decode_string() in frame_codec.cpp.
func ReadLenStr(r io.Reader) (string, error) {
	b, err := ReadLenBytes(r)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// ReadLenBytes reads a lenbytes from r: first a varint length, then that
// many raw bytes.
func ReadLenBytes(r io.Reader) ([]byte, error) {
	n, err := ReadVarint(r)
	if err != nil {
		return nil, fmt.Errorf("wire: read lenbytes length: %w", err)
	}
	if n == 0 {
		return []byte{}, nil
	}
	if n > MaxPayload {
		return nil, fmt.Errorf("wire: lenbytes length %d exceeds max %d", n, MaxPayload)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("wire: lenbytes declared %d bytes: %w", n, ErrTruncated)
		}
		return nil, fmt.Errorf("wire: read lenbytes data (%d bytes): %w", n, err)
	}
	return buf, nil
}
