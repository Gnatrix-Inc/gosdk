package wire

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestLenStr_RoundTrip_VariousLengths(t *testing.T) {
	for _, length := range []int{0, 1, 127, 128, 65000} {
		t.Run(fmt.Sprintf("len_%d", length), func(t *testing.T) {
			s := strings.Repeat("a", length)

			encoded := AppendLenStr(nil, s)

			r := bytes.NewReader(encoded)
			decoded, err := ReadLenStr(r)
			if err != nil {
				t.Fatalf("ReadLenStr: %v", err)
			}
			if decoded != s {
				t.Errorf("round-trip mismatch: encoded %d bytes, decoded len %d (want %d)",
					len(encoded), len(decoded), length)
			}
			if r.Len() != 0 {
				t.Errorf("ReadLenStr left %d bytes unconsumed; want 0", r.Len())
			}
		})
	}
}

func TestLenStr_TruncatedBuffer_ReturnsErrTruncated(t *testing.T) {
	// Build a valid 100-byte lenstr, then drop bytes from the tail so the
	// length prefix still claims 100 but only 90 content bytes are available.
	s := strings.Repeat("a", 100)
	encoded := AppendLenStr(nil, s)
	truncated := encoded[:len(encoded)-10]

	_, err := ReadLenStr(bytes.NewReader(truncated))
	if err == nil {
		t.Fatal("ReadLenStr on truncated buffer returned nil; want ErrTruncated")
	}
	if !errors.Is(err, ErrTruncated) {
		t.Errorf("errors.Is(err, ErrTruncated) = false; err = %v", err)
	}
}

func TestLenStr_EmptyContentDeclaredNonZero_ReturnsErrTruncated(t *testing.T) {
	// Length prefix says 5 bytes follow, but the buffer ends right there.
	// io.ReadFull returns io.EOF (no bytes read at all) — must also surface
	// as ErrTruncated, not bare io.EOF.
	prefixOnly := AppendVarint(nil, 5)

	_, err := ReadLenStr(bytes.NewReader(prefixOnly))
	if err == nil {
		t.Fatal("ReadLenStr on prefix-only buffer returned nil; want ErrTruncated")
	}
	if !errors.Is(err, ErrTruncated) {
		t.Errorf("errors.Is(err, ErrTruncated) = false; err = %v", err)
	}
}
