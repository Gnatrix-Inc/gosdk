package wire

import (
	"bytes"
	"math/rand/v2"
	"testing"
)

func TestVarint_BoundaryValues(t *testing.T) {
	cases := []struct {
		name    string
		value   uint64
		encoded []byte
	}{
		{"zero", 0, []byte{0x00}},
		{"one", 1, []byte{0x01}},
		{"max_1byte", 127, []byte{0x7F}},
		{"min_2byte", 128, []byte{0x80, 0x01}},
		{"max_2byte", 16383, []byte{0xFF, 0x7F}},
		{"min_3byte", 16384, []byte{0x80, 0x80, 0x01}},
		{"max_uint32", (1 << 32) - 1, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x0F}},
		{"max_int64", (1 << 63) - 1, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Encode: byte sequence must match the canonical LEB128 form.
			got := AppendVarint(nil, tc.value)
			if !bytes.Equal(got, tc.encoded) {
				t.Errorf("AppendVarint(%d) = %#v; want %#v", tc.value, got, tc.encoded)
			}

			// Decode: round-trip must recover the original value...
			r := bytes.NewReader(tc.encoded)
			v, err := ReadVarint(r)
			if err != nil {
				t.Fatalf("ReadVarint: %v", err)
			}
			if v != tc.value {
				t.Errorf("ReadVarint = %d; want %d", v, tc.value)
			}

			// ...and must consume exactly the encoded bytes, no more, no fewer.
			if r.Len() != 0 {
				t.Errorf("ReadVarint left %d bytes unconsumed; want 0", r.Len())
			}
		})
	}
}

func TestVarint_RandomRoundTrip(t *testing.T) {
	const N = 10_000

	// Fixed PCG seed → reproducible coverage. Bump the constants if the
	// distribution ever needs to vary.
	rng := rand.New(rand.NewPCG(0xC0FFEE, 0xDECAF))

	for i := 0; i < N; i++ {
		v := rng.Uint64()

		encoded := AppendVarint(nil, v)
		if len(encoded) < 1 || len(encoded) > 10 {
			t.Fatalf("iter %d: v=%#x produced %d-byte varint; want 1..10", i, v, len(encoded))
		}

		r := bytes.NewReader(encoded)
		decoded, err := ReadVarint(r)
		if err != nil {
			t.Fatalf("iter %d: v=%#x encoded=%x: ReadVarint failed: %v", i, v, encoded, err)
		}
		if decoded != v {
			t.Fatalf("iter %d: round-trip mismatch\n  input   = %#x\n  encoded = %x\n  decoded = %#x",
				i, v, encoded, decoded)
		}
		if r.Len() != 0 {
			t.Fatalf("iter %d: v=%#x: ReadVarint left %d bytes; want 0", i, v, r.Len())
		}
	}
}
