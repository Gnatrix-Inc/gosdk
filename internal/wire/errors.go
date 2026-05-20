package wire

import (
	"errors"
	"fmt"
)

// ErrMalformed is the sentinel for any decoder rejection. A payload is
// malformed when the byte stream cannot be parsed back into the
// expected struct — examples include truncation mid-field (varint
// overflow, lenstr declared length exceeding buffer, u8 missing) and
// semantic errors (u8 boolean flag with value other than 0 or 1, frame
// type byte out of range).
//
// Callers can recover the broader category with errors.Is(err,
// ErrMalformed). For the underlying cause (io.ErrUnexpectedEOF,
// ErrTruncated, or a custom "invalid value N" formatter), the error
// chain is preserved via fmt.Errorf %w wrapping.
var ErrMalformed = errors.New("wire: malformed payload")

// wrapMalformed is the one-line defer hook each Decode* function uses
// to surface its terminal error under ErrMalformed without rewriting
// every return site:
//
//	func DecodeQueryEnd(r io.Reader) (m QueryEndMsg, err error) {
//	    defer wrapMalformed(&err)
//	    // ... unchanged body ...
//	}
//
// The wrap uses fmt.Errorf with two %w verbs (Go 1.20+), so
// errors.Is(err, ErrMalformed) and errors.Is(err, <underlying>) both
// succeed on the returned error.
func wrapMalformed(perr *error) {
	if *perr != nil {
		*perr = fmt.Errorf("%w: %w", ErrMalformed, *perr)
	}
}
