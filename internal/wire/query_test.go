package wire

import (
	"bytes"
	"math"
	"math/rand/v2"
	"strings"
	"testing"
)

// ---- QUERY_REQUEST -----------------------------------------------------

func TestEncodeQueryRequest_NoTimeRange_MatchesGolden(t *testing.T) {
	msg := QueryRequestMsg{
		QueryID:            7,
		IndexName:          "logs-2026",
		QueryText:          "search error",
		Limit:              100,
		Cursor:             "",
		HasTimeRange:       false,
		ProgressIntervalMs: 0,
	}

	got := EncodeQueryRequest(msg)
	want := loadHexFixture(t, "query_request_no_timerange.hex")

	if !bytes.Equal(got, want) {
		t.Fatalf("QUERY_REQUEST frame mismatch\n got  (%d bytes): %x\n want (%d bytes): %x",
			len(got), got, len(want), want)
	}
}

func TestDecodeQueryRequest_NoTimeRange_RoundTripsGolden(t *testing.T) {
	raw := loadHexFixture(t, "query_request_no_timerange.hex")

	hdr, err := ReadHeader(bytes.NewReader(raw[:8]))
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	if hdr.Type != FrameQueryRequest {
		t.Fatalf("header type = %v; want FrameQueryRequest", hdr.Type)
	}
	if int(hdr.PayloadLen) != len(raw)-8 {
		t.Fatalf("header PayloadLen = %d; want %d", hdr.PayloadLen, len(raw)-8)
	}

	msg, err := DecodeQueryRequest(bytes.NewReader(raw[8:]))
	if err != nil {
		t.Fatalf("DecodeQueryRequest: %v", err)
	}
	if msg.QueryID != 7 {
		t.Errorf("QueryID = %d; want 7", msg.QueryID)
	}
	if msg.IndexName != "logs-2026" {
		t.Errorf("IndexName = %q; want %q", msg.IndexName, "logs-2026")
	}
	if msg.QueryText != "search error" {
		t.Errorf("QueryText = %q; want %q", msg.QueryText, "search error")
	}
	if msg.Limit != 100 {
		t.Errorf("Limit = %d; want 100", msg.Limit)
	}
	if msg.Cursor != "" {
		t.Errorf("Cursor = %q; want empty", msg.Cursor)
	}
	if msg.HasTimeRange {
		t.Errorf("HasTimeRange = true; want false")
	}
	if msg.EarliestNs != 0 {
		t.Errorf("EarliestNs = %d; want 0", msg.EarliestNs)
	}
	if msg.LatestNs != 0 {
		t.Errorf("LatestNs = %d; want 0", msg.LatestNs)
	}
	if msg.ProgressIntervalMs != 0 {
		t.Errorf("ProgressIntervalMs = %d; want 0", msg.ProgressIntervalMs)
	}
}

func TestEncodeQueryRequest_WithTimeRange_MatchesGolden(t *testing.T) {
	msg := QueryRequestMsg{
		QueryID:            42,
		IndexName:          "logs-2026",
		QueryText:          "search error",
		Limit:              1000,
		Cursor:             "abc",
		HasTimeRange:       true,
		EarliestNs:         1000,
		LatestNs:           2000,
		ProgressIntervalMs: 500,
	}

	got := EncodeQueryRequest(msg)
	want := loadHexFixture(t, "query_request_with_timerange.hex")

	if !bytes.Equal(got, want) {
		t.Fatalf("QUERY_REQUEST frame mismatch\n got  (%d bytes): %x\n want (%d bytes): %x",
			len(got), got, len(want), want)
	}
}

func TestDecodeQueryRequest_WithTimeRange_RoundTripsGolden(t *testing.T) {
	raw := loadHexFixture(t, "query_request_with_timerange.hex")

	hdr, err := ReadHeader(bytes.NewReader(raw[:8]))
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	if hdr.Type != FrameQueryRequest {
		t.Fatalf("header type = %v; want FrameQueryRequest", hdr.Type)
	}

	msg, err := DecodeQueryRequest(bytes.NewReader(raw[8:]))
	if err != nil {
		t.Fatalf("DecodeQueryRequest: %v", err)
	}
	if msg.QueryID != 42 {
		t.Errorf("QueryID = %d; want 42", msg.QueryID)
	}
	if msg.IndexName != "logs-2026" {
		t.Errorf("IndexName = %q; want %q", msg.IndexName, "logs-2026")
	}
	if msg.QueryText != "search error" {
		t.Errorf("QueryText = %q; want %q", msg.QueryText, "search error")
	}
	if msg.Limit != 1000 {
		t.Errorf("Limit = %d; want 1000", msg.Limit)
	}
	if msg.Cursor != "abc" {
		t.Errorf("Cursor = %q; want %q", msg.Cursor, "abc")
	}
	if !msg.HasTimeRange {
		t.Errorf("HasTimeRange = false; want true")
	}
	if msg.EarliestNs != 1000 {
		t.Errorf("EarliestNs = %d; want 1000", msg.EarliestNs)
	}
	if msg.LatestNs != 2000 {
		t.Errorf("LatestNs = %d; want 2000", msg.LatestNs)
	}
	if msg.ProgressIntervalMs != 500 {
		t.Errorf("ProgressIntervalMs = %d; want 500", msg.ProgressIntervalMs)
	}
}

// TestQueryRequest_NegativeTimestamps_RoundTrip locks the int64↔uint64
// reinterpretation. Negative ns timestamps must round-trip exactly,
// since the wire encodes the two's-complement bit pattern via varint.
func TestQueryRequest_NegativeTimestamps_RoundTrip(t *testing.T) {
	original := QueryRequestMsg{
		QueryID:      1,
		IndexName:    "x",
		QueryText:    "y",
		HasTimeRange: true,
		EarliestNs:   -1,
		LatestNs:     -1_000_000_000,
	}

	raw := EncodeQueryRequest(original)

	// Skip the 8-byte header.
	decoded, err := DecodeQueryRequest(bytes.NewReader(raw[8:]))
	if err != nil {
		t.Fatalf("DecodeQueryRequest: %v", err)
	}
	if decoded.EarliestNs != -1 {
		t.Errorf("EarliestNs = %d; want -1", decoded.EarliestNs)
	}
	if decoded.LatestNs != -1_000_000_000 {
		t.Errorf("LatestNs = %d; want -1_000_000_000", decoded.LatestNs)
	}
}

// TestQueryRequest_PropertyRoundTrip exercises 1000 PCG-seeded random
// QueryRequestMsg values plus a fixed set of boundary cases. For each
// input it verifies three properties simultaneously:
//
//  1. Encode → Decode produces a struct identical to the input (modulo
//     the documented "HasTimeRange=false zeroes EarliestNs/LatestNs"
//     normalization, since those fields are not on the wire when the
//     flag is unset).
//  2. Re-encoding the decoded struct produces a byte sequence identical
//     to the original — the encoder is idempotent over its own output.
//  3. Negative int64 timestamps survive the two's-complement reinterpret
//     (uint64 varint) round-trip. This is the bit_cast invariant; a
//     regression that uses signed varint (zigzag) or truncates to
//     uint32 would surface as elapsed bytes differing or sign loss.
//
// The PCG seed is fixed for reproducibility: a flake at iteration N
// can be re-debugged by stopping at that index.
func TestQueryRequest_PropertyRoundTrip(t *testing.T) {
	const iterations = 1000
	rng := rand.New(rand.NewPCG(0xCAFEBABE, 0xDEADBEEF))

	// Boundary cases that random sampling almost never hits. Each
	// must round-trip just like the random samples.
	edgeCases := []QueryRequestMsg{
		// int64 ns boundaries — the two's-complement extremes.
		{QueryID: 1, HasTimeRange: true, EarliestNs: math.MinInt64, LatestNs: math.MaxInt64},
		{QueryID: 2, HasTimeRange: true, EarliestNs: -1, LatestNs: 1},
		{QueryID: 3, HasTimeRange: true, EarliestNs: 0, LatestNs: 0},
		// uint boundaries.
		{QueryID: math.MaxUint64, Limit: math.MaxUint64, ProgressIntervalMs: math.MaxUint32},
		// Empty / minimal payload.
		{},
		// Long strings to exercise multi-byte varint length prefixes.
		{QueryID: 7, QueryText: strings.Repeat("a", 10_000)},
		{QueryID: 8, Cursor: strings.Repeat("c", 5_000), HasTimeRange: true},
		// HasTimeRange with both timestamps at zero (still emitted on wire).
		{QueryID: 9, HasTimeRange: true},
	}

	for i, msg := range edgeCases {
		if err := assertQueryRequestRoundTrip(msg); err != nil {
			t.Errorf("edge case %d (%+v): %v", i, msg, err)
		}
	}

	negativeSeen := 0
	for i := 0; i < iterations; i++ {
		msg := randomQueryRequest(rng)
		if msg.HasTimeRange && (msg.EarliestNs < 0 || msg.LatestNs < 0) {
			negativeSeen++
		}
		if err := assertQueryRequestRoundTrip(msg); err != nil {
			t.Fatalf("iter %d failed: %v\nmsg: %+v", i, err, msg)
		}
	}

	// Sanity: with full int64 range sampling (~50% negative per field)
	// and HasTimeRange ~50%, we expect a healthy fraction of iterations
	// to actually hit the negative-timestamp branch. A regression that
	// dropped the negative path entirely would make this assertion
	// flag the missing coverage.
	if negativeSeen < iterations/8 {
		t.Errorf("only %d/%d iterations exercised negative timestamps; expected >= %d",
			negativeSeen, iterations, iterations/8)
	}
}

// assertQueryRequestRoundTrip is the property predicate shared by the
// edge-case loop and the random loop. Returns nil on success.
func assertQueryRequestRoundTrip(msg QueryRequestMsg) error {
	encoded := EncodeQueryRequest(msg)
	decoded, err := DecodeQueryRequest(bytes.NewReader(encoded[8:]))
	if err != nil {
		return decodeErr(err)
	}

	// HasTimeRange=false means EarliestNs/LatestNs are not on the wire
	// and decode back as zero regardless of what was passed in. The
	// encoder ignores them too. Normalize before comparing.
	expected := msg
	if !expected.HasTimeRange {
		expected.EarliestNs = 0
		expected.LatestNs = 0
	}
	if decoded != expected {
		return structMismatch(decoded, expected)
	}

	reEncoded := EncodeQueryRequest(decoded)
	if !bytes.Equal(encoded, reEncoded) {
		return reEncodeMismatch(encoded, reEncoded)
	}
	return nil
}

func randomQueryRequest(rng *rand.Rand) QueryRequestMsg {
	return QueryRequestMsg{
		QueryID: rng.Uint64(),
		// Strings capped short enough that 1000 iterations stay fast,
		// but long enough to exercise multi-byte length prefixes.
		IndexName:    randomBytes(rng, rng.IntN(64)),
		QueryText:    randomBytes(rng, rng.IntN(512)),
		Limit:        rng.Uint64(),
		Cursor:       randomBytes(rng, rng.IntN(256)),
		HasTimeRange: rng.IntN(2) == 1,
		// Full int64 range via uint64 bit reinterpretation — this is
		// exactly what the wire does. ~50% of values land negative,
		// proving the two's-complement varint path.
		EarliestNs:         int64(rng.Uint64()),
		LatestNs:           int64(rng.Uint64()),
		ProgressIntervalMs: uint32(rng.Uint32()),
	}
}

// randomBytes returns a string of n bytes drawn from a printable
// subset. Avoiding random bytes 0..31 keeps the test failure messages
// legible when something goes wrong; the codec treats the bytes
// opaquely (lenstr is length-prefixed) so the byte distribution does
// not affect what is being tested.
func randomBytes(rng *rand.Rand, n int) string {
	if n == 0 {
		return ""
	}
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(32 + rng.IntN(95)) // printable ASCII 0x20..0x7e
	}
	return string(b)
}

// Small helpers so the predicate's return path is one line each.
func decodeErr(err error) error               { return err }
func structMismatch(got, want QueryRequestMsg) error {
	return roundTripErr{Reason: "struct mismatch", Got: got, Want: want}
}
func reEncodeMismatch(orig, re []byte) error {
	return roundTripErr{Reason: "re-encode bytes differ", OrigBytes: orig, ReBytes: re}
}

type roundTripErr struct {
	Reason            string
	Got, Want         QueryRequestMsg
	OrigBytes, ReBytes []byte
}

func (e roundTripErr) Error() string {
	if e.OrigBytes != nil {
		return e.Reason + ": orig=" + hexCompact(e.OrigBytes) + " re=" + hexCompact(e.ReBytes)
	}
	return e.Reason
}

func hexCompact(b []byte) string {
	const max = 64
	if len(b) > max {
		return hexEncode(b[:max]) + "..."
	}
	return hexEncode(b)
}

func hexEncode(b []byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, 0, len(b)*2)
	for _, x := range b {
		out = append(out, digits[x>>4], digits[x&0x0f])
	}
	return string(out)
}

// TestSignedInt64NsTimestamp_MatchesServerEncoding pins the exact bytes
// produced for signed-int64 ns timestamps (earliest_ns / latest_ns in
// QUERY_REQUEST). The wire contract is:
//
//	server (server_client/src/query/frames.cpp:14):
//	    void encode_i64_varint(std::int64_t v, std::vector<std::byte>& out) {
//	        net::encode_varint(std::bit_cast<std::uint64_t>(v), out);
//	    }
//
//	SDK (this file's sibling internal/wire/query.go, EncodeQueryRequest):
//	    AppendVarint(p, uint64(msg.EarliestNs))
//	    AppendVarint(p, uint64(msg.LatestNs))
//
// Go's int64→uint64 conversion is a bit-pattern reinterpret per the
// language spec; C++ std::bit_cast<uint64_t> is the same operation.
// LEB128 over the reinterpreted uint64 is then a deterministic byte
// sequence. The expected bytes below are computed by hand from those
// two rules and any divergence — sign-extension bug, zigzag varint
// regression, uint32 truncation — surfaces as a byte mismatch.
//
// Decode side runs symmetrically: ReadVarint produces uint64, then
// int64(uint64) reinterprets back. Round-trip equality is part of the
// contract.
func TestSignedInt64NsTimestamp_MatchesServerEncoding(t *testing.T) {
	tests := []struct {
		name string
		in   int64
		want []byte
	}{
		{
			name: "zero",
			in:   0,
			want: []byte{0x00},
		},
		{
			name: "one",
			in:   1,
			want: []byte{0x01},
		},
		{
			// int64(-1) → uint64(0xFFFFFFFFFFFFFFFF) → 10-byte LEB128
			// of 64-bit all-ones: nine 0xff continuation bytes + 0x01
			// for the top bit.
			name: "minus_one",
			in:   -1,
			want: []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01},
		},
		{
			// math.MinInt64 = -2^63 → uint64(0x8000000000000000) → bit
			// 63 set only. LEB128: nine 0x80 bytes (each carrying a
			// continuation flag with zero payload) + 0x01 for bit 63.
			// Distinct from -1 in byte-exact form, even though both
			// have a high bit set.
			name: "min_int64",
			in:   math.MinInt64,
			want: []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01},
		},
		{
			// math.MaxInt64 = 2^63 - 1 → uint64(0x7FFFFFFFFFFFFFFF) →
			// 63 ones. LEB128: eight 0xff + 0x7f (last byte has no
			// continuation because bit 63 is 0). 9 bytes — one byte
			// shorter than -1 because the high bit terminates the
			// stream.
			name: "max_int64",
			in:   math.MaxInt64,
			want: []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Mirror what EncodeQueryRequest does for the ns timestamp
			// fields at the source level.
			got := AppendVarint(nil, uint64(tt.in))
			if !bytes.Equal(got, tt.want) {
				t.Errorf("encode int64(%d):\n got  %x\n want %x", tt.in, got, tt.want)
			}

			// Decode side: ReadVarint yields uint64; int64(u) reinterprets
			// per spec. Verify the round-trip recovers the input bit-
			// exact (no sign loss, no truncation).
			back, err := ReadVarint(bytes.NewReader(got))
			if err != nil {
				t.Fatalf("ReadVarint: %v", err)
			}
			if int64(back) != tt.in {
				t.Errorf("round-trip int64(%d): got %d", tt.in, int64(back))
			}
		})
	}
}

// TestQueryRequest_SignedTimestamps_InFullFrame verifies that the
// ns-timestamp encoding pinned above is the encoding the QUERY_REQUEST
// encoder actually emits — i.e. that EncodeQueryRequest routes
// earliest_ns and latest_ns through `AppendVarint(p, uint64(v))` and
// not some other path. The test encodes a request with the boundary
// timestamps and asserts that the expected byte sequences appear
// verbatim in the payload at positions adjacent to the has_time_range
// flag.
func TestQueryRequest_SignedTimestamps_InFullFrame(t *testing.T) {
	msg := QueryRequestMsg{
		QueryID:      1,
		IndexName:    "i",
		QueryText:    "q",
		HasTimeRange: true,
		EarliestNs:   math.MinInt64,
		LatestNs:     math.MaxInt64,
	}

	frame := EncodeQueryRequest(msg)
	payload := frame[8:] // skip 8-byte header

	// Locate the has_time_range byte. Preceding fields are:
	//   varint(query_id=1)        = 0x01            (1 byte)
	//   lenstr("i")               = 0x01 0x69       (2 bytes)
	//   lenstr("q")               = 0x01 0x71       (2 bytes)
	//   varint(limit=0)           = 0x00            (1 byte)
	//   lenstr(cursor="")         = 0x00            (1 byte)
	//                             total preamble    = 7 bytes
	// Then: u8 has_time_range, varint earliest, varint latest, varint progress.
	const preamble = 7
	if got := payload[preamble]; got != 0x01 {
		t.Fatalf("has_time_range = 0x%02x; want 0x01", got)
	}

	wantMin := []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01}
	wantMax := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f}

	earliestStart := preamble + 1
	earliestEnd := earliestStart + len(wantMin)
	if got := payload[earliestStart:earliestEnd]; !bytes.Equal(got, wantMin) {
		t.Errorf("earliest_ns(MinInt64) bytes:\n got  %x\n want %x", got, wantMin)
	}

	latestStart := earliestEnd
	latestEnd := latestStart + len(wantMax)
	if got := payload[latestStart:latestEnd]; !bytes.Equal(got, wantMax) {
		t.Errorf("latest_ns(MaxInt64) bytes:\n got  %x\n want %x", got, wantMax)
	}
}

// ---- QUERY_ROW ---------------------------------------------------------

func TestEncodeQueryRow_MatchesGolden(t *testing.T) {
	msg := QueryRowMsg{
		QueryID: 42,
		RowSeq:  5,
		RowJSON: `{"_time":"2026-05-20","x":1}`,
	}

	got := EncodeQueryRow(msg)
	want := loadHexFixture(t, "query_row.hex")

	if !bytes.Equal(got, want) {
		t.Fatalf("QUERY_ROW frame mismatch\n got  (%d bytes): %x\n want (%d bytes): %x",
			len(got), got, len(want), want)
	}
}

func TestDecodeQueryRow_RoundTripsGolden(t *testing.T) {
	raw := loadHexFixture(t, "query_row.hex")

	hdr, err := ReadHeader(bytes.NewReader(raw[:8]))
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	if hdr.Type != FrameQueryRow {
		t.Fatalf("header type = %v; want FrameQueryRow", hdr.Type)
	}
	if int(hdr.PayloadLen) != len(raw)-8 {
		t.Fatalf("header PayloadLen = %d; want %d", hdr.PayloadLen, len(raw)-8)
	}

	msg, err := DecodeQueryRow(bytes.NewReader(raw[8:]))
	if err != nil {
		t.Fatalf("DecodeQueryRow: %v", err)
	}
	if msg.QueryID != 42 {
		t.Errorf("QueryID = %d; want 42", msg.QueryID)
	}
	if msg.RowSeq != 5 {
		t.Errorf("RowSeq = %d; want 5", msg.RowSeq)
	}
	if msg.RowJSON != `{"_time":"2026-05-20","x":1}` {
		t.Errorf("RowJSON = %q; want %q", msg.RowJSON, `{"_time":"2026-05-20","x":1}`)
	}
}

// ---- QUERY_END ---------------------------------------------------------

func TestEncodeQueryEnd_OK_MatchesGolden(t *testing.T) {
	msg := QueryEndMsg{
		QueryID:       42,
		Status:        QueryStatusOK,
		RowsReturned:  100,
		EventsScanned: 50000,
		EventsMatched: 100,
		ElapsedMs:     1500,
		Truncated:     false,
		NextCursor:    "",
		Message:       "",
	}

	got := EncodeQueryEnd(msg)
	want := loadHexFixture(t, "query_end_ok.hex")

	if !bytes.Equal(got, want) {
		t.Fatalf("QUERY_END frame mismatch\n got  (%d bytes): %x\n want (%d bytes): %x",
			len(got), got, len(want), want)
	}
}

func TestDecodeQueryEnd_OK_RoundTripsGolden(t *testing.T) {
	raw := loadHexFixture(t, "query_end_ok.hex")

	hdr, err := ReadHeader(bytes.NewReader(raw[:8]))
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	if hdr.Type != FrameQueryEnd {
		t.Fatalf("header type = %v; want FrameQueryEnd", hdr.Type)
	}

	msg, err := DecodeQueryEnd(bytes.NewReader(raw[8:]))
	if err != nil {
		t.Fatalf("DecodeQueryEnd: %v", err)
	}
	if msg.QueryID != 42 {
		t.Errorf("QueryID = %d; want 42", msg.QueryID)
	}
	if msg.Status != QueryStatusOK {
		t.Errorf("Status = %d; want %d", msg.Status, QueryStatusOK)
	}
	if msg.RowsReturned != 100 {
		t.Errorf("RowsReturned = %d; want 100", msg.RowsReturned)
	}
	if msg.EventsScanned != 50000 {
		t.Errorf("EventsScanned = %d; want 50000", msg.EventsScanned)
	}
	if msg.EventsMatched != 100 {
		t.Errorf("EventsMatched = %d; want 100", msg.EventsMatched)
	}
	if msg.ElapsedMs != 1500 {
		t.Errorf("ElapsedMs = %d; want 1500", msg.ElapsedMs)
	}
	if msg.Truncated {
		t.Errorf("Truncated = true; want false")
	}
	if msg.NextCursor != "" {
		t.Errorf("NextCursor = %q; want empty", msg.NextCursor)
	}
	if msg.Message != "" {
		t.Errorf("Message = %q; want empty", msg.Message)
	}
}

func TestEncodeQueryEnd_Timeout_MatchesGolden(t *testing.T) {
	msg := QueryEndMsg{
		QueryID:       42,
		Status:        QueryStatusTimeout,
		RowsReturned:  1000,
		EventsScanned: 50000,
		EventsMatched: 1000,
		ElapsedMs:     30000,
		Truncated:     false,
		NextCursor:    "cur1",
		Message:       "query timeout after 30s",
	}

	got := EncodeQueryEnd(msg)
	want := loadHexFixture(t, "query_end_timeout.hex")

	if !bytes.Equal(got, want) {
		t.Fatalf("QUERY_END frame mismatch\n got  (%d bytes): %x\n want (%d bytes): %x",
			len(got), got, len(want), want)
	}
}

func TestDecodeQueryEnd_Timeout_RoundTripsGolden(t *testing.T) {
	raw := loadHexFixture(t, "query_end_timeout.hex")

	msg, err := DecodeQueryEnd(bytes.NewReader(raw[8:]))
	if err != nil {
		t.Fatalf("DecodeQueryEnd: %v", err)
	}
	if msg.Status != QueryStatusTimeout {
		t.Errorf("Status = %d; want %d", msg.Status, QueryStatusTimeout)
	}
	if msg.RowsReturned != 1000 {
		t.Errorf("RowsReturned = %d; want 1000", msg.RowsReturned)
	}
	if msg.ElapsedMs != 30000 {
		t.Errorf("ElapsedMs = %d; want 30000", msg.ElapsedMs)
	}
	if msg.NextCursor != "cur1" {
		t.Errorf("NextCursor = %q; want %q", msg.NextCursor, "cur1")
	}
	if msg.Message != "query timeout after 30s" {
		t.Errorf("Message = %q; want %q", msg.Message, "query timeout after 30s")
	}
}

// TestQueryEnd_TruncatedInvalidValue rejects bytes other than 0/1 in the
// truncated slot.
func TestQueryEnd_TruncatedInvalidValue(t *testing.T) {
	good := EncodeQueryEnd(QueryEndMsg{QueryID: 1})
	// Payload layout: varint(1)=01, varint(0)=00, varint(0)=00, varint(0)=00,
	// varint(0)=00, varint(0)=00, then u8 truncated. After 8-byte header
	// the truncated byte is at offset 8 + 6 = 14.
	corrupted := append([]byte(nil), good...)
	corrupted[14] = 0x02

	_, err := DecodeQueryEnd(bytes.NewReader(corrupted[8:]))
	if err == nil {
		t.Fatal("DecodeQueryEnd accepted truncated=0x02; want error")
	}
}

// ---- QUERY_PROGRESS ----------------------------------------------------

func TestEncodeQueryProgress_MatchesGolden(t *testing.T) {
	msg := QueryProgressMsg{
		QueryID:       42,
		EventsScanned: 10000,
		EventsMatched: 250,
		SegmentsDone:  3,
		SegmentsTotal: 10,
		ElapsedMs:     750,
	}

	got := EncodeQueryProgress(msg)
	want := loadHexFixture(t, "query_progress.hex")

	if !bytes.Equal(got, want) {
		t.Fatalf("QUERY_PROGRESS frame mismatch\n got  (%d bytes): %x\n want (%d bytes): %x",
			len(got), got, len(want), want)
	}
}

func TestDecodeQueryProgress_RoundTripsGolden(t *testing.T) {
	raw := loadHexFixture(t, "query_progress.hex")

	hdr, err := ReadHeader(bytes.NewReader(raw[:8]))
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	if hdr.Type != FrameQueryProgress {
		t.Fatalf("header type = %v; want FrameQueryProgress", hdr.Type)
	}

	msg, err := DecodeQueryProgress(bytes.NewReader(raw[8:]))
	if err != nil {
		t.Fatalf("DecodeQueryProgress: %v", err)
	}
	if msg.QueryID != 42 {
		t.Errorf("QueryID = %d; want 42", msg.QueryID)
	}
	if msg.EventsScanned != 10000 {
		t.Errorf("EventsScanned = %d; want 10000", msg.EventsScanned)
	}
	if msg.EventsMatched != 250 {
		t.Errorf("EventsMatched = %d; want 250", msg.EventsMatched)
	}
	if msg.SegmentsDone != 3 {
		t.Errorf("SegmentsDone = %d; want 3", msg.SegmentsDone)
	}
	if msg.SegmentsTotal != 10 {
		t.Errorf("SegmentsTotal = %d; want 10", msg.SegmentsTotal)
	}
	if msg.ElapsedMs != 750 {
		t.Errorf("ElapsedMs = %d; want 750", msg.ElapsedMs)
	}
}

// TestQueryRequest_HasTimeRangeInvalidValue rejects bytes other than 0/1
// in the has_time_range slot, matching the HELLO has_email contract.
func TestQueryRequest_HasTimeRangeInvalidValue(t *testing.T) {
	// Build a valid payload then corrupt the has_time_range byte to 0x02.
	good := EncodeQueryRequest(QueryRequestMsg{QueryID: 1, IndexName: "x", QueryText: "y"})
	// Layout after header: varint(1)=01, lenstr("x")=01 78, lenstr("y")=01 79,
	// varint(limit=0)=00, lenstr(cursor="")=00, then has_time_range byte.
	// Header is 8 bytes; payload offset of has_time_range = 8 + 1 + 2 + 2 + 1 + 1 = 15.
	corrupted := append([]byte(nil), good...)
	corrupted[15] = 0x02

	_, err := DecodeQueryRequest(bytes.NewReader(corrupted[8:]))
	if err == nil {
		t.Fatal("DecodeQueryRequest accepted has_time_range=0x02; want error")
	}
}
