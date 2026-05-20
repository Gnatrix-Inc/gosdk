package wire

import (
	"bytes"
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
