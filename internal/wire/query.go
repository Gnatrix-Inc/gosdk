package wire

import (
	"fmt"
	"io"
)

// QueryStatus values for QUERY_END (wire-protocolv2.md §QUERY_END).
const (
	QueryStatusOK                  uint64 = 0
	QueryStatusCancelled           uint64 = 1
	QueryStatusEngineError         uint64 = 2
	QueryStatusPermissionDenied    uint64 = 3
	QueryStatusTenantQuotaExceeded uint64 = 4
	QueryStatusMemoryLimitExceeded uint64 = 5
	QueryStatusTimeout             uint64 = 6
	QueryStatusStorageUnavailable  uint64 = 7
)

// ---- QUERY_REQUEST (0x10) ----------------------------------------------

// QueryRequestMsg is the decoded form of a QUERY_REQUEST frame payload.
// Matches wire-protocolv2.md §QUERY_REQUEST.
type QueryRequestMsg struct {
	QueryID   uint64 // client-chosen correlation id
	IndexName string // target index, e.g. "logs-2026"
	QueryText string // the query body
	Limit     uint64 // max rows the client wants; 0 = no cap
	Cursor    string // optional opaque pagination cursor

	// HasTimeRange controls whether EarliestNs/LatestNs are emitted.
	// Encoded as a raw u8 (0x00 or 0x01), not a varint.
	HasTimeRange bool
	// EarliestNs / LatestNs are int64 ns since Unix epoch, encoded as
	// varint of the two's-complement uint64 reinterpretation. Decoder
	// casts back to int64. Both fields are only on the wire when
	// HasTimeRange is true.
	EarliestNs int64
	LatestNs   int64

	// ProgressIntervalMs requests QUERY_PROGRESS cadence. 0 = use the
	// server default (250 ms per spec).
	ProgressIntervalMs uint32
}

// EncodeQueryRequest encodes msg into a complete on-wire QUERY_REQUEST frame.
func EncodeQueryRequest(msg QueryRequestMsg) []byte {
	var p []byte
	p = AppendVarint(p, msg.QueryID)
	p = AppendLenStr(p, msg.IndexName)
	p = AppendLenStr(p, msg.QueryText)
	p = AppendVarint(p, msg.Limit)
	p = AppendLenStr(p, msg.Cursor)

	if msg.HasTimeRange {
		p = append(p, 0x01)
		p = AppendVarint(p, uint64(msg.EarliestNs))
		p = AppendVarint(p, uint64(msg.LatestNs))
	} else {
		p = append(p, 0x00)
	}

	p = AppendVarint(p, uint64(msg.ProgressIntervalMs))

	frame := AppendHeader(nil, FrameQueryRequest, uint32(len(p)))
	return append(frame, p...)
}

// DecodeQueryRequest reads a QUERY_REQUEST payload from r.
func DecodeQueryRequest(r io.Reader) (QueryRequestMsg, error) {
	var m QueryRequestMsg
	var err error
	if m.QueryID, err = ReadVarint(r); err != nil {
		return m, fmt.Errorf("wire: query_request query_id: %w", err)
	}
	if m.IndexName, err = ReadLenStr(r); err != nil {
		return m, fmt.Errorf("wire: query_request index_name: %w", err)
	}
	if m.QueryText, err = ReadLenStr(r); err != nil {
		return m, fmt.Errorf("wire: query_request query_text: %w", err)
	}
	if m.Limit, err = ReadVarint(r); err != nil {
		return m, fmt.Errorf("wire: query_request limit: %w", err)
	}
	if m.Cursor, err = ReadLenStr(r); err != nil {
		return m, fmt.Errorf("wire: query_request cursor: %w", err)
	}

	var hasTR [1]byte
	if _, err := io.ReadFull(r, hasTR[:]); err != nil {
		return m, fmt.Errorf("wire: query_request has_time_range: %w", err)
	}
	switch hasTR[0] {
	case 0:
		// no time range
	case 1:
		m.HasTimeRange = true
		earliest, err := ReadVarint(r)
		if err != nil {
			return m, fmt.Errorf("wire: query_request earliest_ns: %w", err)
		}
		m.EarliestNs = int64(earliest)
		latest, err := ReadVarint(r)
		if err != nil {
			return m, fmt.Errorf("wire: query_request latest_ns: %w", err)
		}
		m.LatestNs = int64(latest)
	default:
		return m, fmt.Errorf("wire: query_request has_time_range invalid value %d", hasTR[0])
	}

	progress, err := ReadVarint(r)
	if err != nil {
		return m, fmt.Errorf("wire: query_request progress_interval_ms: %w", err)
	}
	if progress > 0xFFFFFFFF {
		return m, fmt.Errorf("wire: query_request progress_interval_ms %d exceeds uint32", progress)
	}
	m.ProgressIntervalMs = uint32(progress)

	return m, nil
}

// ---- QUERY_ROW (0x11) --------------------------------------------------

// QueryRowMsg is the decoded form of a QUERY_ROW frame payload.
// QUERY_ROW frames are streamed: zero or more per QUERY_REQUEST,
// terminated by exactly one QUERY_END.
type QueryRowMsg struct {
	QueryID uint64 // correlation id from QUERY_REQUEST
	RowSeq  uint64 // monotonic per QueryID, starts at 0
	RowJSON string // one row as canonical JSON
}

// EncodeQueryRow encodes msg into a complete on-wire QUERY_ROW frame.
func EncodeQueryRow(msg QueryRowMsg) []byte {
	var p []byte
	p = AppendVarint(p, msg.QueryID)
	p = AppendVarint(p, msg.RowSeq)
	p = AppendLenStr(p, msg.RowJSON)

	frame := AppendHeader(nil, FrameQueryRow, uint32(len(p)))
	return append(frame, p...)
}

// DecodeQueryRow reads a QUERY_ROW payload from r.
func DecodeQueryRow(r io.Reader) (QueryRowMsg, error) {
	var m QueryRowMsg
	var err error
	if m.QueryID, err = ReadVarint(r); err != nil {
		return m, fmt.Errorf("wire: query_row query_id: %w", err)
	}
	if m.RowSeq, err = ReadVarint(r); err != nil {
		return m, fmt.Errorf("wire: query_row row_seq: %w", err)
	}
	if m.RowJSON, err = ReadLenStr(r); err != nil {
		return m, fmt.Errorf("wire: query_row row_json: %w", err)
	}
	return m, nil
}

// ---- QUERY_END (0x12) --------------------------------------------------

// QueryEndMsg is the decoded form of a QUERY_END frame payload.
// Always emitted (even on failure) so the client knows the request is
// done. Matches wire-protocolv2.md §QUERY_END.
type QueryEndMsg struct {
	QueryID       uint64
	Status        uint64 // one of QueryStatus* constants
	RowsReturned  uint64
	EventsScanned uint64
	EventsMatched uint64
	ElapsedMs     uint64
	// Truncated is true iff the server cut rows because Limit was hit.
	// Encoded as a raw u8 (0x00 or 0x01).
	Truncated  bool
	NextCursor string // non-empty if more rows are available
	Message    string // diagnostic for non-ok status; "NNNN: msg" prefix when Status=2
}

// EncodeQueryEnd encodes msg into a complete on-wire QUERY_END frame.
func EncodeQueryEnd(msg QueryEndMsg) []byte {
	var p []byte
	p = AppendVarint(p, msg.QueryID)
	p = AppendVarint(p, msg.Status)
	p = AppendVarint(p, msg.RowsReturned)
	p = AppendVarint(p, msg.EventsScanned)
	p = AppendVarint(p, msg.EventsMatched)
	p = AppendVarint(p, msg.ElapsedMs)
	if msg.Truncated {
		p = append(p, 0x01)
	} else {
		p = append(p, 0x00)
	}
	p = AppendLenStr(p, msg.NextCursor)
	p = AppendLenStr(p, msg.Message)

	frame := AppendHeader(nil, FrameQueryEnd, uint32(len(p)))
	return append(frame, p...)
}

// DecodeQueryEnd reads a QUERY_END payload from r.
func DecodeQueryEnd(r io.Reader) (QueryEndMsg, error) {
	var m QueryEndMsg
	var err error
	if m.QueryID, err = ReadVarint(r); err != nil {
		return m, fmt.Errorf("wire: query_end query_id: %w", err)
	}
	if m.Status, err = ReadVarint(r); err != nil {
		return m, fmt.Errorf("wire: query_end status: %w", err)
	}
	if m.RowsReturned, err = ReadVarint(r); err != nil {
		return m, fmt.Errorf("wire: query_end rows_returned: %w", err)
	}
	if m.EventsScanned, err = ReadVarint(r); err != nil {
		return m, fmt.Errorf("wire: query_end events_scanned: %w", err)
	}
	if m.EventsMatched, err = ReadVarint(r); err != nil {
		return m, fmt.Errorf("wire: query_end events_matched: %w", err)
	}
	if m.ElapsedMs, err = ReadVarint(r); err != nil {
		return m, fmt.Errorf("wire: query_end elapsed_ms: %w", err)
	}

	var truncated [1]byte
	if _, err := io.ReadFull(r, truncated[:]); err != nil {
		return m, fmt.Errorf("wire: query_end truncated: %w", err)
	}
	switch truncated[0] {
	case 0:
		m.Truncated = false
	case 1:
		m.Truncated = true
	default:
		return m, fmt.Errorf("wire: query_end truncated invalid value %d", truncated[0])
	}

	if m.NextCursor, err = ReadLenStr(r); err != nil {
		return m, fmt.Errorf("wire: query_end next_cursor: %w", err)
	}
	if m.Message, err = ReadLenStr(r); err != nil {
		return m, fmt.Errorf("wire: query_end message: %w", err)
	}
	return m, nil
}

// ---- QUERY_CANCEL (0x13) -----------------------------------------------
//
// Reserved per wire-protocol.md §Frame types: type byte allocated, no
// payload codec defined yet. Type constant: FrameQueryCancel in frame.go.

// ---- QUERY_PROGRESS (0x14) ---------------------------------------------

// QueryProgressMsg is the decoded form of a QUERY_PROGRESS frame
// payload. Emitted periodically by the server during query execution
// (cadence controlled by QueryRequestMsg.ProgressIntervalMs).
// Matches wire-protocolv2.md §QUERY_PROGRESS.
//
// Counters are cumulative from query start and monotonically increasing
// within a single query. The last QUERY_PROGRESS is not guaranteed to
// hold the final totals — QUERY_END carries the authoritative numbers.
type QueryProgressMsg struct {
	QueryID        uint64
	EventsScanned  uint64
	EventsMatched  uint64
	SegmentsDone   uint64
	SegmentsTotal  uint64
	ElapsedMs      uint64
}

// EncodeQueryProgress encodes msg into a complete on-wire
// QUERY_PROGRESS frame.
func EncodeQueryProgress(msg QueryProgressMsg) []byte {
	var p []byte
	p = AppendVarint(p, msg.QueryID)
	p = AppendVarint(p, msg.EventsScanned)
	p = AppendVarint(p, msg.EventsMatched)
	p = AppendVarint(p, msg.SegmentsDone)
	p = AppendVarint(p, msg.SegmentsTotal)
	p = AppendVarint(p, msg.ElapsedMs)

	frame := AppendHeader(nil, FrameQueryProgress, uint32(len(p)))
	return append(frame, p...)
}

// DecodeQueryProgress reads a QUERY_PROGRESS payload from r.
func DecodeQueryProgress(r io.Reader) (QueryProgressMsg, error) {
	var m QueryProgressMsg
	var err error
	if m.QueryID, err = ReadVarint(r); err != nil {
		return m, fmt.Errorf("wire: query_progress query_id: %w", err)
	}
	if m.EventsScanned, err = ReadVarint(r); err != nil {
		return m, fmt.Errorf("wire: query_progress events_scanned: %w", err)
	}
	if m.EventsMatched, err = ReadVarint(r); err != nil {
		return m, fmt.Errorf("wire: query_progress events_matched: %w", err)
	}
	if m.SegmentsDone, err = ReadVarint(r); err != nil {
		return m, fmt.Errorf("wire: query_progress segments_done: %w", err)
	}
	if m.SegmentsTotal, err = ReadVarint(r); err != nil {
		return m, fmt.Errorf("wire: query_progress segments_total: %w", err)
	}
	if m.ElapsedMs, err = ReadVarint(r); err != nil {
		return m, fmt.Errorf("wire: query_progress elapsed_ms: %w", err)
	}
	return m, nil
}
