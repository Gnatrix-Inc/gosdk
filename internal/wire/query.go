package wire

import (
	"fmt"
	"io"
)

// QueryStatus values for QUERY_END (wire-protocol.md §QUERY_END).
const (
	QueryStatusOK                  uint64 = 0
	QueryStatusCancelled           uint64 = 1
	QueryStatusEngineError         uint64 = 2
	QueryStatusPermissionDenied    uint64 = 3
	QueryStatusTenantQuotaExceeded uint64 = 4
)

// ---- QUERY_REQUEST (0x10) ----------------------------------------------

// QueryRequestMsg is the decoded form of a QUERY_REQUEST frame payload.
//
// NOTE: this scaffolding reflects the v1 spec minus the dropped `query_kind`
// field. The v2 spec (server_client/docs/schema/wire-protocolv2.md) adds
// has_time_range / earliest_ns / latest_ns / progress_interval_ms, which
// will land with Slice 1. Do not consume this struct from the public client
// yet — it is inert wire scaffolding.
type QueryRequestMsg struct {
	QueryID   uint64 // client-chosen correlation id
	IndexName string // target index, e.g. "logs-2026"
	QueryText string // the query body
	Limit     uint64 // max rows the client wants; 0 = no cap
	Cursor    string // optional opaque pagination cursor
}

// EncodeQueryRequest encodes msg into a complete on-wire QUERY_REQUEST frame.
func EncodeQueryRequest(msg QueryRequestMsg) []byte {
	var p []byte
	p = AppendVarint(p, msg.QueryID)
	p = AppendLenStr(p, msg.IndexName)
	p = AppendLenStr(p, msg.QueryText)
	p = AppendVarint(p, msg.Limit)
	p = AppendLenStr(p, msg.Cursor)

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
// Always emitted (even on failure) so the client knows the request is done.
type QueryEndMsg struct {
	QueryID      uint64
	Status       uint64 // one of QueryStatus* constants
	RowsReturned uint64
	NextCursor   string // non-empty if more rows are available
	Message      string // diagnostic for non-ok status
}

// EncodeQueryEnd encodes msg into a complete on-wire QUERY_END frame.
func EncodeQueryEnd(msg QueryEndMsg) []byte {
	var p []byte
	p = AppendVarint(p, msg.QueryID)
	p = AppendVarint(p, msg.Status)
	p = AppendVarint(p, msg.RowsReturned)
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
//
// Reserved (not in wire-protocol.md v1 yet). Type constant:
// FrameQueryProgress in frame.go.
