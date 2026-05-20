package wire

// PingMsg represents PING (0x20). Empty payload per wire-protocol.md
// §PING/PONG.
type PingMsg struct{}

// PongMsg represents PONG (0x21). Empty payload per wire-protocol.md
// §PING/PONG. The server replies to every PING with exactly one PONG.
type PongMsg struct{}

// EncodePing emits a complete 8-byte PING frame (header only, no payload).
func EncodePing() []byte {
	return AppendHeader(nil, FramePing, 0)
}

// EncodePong emits a complete 8-byte PONG frame (header only, no payload).
func EncodePong() []byte {
	return AppendHeader(nil, FramePong, 0)
}
