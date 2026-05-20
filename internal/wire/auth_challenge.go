package wire

// AUTH_CHALLENGE (0x04) is reserved per wire-protocol.md §Frame types:
// the frame-type byte is allocated, no payload codec is defined yet.
// The type constant lives in frame.go as FrameAuthChallenge.
