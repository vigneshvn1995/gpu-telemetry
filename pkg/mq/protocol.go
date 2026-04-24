// Package mq provides the client and server libraries for the gpu-telemetry
// custom message broker. The wire protocol is a simple length-prefix framing
// scheme over TCP: each message is preceded by a 4-byte big-endian uint32
// that encodes the body length, followed by a JSON-encoded Frame.
//
// Design goals:
//   - Zero external runtime dependencies (stdlib only).
//   - Human-readable wire format for easy debugging with netcat / tcpdump.
//   - Extensible Frame type allows adding new message types without breaking
//     existing clients (unknown types are silently ignored).
package mq

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

// maxFrameSize caps individual frame bodies to 4 MiB. Frames larger than this
// are rejected to prevent a misbehaving client from exhausting broker memory.
const maxFrameSize = 4 << 20 // 4 MiB

// FrameType is a discriminated union tag carried in every Frame.
type FrameType string

const (
	// FramePub – client → server: publish payload to topic.
	FramePub FrameType = "pub"
	// FrameSub – client → server: subscribe to topic.
	FrameSub FrameType = "sub"
	// FrameUnsub – client → server: cancel subscription to topic.
	FrameUnsub FrameType = "unsub"
	// FrameMsg – server → client: a message has arrived on a subscribed topic.
	FrameMsg FrameType = "msg"
	// FrameAck – server → client: publish acknowledged.
	FrameAck FrameType = "ack"
	// FrameErr – server → client: error response to a client request.
	FrameErr FrameType = "err"
	// FramePing / FramePong – bidirectional keep-alive probes.
	FramePing FrameType = "ping"
	FramePong FrameType = "pong"
)

// Frame is the atomic unit exchanged between broker clients and the server.
// Fields are intentionally sparse – unused fields are omitted from JSON to
// keep the wire footprint small for high-throughput telemetry workloads.
type Frame struct {
	// Type identifies the frame's purpose (required).
	Type FrameType `json:"type"`

	// ID is an optional client-supplied correlation identifier. The server
	// echoes it back in ack/err responses so callers can match async replies.
	ID string `json:"id,omitempty"`

	// Topic is the routing key used for pub, sub, unsub, and msg frames.
	Topic string `json:"topic,omitempty"`

	// Payload carries the opaque message body for pub/msg frames. Keeping it
	// as json.RawMessage avoids double-serialization: the broker stores and
	// forwards the bytes verbatim without unmarshalling them.
	Payload json.RawMessage `json:"payload,omitempty"`

	// Error is populated only in FrameErr responses.
	Error string `json:"error,omitempty"`
}

// WriteFrame serialises f and writes it to w using 4-byte length-prefix framing.
// It is NOT safe to call WriteFrame concurrently on the same writer; callers
// must serialise access externally.
func WriteFrame(w io.Writer, f *Frame) error {
	body, err := json.Marshal(f)
	if err != nil {
		return fmt.Errorf("mq: marshal frame: %w", err)
	}
	if len(body) > maxFrameSize {
		return fmt.Errorf("mq: frame size %d exceeds limit %d", len(body), maxFrameSize)
	}

	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body)))

	// Two writes; the OS will typically coalesce them into one TCP segment.
	if _, err = w.Write(hdr[:]); err != nil {
		return fmt.Errorf("mq: write frame header: %w", err)
	}
	if _, err = w.Write(body); err != nil {
		return fmt.Errorf("mq: write frame body: %w", err)
	}
	return nil
}

// ReadFrame reads exactly one length-prefixed frame from r.
// It blocks until the full frame is available or an error occurs.
func ReadFrame(r io.Reader) (*Frame, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, fmt.Errorf("mq: read frame header: %w", err)
	}

	n := binary.BigEndian.Uint32(hdr[:])
	if n > maxFrameSize {
		return nil, fmt.Errorf("mq: incoming frame size %d exceeds limit %d", n, maxFrameSize)
	}

	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, fmt.Errorf("mq: read frame body: %w", err)
	}

	var f Frame
	if err := json.Unmarshal(body, &f); err != nil {
		return nil, fmt.Errorf("mq: unmarshal frame: %w", err)
	}
	return &f, nil
}
