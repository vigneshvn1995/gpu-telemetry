// Package mq_test contains unit tests for the length-prefix wire protocol.
package mq_test

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"gpu-telemetry/pkg/mq"
)

// -------------------------------------------------------------------------
// WriteFrame / ReadFrame round-trip
// -------------------------------------------------------------------------

func TestProtocol_RoundTrip(t *testing.T) {
	t.Parallel()

	frames := []*mq.Frame{
		{Type: mq.FramePub, Topic: "gpu.util", Payload: json.RawMessage(`{"value":99.5}`)},
		{Type: mq.FrameSub, Topic: "telemetry.gpu.*"},
		{Type: mq.FrameUnsub, Topic: "telemetry.gpu.*"},
		{Type: mq.FrameMsg, Topic: "events", ID: "abc-123", Payload: json.RawMessage(`"hello"`)},
		{Type: mq.FrameAck, ID: "abc-123"},
		{Type: mq.FrameErr, ID: "xyz", Error: "bad request"},
		{Type: mq.FramePing},
		{Type: mq.FramePong},
	}

	for _, want := range frames {
		want := want
		t.Run(string(want.Type), func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer

			if err := mq.WriteFrame(&buf, want); err != nil {
				t.Fatalf("WriteFrame: %v", err)
			}

			got, err := mq.ReadFrame(&buf)
			if err != nil {
				t.Fatalf("ReadFrame: %v", err)
			}

			if got.Type != want.Type {
				t.Errorf("Type = %q, want %q", got.Type, want.Type)
			}
			if got.Topic != want.Topic {
				t.Errorf("Topic = %q, want %q", got.Topic, want.Topic)
			}
			if got.ID != want.ID {
				t.Errorf("ID = %q, want %q", got.ID, want.ID)
			}
			if got.Error != want.Error {
				t.Errorf("Error = %q, want %q", got.Error, want.Error)
			}
			if want.Payload != nil && string(got.Payload) != string(want.Payload) {
				t.Errorf("Payload = %s, want %s", got.Payload, want.Payload)
			}
		})
	}
}

func TestProtocol_MultipleFrames_SamePipe(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer

	in := []*mq.Frame{
		{Type: mq.FramePub, Topic: "a", Payload: json.RawMessage(`1`)},
		{Type: mq.FramePub, Topic: "b", Payload: json.RawMessage(`2`)},
		{Type: mq.FramePub, Topic: "c", Payload: json.RawMessage(`3`)},
	}
	for _, f := range in {
		if err := mq.WriteFrame(&buf, f); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
	}

	for i, want := range in {
		got, err := mq.ReadFrame(&buf)
		if err != nil {
			t.Fatalf("ReadFrame[%d]: %v", i, err)
		}
		if got.Topic != want.Topic {
			t.Errorf("[%d] Topic = %q, want %q", i, got.Topic, want.Topic)
		}
	}
}

// -------------------------------------------------------------------------
// Frame size enforcement
// -------------------------------------------------------------------------

func TestProtocol_WriteFrame_TooLarge(t *testing.T) {
	t.Parallel()
	// Build a payload that is larger than 4 MiB so the marshal result overflows.
	huge := mq.Frame{
		Type:    mq.FramePub,
		Payload: json.RawMessage(`"` + strings.Repeat("x", 4<<20+1) + `"`),
	}
	var buf bytes.Buffer
	err := mq.WriteFrame(&buf, &huge)
	if err == nil {
		t.Fatal("expected error for frame exceeding maxFrameSize, got nil")
	}
}

func TestProtocol_ReadFrame_TooLarge(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	// Write a length header that declares 4 MiB + 1 bytes.
	tooLarge := uint32((4 << 20) + 1)
	_ = binary.Write(&buf, binary.BigEndian, tooLarge)

	_, err := mq.ReadFrame(&buf)
	if err == nil {
		t.Fatal("expected error for oversized incoming frame, got nil")
	}
}

// -------------------------------------------------------------------------
// Error propagation
// -------------------------------------------------------------------------

func TestProtocol_ReadFrame_EOF(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer // empty – ReadFull will return io.ErrUnexpectedEOF
	_, err := mq.ReadFrame(&buf)
	if err == nil {
		t.Fatal("expected error on empty reader, got nil")
	}
}

func TestProtocol_ReadFrame_TruncatedBody(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	// Header says 100 bytes, but body is only 5.
	_ = binary.Write(&buf, binary.BigEndian, uint32(100))
	_, _ = buf.Write([]byte("short"))

	_, err := mq.ReadFrame(&buf)
	if err == nil {
		t.Fatal("expected error for truncated body, got nil")
	}
}

func TestProtocol_ReadFrame_InvalidJSON(t *testing.T) {
	t.Parallel()
	corrupt := []byte("not-valid-json")
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, uint32(len(corrupt)))
	_, _ = buf.Write(corrupt)

	_, err := mq.ReadFrame(&buf)
	if err == nil {
		t.Fatal("expected unmarshal error, got nil")
	}
}

func TestProtocol_WriteFrame_WriterError(t *testing.T) {
	t.Parallel()
	f := &mq.Frame{Type: mq.FramePing}
	// errWriter always fails on the first write.
	if err := mq.WriteFrame(&errWriter{}, f); err == nil {
		t.Fatal("expected write error, got nil")
	}
}

// errWriter is a writer that always returns an error.
type errWriter struct{}

func (e *errWriter) Write(_ []byte) (int, error) { return 0, io.ErrClosedPipe }
