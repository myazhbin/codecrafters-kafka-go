package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"
)

// exampleRequestHex is the ApiVersions request from the "Parse Correlation ID"
// stage instructions.
const exampleRequestHex = "00000023" + // message_size:        35
	"0012" + // request_api_key:     18
	"0004" + // request_api_version: 4
	"6f7fc661" + // correlation_id:      1870644833
	"00096b61666b612d636c69" + // client_id:           "kafka-cli"
	"00" + // tag buffer
	"0a6b61666b612d636c69" + // client_software_name:    "kafka-cli"
	"04302e31" + // client_software_version: "0.1"
	"00" // tag buffer

// mustDecodeHex decodes a hex string, ignoring any whitespace in it.
func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()

	b, err := hex.DecodeString(strings.Join(strings.Fields(s), ""))
	if err != nil {
		t.Fatalf("invalid hex string %q: %v", s, err)
	}
	return b
}

func TestReadMessage(t *testing.T) {
	t.Run("returns the header and body", func(t *testing.T) {
		request := mustDecodeHex(t, exampleRequestHex)

		message, err := readMessage(bytes.NewReader(request))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !bytes.Equal(message, request[4:]) {
			t.Errorf("message = % x, want % x", message, request[4:])
		}
	})

	t.Run("reads consecutive messages", func(t *testing.T) {
		r := bytes.NewReader(mustDecodeHex(t, "00000002 aabb 00000001 cc"))

		for _, want := range [][]byte{{0xaa, 0xbb}, {0xcc}} {
			message, err := readMessage(r)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !bytes.Equal(message, want) {
				t.Errorf("message = % x, want % x", message, want)
			}
		}

		if _, err := readMessage(r); !errors.Is(err, io.EOF) {
			t.Errorf("error after the last message = %v, want io.EOF", err)
		}
	})

	t.Run("reassembles a message that arrives in pieces", func(t *testing.T) {
		request := mustDecodeHex(t, exampleRequestHex)

		// Like a slow network, the reader hands out one byte per Read call.
		message, err := readMessage(iotest.OneByteReader(bytes.NewReader(request)))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !bytes.Equal(message, request[4:]) {
			t.Errorf("message = % x, want % x", message, request[4:])
		}
	})

	errorTests := []struct {
		name    string
		input   string
		wantErr error // nil means "any error"
	}{
		{name: "empty input", input: "", wantErr: io.EOF},
		{name: "truncated message_size", input: "0000", wantErr: io.ErrUnexpectedEOF},
		{name: "missing message", input: "00000004", wantErr: io.ErrUnexpectedEOF},
		{name: "truncated message", input: "00000004 aabb", wantErr: io.ErrUnexpectedEOF},
		{name: "negative message_size", input: "ffffffff"},
		{name: "oversized message_size", input: "7fffffff"},
	}

	for _, tt := range errorTests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := readMessage(bytes.NewReader(mustDecodeHex(t, tt.input)))
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestDecodeRequestHeader(t *testing.T) {
	t.Run("example from the stage instructions", func(t *testing.T) {
		request := mustDecodeHex(t, exampleRequestHex)
		d := newDecoder(request[4:])

		header, err := decodeRequestHeader(d)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if header.apiKey != 18 {
			t.Errorf("apiKey = %d, want 18", header.apiKey)
		}
		if header.apiVersion != 4 {
			t.Errorf("apiVersion = %d, want 4", header.apiVersion)
		}
		if header.correlationID != 1870644833 {
			t.Errorf("correlationID = %d, want 1870644833", header.correlationID)
		}
		if header.clientID == nil || *header.clientID != "kafka-cli" {
			t.Errorf("clientID = %v, want \"kafka-cli\"", header.clientID)
		}

		// The decoder must be left at the start of the request body.
		wantBody := mustDecodeHex(t, "0a6b61666b612d636c69 04302e31 00")
		if !bytes.Equal(d.buf, wantBody) {
			t.Errorf("remaining body = % x, want % x", d.buf, wantBody)
		}
	})

	t.Run("null client_id", func(t *testing.T) {
		header, err := decodeRequestHeader(newDecoder(mustDecodeHex(t, "0012 0004 00000007 ffff 00")))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if header.clientID != nil {
			t.Errorf("clientID = %q, want nil", *header.clientID)
		}
		if header.correlationID != 7 {
			t.Errorf("correlationID = %d, want 7", header.correlationID)
		}
	})

	t.Run("empty client_id", func(t *testing.T) {
		header, err := decodeRequestHeader(newDecoder(mustDecodeHex(t, "0012 0004 00000007 0000 00")))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if header.clientID == nil || *header.clientID != "" {
			t.Errorf("clientID = %v, want an empty string", header.clientID)
		}
	})

	t.Run("negative correlation_id", func(t *testing.T) {
		header, err := decodeRequestHeader(newDecoder(mustDecodeHex(t, "0012 0004 80000000 ffff 00")))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if header.correlationID != -2147483648 {
			t.Errorf("correlationID = %d, want -2147483648", header.correlationID)
		}
	})

	t.Run("tagged fields are skipped", func(t *testing.T) {
		// Two tagged fields: tag 0 with 2 bytes of data, tag 1 with 1 byte.
		d := newDecoder(mustDecodeHex(t, "0012 0004 00000007 ffff 02 00 02 aabb 01 01 cc ee"))

		if _, err := decodeRequestHeader(d); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !bytes.Equal(d.buf, []byte{0xee}) {
			t.Errorf("remaining body = % x, want ee", d.buf)
		}
	})

	errorTests := []struct {
		name  string
		input string
	}{
		{name: "empty header", input: ""},
		{name: "truncated request_api_key", input: "00"},
		{name: "truncated request_api_version", input: "0012 00"},
		{name: "truncated correlation_id", input: "0012 0004 0000"},
		{name: "missing client_id", input: "0012 0004 00000007"},
		{name: "truncated client_id", input: "0012 0004 00000007 0009 6b61"},
		{name: "invalid client_id length", input: "0012 0004 00000007 fffe 00"},
		{name: "missing tag buffer", input: "0012 0004 00000007 ffff"},
		{name: "truncated tagged field", input: "0012 0004 00000007 ffff 01 00 04 aabb"},
	}

	for _, tt := range errorTests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := decodeRequestHeader(newDecoder(mustDecodeHex(t, tt.input))); err == nil {
				t.Error("expected an error, got nil")
			}
		})
	}
}

func TestDecoderIntegers(t *testing.T) {
	d := newDecoder(mustDecodeHex(t, "0102 fffe 01020304 fffffffe"))

	if got := d.int16(); got != 0x0102 {
		t.Errorf("int16() = %#x, want 0x0102", got)
	}
	if got := d.int16(); got != -2 {
		t.Errorf("int16() = %d, want -2", got)
	}
	if got := d.int32(); got != 0x01020304 {
		t.Errorf("int32() = %#x, want 0x01020304", got)
	}
	if got := d.int32(); got != -2 {
		t.Errorf("int32() = %d, want -2", got)
	}
	if d.err != nil {
		t.Errorf("unexpected error: %v", d.err)
	}
	if len(d.buf) != 0 {
		t.Errorf("expected the whole input to be consumed, %d bytes left", len(d.buf))
	}
}

func TestDecoderUvarint(t *testing.T) {
	tests := []struct {
		input string
		want  uint32
	}{
		{input: "00", want: 0},
		{input: "01", want: 1},
		{input: "7f", want: 127},
		{input: "8001", want: 128},
		{input: "ac02", want: 300},
		{input: "ffffffff0f", want: 4294967295},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			d := newDecoder(mustDecodeHex(t, tt.input))

			if got := d.uvarint(); got != tt.want {
				t.Errorf("uvarint() = %d, want %d", got, tt.want)
			}
			if d.err != nil {
				t.Errorf("unexpected error: %v", d.err)
			}
			if len(d.buf) != 0 {
				t.Errorf("expected the whole input to be consumed, %d bytes left", len(d.buf))
			}

			// The encoding must agree with the standard library's.
			if want := binary.AppendUvarint(nil, uint64(tt.want)); !bytes.Equal(want, mustDecodeHex(t, tt.input)) {
				t.Errorf("test input % x is not the canonical encoding % x", mustDecodeHex(t, tt.input), want)
			}
		})
	}

	errorTests := []struct {
		name  string
		input string
	}{
		{name: "empty input", input: ""},
		{name: "unterminated varint", input: "80"},
		{name: "more than 32 bits", input: "ffffffff1f"},
		{name: "more than 64 bits", input: "ffffffffffffffffffff01"},
	}

	for _, tt := range errorTests {
		t.Run(tt.name, func(t *testing.T) {
			d := newDecoder(mustDecodeHex(t, tt.input))

			if got := d.uvarint(); got != 0 {
				t.Errorf("uvarint() = %d, want 0 on error", got)
			}
			if d.err == nil {
				t.Error("expected an error, got nil")
			}
		})
	}
}

// Once a read has failed, the decoder keeps reporting that first error and
// stops consuming input.
func TestDecoderErrorIsSticky(t *testing.T) {
	d := newDecoder([]byte{0x00, 0x01, 0x02})

	if got := d.int32(); got != 0 {
		t.Errorf("int32() = %d, want 0 on error", got)
	}
	if !errors.Is(d.err, errTruncated) {
		t.Fatalf("err = %v, want errTruncated", d.err)
	}

	// These would succeed on a fresh decoder, but not after a failure.
	if got := d.int16(); got != 0 {
		t.Errorf("int16() after an error = %d, want 0", got)
	}
	if got := d.uvarint(); got != 0 {
		t.Errorf("uvarint() after an error = %d, want 0", got)
	}
	if got := d.nullableString(); got != nil {
		t.Errorf("nullableString() after an error = %q, want nil", *got)
	}
	if got := d.bytes(1); got != nil {
		t.Errorf("bytes() after an error = % x, want nil", got)
	}
	d.taggedFields()

	if !errors.Is(d.err, errTruncated) {
		t.Errorf("err = %v, want it to still be errTruncated", d.err)
	}
	if len(d.buf) != 3 {
		t.Errorf("expected no input to be consumed after an error, %d bytes left", len(d.buf))
	}
}
