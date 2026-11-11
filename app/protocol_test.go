package main

import (
	"bytes"
	"encoding/binary"
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
	d := newDecoder(mustDecodeHex(t, "01 ff 0102 fffe 01020304 fffffffe"))

	if got := d.int8(); got != 1 {
		t.Errorf("int8() = %d, want 1", got)
	}
	if got := d.int8(); got != -1 {
		t.Errorf("int8() = %d, want -1", got)
	}
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
	if got := d.int8(); got != 0 {
		t.Errorf("int8() after an error = %d, want 0", got)
	}
	if got := d.uvarint(); got != 0 {
		t.Errorf("uvarint() after an error = %d, want 0", got)
	}
	if got := d.compactString(); got != "" {
		t.Errorf("compactString() after an error = %q, want \"\"", got)
	}
	if got := d.compactArrayLength(); got != 0 {
		t.Errorf("compactArrayLength() after an error = %d, want 0", got)
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

func TestEncodeResponse(t *testing.T) {
	tests := []struct {
		name          string
		headerVersion int
		correlationID int32
		body          string
		want          string
	}{
		{
			name:          "header v0 only",
			headerVersion: responseHeaderV0,
			correlationID: 7,
			body:          "",
			want:          "00000004 00000007",
		},
		{
			name:          "big-endian byte order",
			headerVersion: responseHeaderV0,
			correlationID: 0x01020304,
			body:          "",
			want:          "00000004 01020304",
		},
		{
			name:          "negative correlation ID",
			headerVersion: responseHeaderV0,
			correlationID: -1,
			body:          "",
			want:          "00000004 ffffffff",
		},
		{
			name:          "message_size covers the header and the body",
			headerVersion: responseHeaderV0,
			correlationID: 1333056139,
			body:          "0023",
			want:          "00000006 4f74d28b 0023",
		},
		{
			name:          "header v1 adds an empty tag buffer",
			headerVersion: responseHeaderV1,
			correlationID: 7,
			body:          "",
			want:          "00000005 00000007 00",
		},
		{
			name:          "header v1 with a body",
			headerVersion: responseHeaderV1,
			correlationID: 7,
			body:          "aabbcc",
			want:          "00000008 00000007 00 aabbcc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := encodeResponse(tt.headerVersion, tt.correlationID, mustDecodeHex(t, tt.body))
			if want := mustDecodeHex(t, tt.want); !bytes.Equal(got, want) {
				t.Errorf("encodeResponse(%d, %d, %s) = % x, want % x", tt.headerVersion, tt.correlationID, tt.body, got, want)
			}
		})
	}
}

func TestEncoderIntegers(t *testing.T) {
	var e encoder
	e.int8(0x01)
	e.int8(-1)
	e.int16(0x0102)
	e.int16(-2)
	e.int32(0x01020304)
	e.int32(-2)
	e.int64(0x0102030405060708)
	e.int64(-2)
	e.raw([]byte{0xaa, 0xbb})

	if want := mustDecodeHex(t, "01 ff 0102 fffe 01020304 fffffffe 0102030405060708 fffffffffffffffe aabb"); !bytes.Equal(e.buf, want) {
		t.Errorf("encoded = % x, want % x", e.buf, want)
	}
}

func TestEncoderBoolean(t *testing.T) {
	var e encoder
	e.boolean(false)
	e.boolean(true)

	if want := []byte{0x00, 0x01}; !bytes.Equal(e.buf, want) {
		t.Errorf("encoded = % x, want % x", e.buf, want)
	}
}

func TestEncoderUUID(t *testing.T) {
	id := uuid(mustDecodeHex(t, "00010203 0405 0607 0809 0a0b0c0d0e0f"))

	var e encoder
	e.uuid(id)
	e.uuid(uuid{})

	if want := mustDecodeHex(t, "000102030405060708090a0b0c0d0e0f 00000000000000000000000000000000"); !bytes.Equal(e.buf, want) {
		t.Errorf("encoded = % x, want % x", e.buf, want)
	}
}

func TestUUIDString(t *testing.T) {
	tests := []struct {
		id   uuid
		want string
	}{
		{id: uuid{}, want: "00000000-0000-0000-0000-000000000000"},
		{id: uuid(mustDecodeHex(t, "000102030405060708090a0b0c0d0e0f")), want: "00010203-0405-0607-0809-0a0b0c0d0e0f"},
		{id: uuid(mustDecodeHex(t, "71a59a5189694f8ba7a6a1aa3bc6d8d5")), want: "71a59a51-8969-4f8b-a7a6-a1aa3bc6d8d5"},
	}

	for _, tt := range tests {
		if got := tt.id.String(); got != tt.want {
			t.Errorf("String() = %q, want %q", got, tt.want)
		}
	}
}

func TestCompactString(t *testing.T) {
	tests := []struct {
		value string
		want  string
	}{
		{value: "", want: "01"},
		{value: "foo", want: "04 666f6f"},
		{value: "kafka-cli", want: "0a 6b61666b612d636c69"},
		{value: strings.Repeat("a", 127), want: "8001" + strings.Repeat("61", 127)},
	}

	for _, tt := range tests {
		var e encoder
		e.compactString(tt.value)

		if want := mustDecodeHex(t, tt.want); !bytes.Equal(e.buf, want) {
			t.Errorf("compactString(%q) = % x, want % x", tt.value, e.buf, want)
		}

		// The decoder must read back what the encoder wrote.
		d := newDecoder(e.buf)
		if got := d.compactString(); got != tt.value || d.err != nil || len(d.buf) != 0 {
			t.Errorf("decoded %q (err: %v, %d bytes left), want %q", got, d.err, len(d.buf), tt.value)
		}
	}

	errorTests := []struct {
		name  string
		input string
	}{
		{name: "empty input", input: ""},
		{name: "null string", input: "00"},
		{name: "truncated string", input: "04 666f"},
		{name: "unterminated length", input: "80"},
	}

	for _, tt := range errorTests {
		t.Run(tt.name, func(t *testing.T) {
			d := newDecoder(mustDecodeHex(t, tt.input))

			if got := d.compactString(); got != "" {
				t.Errorf("compactString() = %q, want \"\" on error", got)
			}
			if d.err == nil {
				t.Error("expected an error, got nil")
			}
		})
	}
}

func TestDecoderCompactNullableString(t *testing.T) {
	tests := []struct {
		input string
		want  *string // nil for null
	}{
		{input: "00", want: nil},
		{input: "01", want: new(string)},
		{input: "04 666f6f", want: func() *string { s := "foo"; return &s }()},
	}

	for _, tt := range tests {
		d := newDecoder(mustDecodeHex(t, tt.input))

		got := d.compactNullableString()
		if d.err != nil || len(d.buf) != 0 {
			t.Errorf("compactNullableString() of %s: err = %v, %d bytes left", tt.input, d.err, len(d.buf))
		}
		if (got == nil) != (tt.want == nil) || (got != nil && *got != *tt.want) {
			t.Errorf("compactNullableString() of %s = %v, want %v", tt.input, got, tt.want)
		}
	}

	for _, input := range []string{"", "04 666f", "80"} {
		d := newDecoder(mustDecodeHex(t, input))

		if got := d.compactNullableString(); got != nil || d.err == nil {
			t.Errorf("compactNullableString() of %q = %v (err: %v), want an error", input, got, d.err)
		}
	}
}

func TestDecoderCompactBytes(t *testing.T) {
	tests := []struct {
		input string
		want  []byte // nil for null
	}{
		{input: "00", want: nil},
		{input: "01", want: []byte{}},
		{input: "04 aabbcc", want: []byte{0xaa, 0xbb, 0xcc}},
		{input: "c901" + strings.Repeat("ee", 200), want: bytes.Repeat([]byte{0xee}, 200)},
	}

	for _, tt := range tests {
		d := newDecoder(mustDecodeHex(t, tt.input))

		got := d.compactBytes()
		if d.err != nil || len(d.buf) != 0 {
			t.Errorf("compactBytes() of %.12s: err = %v, %d bytes left", tt.input, d.err, len(d.buf))
		}
		if (got == nil) != (tt.want == nil) || !bytes.Equal(got, tt.want) {
			t.Errorf("compactBytes() of %.12s = % x, want % x", tt.input, got, tt.want)
		}

		// The encoder must write what the decoder reads (null aside, which
		// the broker never has to write).
		if tt.want != nil {
			var e encoder
			e.compactBytes(tt.want)
			if !bytes.Equal(e.buf, mustDecodeHex(t, tt.input)) {
				t.Errorf("compactBytes(% x) is encoded as % x, want %s", tt.want, e.buf, tt.input)
			}
		}
	}

	for _, input := range []string{"", "04 aabb", "80"} {
		d := newDecoder(mustDecodeHex(t, input))

		if got := d.compactBytes(); got != nil || d.err == nil {
			t.Errorf("compactBytes() of %q = % x (err: %v), want an error", input, got, d.err)
		}
	}
}

func TestDecoderCompactArrayLength(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{input: "01", want: 0},
		{input: "02", want: 1},
		{input: "7f", want: 126},
		{input: "8001", want: 127},
	}

	for _, tt := range tests {
		d := newDecoder(mustDecodeHex(t, tt.input))

		if got := d.compactArrayLength(); got != tt.want || d.err != nil {
			t.Errorf("compactArrayLength() of %s = %d (err: %v), want %d", tt.input, got, d.err, tt.want)
		}
	}

	errorTests := []struct {
		name  string
		input string
	}{
		{name: "empty input", input: ""},
		{name: "null array", input: "00"},
	}

	for _, tt := range errorTests {
		t.Run(tt.name, func(t *testing.T) {
			d := newDecoder(mustDecodeHex(t, tt.input))

			if got := d.compactArrayLength(); got != 0 {
				t.Errorf("compactArrayLength() = %d, want 0 on error", got)
			}
			if d.err == nil {
				t.Error("expected an error, got nil")
			}
		})
	}
}

func TestEncoderUvarint(t *testing.T) {
	tests := []struct {
		value uint32
		want  string
	}{
		{value: 0, want: "00"},
		{value: 1, want: "01"},
		{value: 127, want: "7f"},
		{value: 128, want: "8001"},
		{value: 300, want: "ac02"},
		{value: 4294967295, want: "ffffffff0f"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			var e encoder
			e.uvarint(tt.value)

			if want := mustDecodeHex(t, tt.want); !bytes.Equal(e.buf, want) {
				t.Errorf("uvarint(%d) = % x, want % x", tt.value, e.buf, want)
			}

			// The decoder must read back what the encoder wrote.
			if got := newDecoder(e.buf).uvarint(); got != tt.value {
				t.Errorf("decoded %d, want %d", got, tt.value)
			}
		})
	}
}

func TestEncoderCompactArrayLength(t *testing.T) {
	tests := []struct {
		elements int
		want     string
	}{
		{elements: 0, want: "01"},
		{elements: 1, want: "02"},
		{elements: 126, want: "7f"},
		{elements: 127, want: "8001"},
	}

	for _, tt := range tests {
		var e encoder
		e.compactArrayLength(tt.elements)

		if want := mustDecodeHex(t, tt.want); !bytes.Equal(e.buf, want) {
			t.Errorf("compactArrayLength(%d) = % x, want % x", tt.elements, e.buf, want)
		}
	}
}

func TestEncoderCompactBytes(t *testing.T) {
	tests := []struct {
		value []byte
		want  string
	}{
		{value: nil, want: "01"},
		{value: []byte{}, want: "01"},
		{value: []byte{0xaa, 0xbb, 0xcc}, want: "04 aabbcc"},
		{value: bytes.Repeat([]byte{0xee}, 200), want: "c901" + strings.Repeat("ee", 200)},
	}

	for _, tt := range tests {
		var e encoder
		e.compactBytes(tt.value)

		if want := mustDecodeHex(t, tt.want); !bytes.Equal(e.buf, want) {
			t.Errorf("compactBytes(% x) = % x, want % x", tt.value, e.buf, want)
		}
	}
}

func TestEncoderEmptyTaggedFields(t *testing.T) {
	var e encoder
	e.emptyTaggedFields()

	if !bytes.Equal(e.buf, []byte{0x00}) {
		t.Errorf("emptyTaggedFields() = % x, want 00", e.buf)
	}
}
