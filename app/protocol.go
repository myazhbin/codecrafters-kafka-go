package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

// maxRequestSize is the largest message_size the broker accepts for a request.
// It matches the default of Kafka's socket.request.max.bytes setting.
const maxRequestSize = 100 * 1024 * 1024

// errTruncated is reported when a message ends before the value being decoded.
var errTruncated = errors.New("message is truncated")

// readMessage reads one Kafka message from r: a 4-byte message_size followed
// by that many bytes of header and body. It returns the header and body.
//
// io.EOF is returned only when r ends cleanly before a new message starts.
func readMessage(r io.Reader) ([]byte, error) {
	sizeBytes := make([]byte, 4)
	if _, err := io.ReadFull(r, sizeBytes); err != nil {
		return nil, err
	}

	size := int32(binary.BigEndian.Uint32(sizeBytes))
	if size < 0 || size > maxRequestSize {
		return nil, fmt.Errorf("invalid message_size: %d", size)
	}

	message := make([]byte, size)
	if _, err := io.ReadFull(r, message); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}

	return message, nil
}

// requestHeader is a Kafka request header v2.
type requestHeader struct {
	apiKey        int16
	apiVersion    int16
	correlationID int32
	clientID      *string // nil when the client sent a null client_id
}

// decodeRequestHeader decodes a request header v2 from d, leaving d positioned
// at the start of the request body.
func decodeRequestHeader(d *decoder) (requestHeader, error) {
	header := requestHeader{
		apiKey:        d.int16(),
		apiVersion:    d.int16(),
		correlationID: d.int32(),
		clientID:      d.nullableString(),
	}
	d.taggedFields()

	return header, d.err
}

// decoder reads Kafka protocol primitive types from a message. All integers
// are big-endian.
//
// The first failure is kept in err and turns every later read into a no-op
// that returns a zero value, so a whole structure can be decoded before err is
// checked once.
type decoder struct {
	buf []byte
	err error
}

func newDecoder(buf []byte) *decoder {
	return &decoder{buf: buf}
}

// bytes consumes the next n bytes of the message.
func (d *decoder) bytes(n int) []byte {
	if d.err != nil {
		return nil
	}
	if n < 0 || n > len(d.buf) {
		d.err = errTruncated
		return nil
	}

	b := d.buf[:n]
	d.buf = d.buf[n:]
	return b
}

// int16 decodes an INT16.
func (d *decoder) int16() int16 {
	b := d.bytes(2)
	if d.err != nil {
		return 0
	}
	return int16(binary.BigEndian.Uint16(b))
}

// int32 decodes an INT32.
func (d *decoder) int32() int32 {
	b := d.bytes(4)
	if d.err != nil {
		return 0
	}
	return int32(binary.BigEndian.Uint32(b))
}

// uvarint decodes an UNSIGNED_VARINT.
func (d *decoder) uvarint() uint32 {
	if d.err != nil {
		return 0
	}

	v, n := binary.Uvarint(d.buf)
	switch {
	case n == 0:
		d.err = errTruncated
		return 0
	case n < 0 || v > math.MaxUint32:
		d.err = errors.New("unsigned varint overflows 32 bits")
		return 0
	}

	d.buf = d.buf[n:]
	return uint32(v)
}

// nullableString decodes a NULLABLE_STRING: an INT16 length followed by that
// many bytes, where a length of -1 stands for null (returned as nil).
func (d *decoder) nullableString() *string {
	n := d.int16()
	if d.err != nil || n == -1 {
		return nil
	}
	if n < 0 {
		d.err = fmt.Errorf("invalid string length: %d", n)
		return nil
	}

	b := d.bytes(int(n))
	if d.err != nil {
		return nil
	}
	s := string(b)
	return &s
}

// taggedFields skips over a TAG_BUFFER: the number of tagged fields, then for
// each of them its tag, its size and its data.
func (d *decoder) taggedFields() {
	count := d.uvarint()
	for i := uint32(0); i < count && d.err == nil; i++ {
		d.uvarint() // tag
		size := d.uvarint()
		d.bytes(int(size))
	}
}
