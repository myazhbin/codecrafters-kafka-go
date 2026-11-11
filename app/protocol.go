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

// uuid is a Kafka UUID: 16 bytes, shown as 8-4-4-4-12 hex digits.
type uuid [16]byte

func (u uuid) String() string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
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

// sub consumes the next n bytes of the message and returns a decoder that
// reads from them. It is used for the parts of a message that announce their
// own size. A failure of the returned decoder has to be handed back with fail.
func (d *decoder) sub(n int) *decoder {
	return &decoder{buf: d.bytes(n), err: d.err}
}

// fail records err as the failure of the decoder, unless the decoder has
// already failed or err is nil.
func (d *decoder) fail(err error) {
	if d.err == nil {
		d.err = err
	}
}

// int8 decodes an INT8.
func (d *decoder) int8() int8 {
	b := d.bytes(1)
	if d.err != nil {
		return 0
	}
	return int8(b[0])
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

// int64 decodes an INT64.
func (d *decoder) int64() int64 {
	b := d.bytes(8)
	if d.err != nil {
		return 0
	}
	return int64(binary.BigEndian.Uint64(b))
}

// uuid decodes a UUID.
func (d *decoder) uuid() uuid {
	b := d.bytes(16)
	if d.err != nil {
		return uuid{}
	}
	return uuid(b)
}

// varlong decodes a VARLONG: a 64-bit integer in zigzag encoding, stored in a
// variable number of bytes.
func (d *decoder) varlong() int64 {
	if d.err != nil {
		return 0
	}

	v, n := binary.Varint(d.buf)
	switch {
	case n == 0:
		d.err = errTruncated
		return 0
	case n < 0:
		d.err = errors.New("varlong overflows 64 bits")
		return 0
	}

	d.buf = d.buf[n:]
	return v
}

// varint decodes a VARINT: a 32-bit integer in zigzag encoding, stored in a
// variable number of bytes.
func (d *decoder) varint() int32 {
	v := d.varlong()
	if d.err != nil {
		return 0
	}
	if v < math.MinInt32 || v > math.MaxInt32 {
		d.err = errors.New("varint overflows 32 bits")
		return 0
	}
	return int32(v)
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

// compactString decodes a COMPACT_STRING: the length of the string + 1 as an
// UNSIGNED_VARINT, followed by the bytes of the string. A length of 0 stands
// for null, which a COMPACT_STRING must not be.
func (d *decoder) compactString() string {
	n := d.uvarint()
	if d.err != nil {
		return ""
	}
	if n == 0 {
		d.err = errors.New("non-nullable string is null")
		return ""
	}

	b := d.bytes(int(n - 1))
	if d.err != nil {
		return ""
	}
	return string(b)
}

// compactArrayLength decodes the length of a COMPACT_ARRAY and returns the
// number of elements that follow. The length is stored as that number + 1,
// where 0 stands for a null array, which is reported as an error: none of the
// arrays that the broker decodes may be null.
func (d *decoder) compactArrayLength() int {
	n := d.uvarint()
	if d.err != nil {
		return 0
	}
	if n == 0 {
		d.err = errors.New("non-nullable array is null")
		return 0
	}
	return int(n - 1)
}

// compactInt32Array decodes a COMPACT_ARRAY of INT32 values.
func (d *decoder) compactInt32Array() []int32 {
	n := d.compactArrayLength()

	var values []int32
	for i := 0; i < n && d.err == nil; i++ {
		values = append(values, d.int32())
	}
	if d.err != nil {
		return nil
	}
	return values
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

// Versions of the response header. Which one a response uses depends on the
// API and on the API version of the request it answers.
const (
	// responseHeaderV0 only holds the correlation ID.
	responseHeaderV0 = 0
	// responseHeaderV1 holds the correlation ID followed by a TAG_BUFFER.
	responseHeaderV1 = 1
)

// encodeResponse builds a Kafka response message: a 4-byte message_size
// followed by a response header of the given version and the response body.
func encodeResponse(headerVersion int, correlationID int32, body []byte) []byte {
	var header encoder
	header.int32(correlationID)
	if headerVersion >= responseHeaderV1 {
		header.emptyTaggedFields()
	}

	var e encoder
	e.int32(int32(len(header.buf) + len(body))) // message_size
	e.raw(header.buf)
	e.raw(body)

	return e.buf
}

// encoder builds a message out of Kafka protocol primitive types. All integers
// are big-endian.
type encoder struct {
	buf []byte
}

// raw appends bytes that are already encoded.
func (e *encoder) raw(b []byte) {
	e.buf = append(e.buf, b...)
}

// int8 encodes an INT8.
func (e *encoder) int8(v int8) {
	e.buf = append(e.buf, byte(v))
}

// boolean encodes a BOOLEAN.
func (e *encoder) boolean(v bool) {
	if v {
		e.buf = append(e.buf, 1)
	} else {
		e.buf = append(e.buf, 0)
	}
}

// int16 encodes an INT16.
func (e *encoder) int16(v int16) {
	e.buf = binary.BigEndian.AppendUint16(e.buf, uint16(v))
}

// int32 encodes an INT32.
func (e *encoder) int32(v int32) {
	e.buf = binary.BigEndian.AppendUint32(e.buf, uint32(v))
}

// uvarint encodes an UNSIGNED_VARINT.
func (e *encoder) uvarint(v uint32) {
	e.buf = binary.AppendUvarint(e.buf, uint64(v))
}

// uuid encodes a UUID.
func (e *encoder) uuid(v uuid) {
	e.buf = append(e.buf, v[:]...)
}

// compactString encodes a COMPACT_STRING: the length of the string + 1 as an
// UNSIGNED_VARINT, followed by the bytes of the string.
func (e *encoder) compactString(s string) {
	e.uvarint(uint32(len(s) + 1))
	e.buf = append(e.buf, s...)
}

// compactArrayLength encodes the length of a COMPACT_ARRAY of n elements. It
// is stored as n + 1, because 0 stands for a null array.
func (e *encoder) compactArrayLength(n int) {
	e.uvarint(uint32(n + 1))
}

// compactInt32Array encodes a COMPACT_ARRAY of INT32 values.
func (e *encoder) compactInt32Array(values []int32) {
	e.compactArrayLength(len(values))
	for _, v := range values {
		e.int32(v)
	}
}

// emptyTaggedFields encodes a TAG_BUFFER that holds no tagged fields.
func (e *encoder) emptyTaggedFields() {
	e.uvarint(0)
}
