package main

import (
	"errors"
	"fmt"
)

// recordBatchMagic is the version of the record batch format that the broker
// understands. It is the only one that Kafka has written since version 0.11.
const recordBatchMagic = 2

// Bits of the attributes of a record batch.
const (
	// batchCompressionMask selects the compression codec of the records.
	batchCompressionMask = 0x07
	// batchControlFlag marks a control batch: its records are for the
	// brokers and clients themselves, not for applications.
	batchControlFlag = 0x20
)

// recordBatch is a batch of records, the unit in which Kafka stores records in
// the segment files of a log and sends them over the network.
type recordBatch struct {
	baseOffset           int64 // the offset of the first record of the batch
	partitionLeaderEpoch int32
	crc                  uint32
	attributes           int16
	lastOffsetDelta      int32 // the offset of the last record, relative to baseOffset
	baseTimestamp        int64
	maxTimestamp         int64
	producerID           int64
	producerEpoch        int16
	baseSequence         int32
	recordCount          int32

	// recordData holds the records of the batch as they are stored, which may
	// be compressed: see records.
	recordData []byte

	// raw is the batch as it is stored: all of the above, encoded.
	raw []byte
}

// isControl reports whether the batch is a control batch.
func (b recordBatch) isControl() bool {
	return b.attributes&batchControlFlag != 0
}

// lastOffset returns the offset of the last record of the batch.
func (b recordBatch) lastOffset() int64 {
	return b.baseOffset + int64(b.lastOffsetDelta)
}

// records decodes the records of the batch.
func (b recordBatch) records() ([]record, error) {
	if b.attributes&batchCompressionMask != 0 {
		return nil, errors.New("compressed record batches are not supported")
	}

	var records []record

	d := newDecoder(b.recordData)
	for i := int32(0); i < b.recordCount && d.err == nil; i++ {
		records = append(records, decodeRecord(d))
	}
	if d.err != nil {
		return nil, d.err
	}
	if len(d.buf) != 0 {
		return nil, fmt.Errorf("%d bytes left after the %d records of the batch", len(d.buf), b.recordCount)
	}

	return records, nil
}

// validate checks that the batch is one that a producer may add to a log.
func (b recordBatch) validate() error {
	if b.isControl() {
		return errors.New("control batches can't be produced")
	}

	// The offsets of the records of a batch are consecutive, so the delta of
	// the last one has to match the number of records.
	if count := int64(b.lastOffsetDelta) + 1; count <= 0 || count != int64(b.recordCount) {
		return fmt.Errorf("last offset delta (%d) doesn't match record count (%d)", b.lastOffsetDelta, b.recordCount)
	}

	// The records of a compressed batch can't be looked at.
	if b.attributes&batchCompressionMask == 0 {
		if _, err := b.records(); err != nil {
			return fmt.Errorf("invalid records: %w", err)
		}
	}

	return nil
}

// record is a single record of a record batch.
type record struct {
	attributes     int8
	timestampDelta int64
	offsetDelta    int32
	key            []byte // nil when the record has no key
	value          []byte // nil when the record has no value
	headers        []recordHeader
}

// recordHeader is an application-defined key/value pair attached to a record.
type recordHeader struct {
	key   string
	value []byte // nil when the header has no value
}

// decodeRecordBatches decodes the contents of a log, which is a sequence of
// record batches. The records of the batches are left as they are: they can be
// decoded with recordBatch.records.
func decodeRecordBatches(log []byte) ([]recordBatch, error) {
	var batches []recordBatch

	d := newDecoder(log)
	for len(d.buf) > 0 {
		batch := decodeRecordBatch(d)
		if d.err != nil {
			return nil, fmt.Errorf("decoding record batch %d: %w", len(batches), d.err)
		}
		batches = append(batches, batch)
	}

	return batches, nil
}

// decodeRecordBatch decodes a record batch, except for its records. The CRC of
// the batch is not verified.
func decodeRecordBatch(d *decoder) recordBatch {
	encoded := d.buf

	var batch recordBatch
	batch.baseOffset = d.int64()

	// The batch length counts the bytes of the batch that follow it.
	b := d.sub(int(d.int32()))

	batch.partitionLeaderEpoch = b.int32()
	if magic := b.int8(); b.err == nil && magic != recordBatchMagic {
		b.err = fmt.Errorf("unsupported record batch magic: %d", magic)
	}
	batch.crc = uint32(b.int32())
	batch.attributes = b.int16()
	batch.lastOffsetDelta = b.int32()
	batch.baseTimestamp = b.int64()
	batch.maxTimestamp = b.int64()
	batch.producerID = b.int64()
	batch.producerEpoch = b.int16()
	batch.baseSequence = b.int32()
	batch.recordCount = b.int32()
	batch.recordData = b.buf

	d.fail(b.err)
	batch.raw = encoded[:len(encoded)-len(d.buf)]
	return batch
}

// decodeRecord decodes a record of a record batch.
func decodeRecord(d *decoder) record {
	// The record starts with the number of bytes that follow.
	r := d.sub(int(d.varint()))

	rec := record{
		attributes:     r.int8(),
		timestampDelta: r.varlong(),
		offsetDelta:    r.varint(),
		key:            decodeRecordBytes(r),
		value:          decodeRecordBytes(r),
	}

	headerCount := r.varint()
	for i := int32(0); i < headerCount && r.err == nil; i++ {
		keyLength := r.varint()
		rec.headers = append(rec.headers, recordHeader{
			key:   string(r.bytes(int(keyLength))),
			value: decodeRecordBytes(r),
		})
	}

	d.fail(r.err)
	return rec
}

// decodeRecordBytes decodes a byte sequence of a record: its length as a
// VARINT, where -1 stands for null (returned as nil), followed by the bytes.
func decodeRecordBytes(d *decoder) []byte {
	n := d.varint()
	if d.err != nil || n == -1 {
		return nil
	}
	return d.bytes(int(n))
}
