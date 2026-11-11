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
// the segment files of a log.
type recordBatch struct {
	baseOffset           int64
	partitionLeaderEpoch int32
	crc                  uint32
	attributes           int16
	lastOffsetDelta      int32
	baseTimestamp        int64
	maxTimestamp         int64
	producerID           int64
	producerEpoch        int16
	baseSequence         int32
	records              []record
}

// isControl reports whether the batch is a control batch.
func (b recordBatch) isControl() bool {
	return b.attributes&batchControlFlag != 0
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

// decodeRecordBatches decodes the contents of a log segment file, which is a
// sequence of record batches.
func decodeRecordBatches(data []byte) ([]recordBatch, error) {
	var batches []recordBatch

	d := newDecoder(data)
	for len(d.buf) > 0 {
		batch := decodeRecordBatch(d)
		if d.err != nil {
			return nil, fmt.Errorf("decoding record batch %d: %w", len(batches), d.err)
		}
		batches = append(batches, batch)
	}

	return batches, nil
}

// decodeRecordBatch decodes a record batch. Its CRC is not verified.
func decodeRecordBatch(d *decoder) recordBatch {
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

	recordCount := b.int32()
	if b.err == nil && batch.attributes&batchCompressionMask != 0 {
		b.err = errors.New("compressed record batches are not supported")
	}
	for i := int32(0); i < recordCount && b.err == nil; i++ {
		batch.records = append(batch.records, decodeRecord(b))
	}

	d.fail(b.err)
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
