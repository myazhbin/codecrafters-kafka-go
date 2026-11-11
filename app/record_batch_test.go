package main

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func TestDecodeRecordBatches(t *testing.T) {
	t.Run("batches of the CodeCrafters cluster metadata log", func(t *testing.T) {
		batches, err := decodeRecordBatches(mustDecodeHex(t, featureLevelBatchHex+sazTopicBatchHex))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(batches) != 2 {
			t.Fatalf("decoded %d batches, want 2", len(batches))
		}

		featureLevel := batches[0]
		wantHeader := recordBatch{
			baseOffset:           1,
			partitionLeaderEpoch: 1,
			crc:                  0xb069457c,
			attributes:           0,
			lastOffsetDelta:      0,
			baseTimestamp:        1726045943832,
			maxTimestamp:         1726045943832,
			producerID:           -1,
			producerEpoch:        -1,
			baseSequence:         -1,
		}
		gotHeader := featureLevel
		gotHeader.records = nil
		if !reflect.DeepEqual(gotHeader, wantHeader) {
			t.Errorf("first batch = %+v, want %+v", gotHeader, wantHeader)
		}
		if featureLevel.isControl() {
			t.Error("first batch is reported as a control batch")
		}
		if len(featureLevel.records) != 1 {
			t.Fatalf("first batch has %d records, want 1", len(featureLevel.records))
		}
		wantRecord := record{
			attributes:     0,
			timestampDelta: 0,
			offsetDelta:    0,
			key:            nil,
			value:          mustDecodeHex(t, "01 0c 00 11 6d657461646174612e76657273696f6e 0014 00"),
			headers:        nil,
		}
		if !reflect.DeepEqual(featureLevel.records[0], wantRecord) {
			t.Errorf("record = %+v, want %+v", featureLevel.records[0], wantRecord)
		}

		topic := batches[1]
		if topic.baseOffset != 2 || topic.lastOffsetDelta != 1 || topic.crc != 0xc64fa9c1 {
			t.Errorf("second batch: baseOffset = %d, lastOffsetDelta = %d, crc = %#x; want 2, 1, 0xc64fa9c1",
				topic.baseOffset, topic.lastOffsetDelta, topic.crc)
		}
		if len(topic.records) != 2 {
			t.Fatalf("second batch has %d records, want 2", len(topic.records))
		}
		if got, want := topic.records[0].value, mustDecodeHex(t, "01 02 00 04 73617a 00000000000040008000000000000091 00"); !bytes.Equal(got, want) {
			t.Errorf("topic record value = % x, want % x", got, want)
		}
		if got := topic.records[1].offsetDelta; got != 1 {
			t.Errorf("offset delta of the second record = %d, want 1", got)
		}
		// The value of the second record is 65 bytes long, which takes two
		// bytes to encode as a VARINT.
		if got := len(topic.records[1].value); got != 65 {
			t.Errorf("partition record value is %d bytes long, want 65", got)
		}
	})

	t.Run("empty log", func(t *testing.T) {
		batches, err := decodeRecordBatches(nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(batches) != 0 {
			t.Errorf("decoded %d batches, want none", len(batches))
		}
	})

	t.Run("keys, null values and headers", func(t *testing.T) {
		batch := "000000000000000a" + // base offset: 10
			"0000004c" + // batch length: 76
			"00000000" + // partition leader epoch
			"02" + // magic byte
			"00000000" + // CRC (not verified)
			"0000" + // attributes
			"00000001" + // last offset delta
			"0000000000000064" + // base timestamp: 100
			"00000000000000c8" + // max timestamp: 200
			"0000000000000005" + // producer ID: 5
			"0001" + // producer epoch: 1
			"00000007" + // base sequence: 7
			"00000002" + // records count
			// Record with a key, a value and two headers.
			"24" + // record length: 18
			"00" + // attributes
			"00" + // timestamp delta: 0
			"00" + // offset delta: 0
			"02" + "6b" + // key: "k"
			"04" + "7631" + // value: "v1"
			"04" + // headers count: 2
			"04" + "6831" + "02" + "78" + // header "h1": "x"
			"04" + "6832" + "01" + // header "h2": null
			// Record without a key, with an empty value, later in time.
			"0e" + // record length: 7
			"00" + // attributes
			"c801" + // timestamp delta: 100
			"02" + // offset delta: 1
			"01" + // key: null
			"00" + // value: empty
			"00" // headers count: 0

		batches, err := decodeRecordBatches(mustDecodeHex(t, batch))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(batches) != 1 {
			t.Fatalf("decoded %d batches, want 1", len(batches))
		}

		got := batches[0]
		if got.baseOffset != 10 || got.producerID != 5 || got.producerEpoch != 1 || got.baseSequence != 7 ||
			got.baseTimestamp != 100 || got.maxTimestamp != 200 {
			t.Errorf("unexpected batch header: %+v", got)
		}

		want := []record{
			{
				key:   []byte("k"),
				value: []byte("v1"),
				headers: []recordHeader{
					{key: "h1", value: []byte("x")},
					{key: "h2", value: nil},
				},
			},
			{
				timestampDelta: 100,
				offsetDelta:    1,
				key:            nil,
				value:          []byte{},
			},
		}
		if !reflect.DeepEqual(got.records, want) {
			t.Errorf("records = %+v, want %+v", got.records, want)
		}
	})

	t.Run("control batch", func(t *testing.T) {
		batch := encodeRecordBatch(testBatch{attributes: 0x20, values: [][]byte{{0x00}}})

		batches, err := decodeRecordBatches(batch)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(batches) != 1 || !batches[0].isControl() {
			t.Errorf("expected a single control batch, got %+v", batches)
		}
	})

	validBatch := mustDecodeHex(t, featureLevelBatchHex)

	// withByte returns a copy of the valid batch in which one byte is changed.
	withByte := func(offset int, value byte) []byte {
		b := bytes.Clone(validBatch)
		b[offset] = value
		return b
	}

	errorTests := []struct {
		name    string
		input   []byte
		wantErr string
	}{
		{name: "truncated base offset", input: validBatch[:5]},
		{name: "truncated batch length", input: validBatch[:10]},
		{name: "batch shorter than its length", input: validBatch[:len(validBatch)-1]},
		{name: "trailing bytes after the last batch", input: append(bytes.Clone(validBatch), 0x00)},
		{name: "negative batch length", input: withByte(8, 0xff)},
		{name: "batch length too small for the header", input: append(bytes.Clone(validBatch[:11]), 0x04, 0x00, 0x00, 0x00, 0x01)},
		{name: "unsupported magic", input: withByte(16, 1), wantErr: "magic"},
		{name: "compressed batch", input: withByte(22, 0x01), wantErr: "compressed"},
		{name: "more records than the batch holds", input: withByte(60, 2)},
		{name: "record longer than the batch", input: withByte(61, 0x7e)},
		{name: "value longer than the record", input: withByte(66, 0x7e)},
	}

	for _, tt := range errorTests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeRecordBatches(tt.input)
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %q, want it to mention %q", err, tt.wantErr)
			}
		})
	}
}

// The batches built by the test fixtures must be byte for byte what Kafka
// writes: encodeRecordBatch has to reproduce the batches of the CodeCrafters
// cluster metadata log, including their CRC.
func TestRecordBatchFixtures(t *testing.T) {
	t.Run("feature level batch", func(t *testing.T) {
		got := encodeRecordBatch(testBatch{
			baseOffset:           1,
			partitionLeaderEpoch: 1,
			timestamp:            1726045943832,
			values:               [][]byte{featureLevelRecord("metadata.version", 20)},
		})
		if want := mustDecodeHex(t, featureLevelBatchHex); !bytes.Equal(got, want) {
			t.Errorf("batch = % x, want % x", got, want)
		}
	})

	t.Run("topic batch", func(t *testing.T) {
		got := encodeRecordBatch(testBatch{
			baseOffset:           2,
			partitionLeaderEpoch: 1,
			timestamp:            1726045957397,
			values: [][]byte{
				topicRecord("saz", sazTopicID),
				partitionRecord(testPartition{index: 1, topicID: sazTopicID, replicas: []int32{1}, isr: []int32{1}, leader: 1}),
			},
		})
		if want := mustDecodeHex(t, sazTopicBatchHex); !bytes.Equal(got, want) {
			t.Errorf("batch = % x, want % x", got, want)
		}
	})
}
