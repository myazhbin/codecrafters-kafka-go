package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

// newProduceRequest builds a Produce (v11) request with the given records,
// like the one the CodeCrafters tester sends. It asks for the acknowledgement
// of the partition leader.
func newProduceRequest(correlationID int32, topics ...produceTopic) []byte {
	return newProduceRequestWithAcks(correlationID, 1, topics...)
}

// newProduceRequestWithAcks builds a Produce (v11) request that asks for the
// given number of acknowledgements.
func newProduceRequestWithAcks(correlationID int32, acks int16, topics ...produceTopic) []byte {
	var body []byte
	body = append(body, 0) // transactional_id: null
	body = binary.BigEndian.AppendUint16(body, uint16(acks))
	body = binary.BigEndian.AppendUint32(body, 5000) // timeout_ms

	body = binary.AppendUvarint(body, uint64(len(topics)+1)) // topic_data
	for _, topic := range topics {
		body = appendCompactString(body, topic.name)

		body = binary.AppendUvarint(body, uint64(len(topic.partitions)+1)) // partition_data
		for _, partition := range topic.partitions {
			body = binary.BigEndian.AppendUint32(body, uint32(partition.index))
			body = binary.AppendUvarint(body, uint64(len(partition.records)+1)) // records
			body = append(body, partition.records...)
			body = append(body, 0) // tag buffer
		}
		body = append(body, 0) // tag buffer
	}
	body = append(body, 0) // tag buffer

	return encodeRequest(0, 11, correlationID, body)
}

// produceResponse is a decoded Produce response body (v11).
type produceResponse struct {
	topics         []producedTopic
	throttleTimeMs int32
}

type producedTopic struct {
	name       string
	partitions []producedPartition
}

type producedPartition struct {
	index           int32
	errorCode       int16
	baseOffset      int64
	logAppendTimeMs int64
	logStartOffset  int64
	recordErrors    int     // number of entries
	errorMessage    *string // nil for a null message
}

// decodeProduceResponse decodes a Produce response body (v11) and fails the
// test if the body is malformed or followed by extra bytes.
func decodeProduceResponse(t *testing.T, body []byte) produceResponse {
	t.Helper()

	d := newDecoder(body)

	var response produceResponse
	for i, n := 0, d.compactArrayLength(); i < n && d.err == nil; i++ {
		topic := producedTopic{name: d.compactString()}

		for j, m := 0, d.compactArrayLength(); j < m && d.err == nil; j++ {
			partition := producedPartition{
				index:           d.int32(),
				errorCode:       d.int16(),
				baseOffset:      d.int64(),
				logAppendTimeMs: d.int64(),
				logStartOffset:  d.int64(),
			}

			partition.recordErrors = d.compactArrayLength()
			for k := 0; k < partition.recordErrors && d.err == nil; k++ {
				d.int32()                 // batch_index
				d.compactNullableString() // batch_index_error_message
				d.taggedFields()
			}

			partition.errorMessage = d.compactNullableString()
			d.taggedFields()

			topic.partitions = append(topic.partitions, partition)
		}
		d.taggedFields()

		response.topics = append(response.topics, topic)
	}

	response.throttleTimeMs = d.int32()
	d.taggedFields()

	if d.err != nil {
		t.Fatalf("failed to decode Produce response body % x: %v", body, d.err)
	}
	if len(d.buf) != 0 {
		t.Fatalf("%d extra bytes after the Produce response body: % x", len(d.buf), d.buf)
	}

	return response
}

// expectProduceResponse reads the response to a Produce request from conn and
// returns it decoded, after checking its message_size, correlation ID and
// header.
func expectProduceResponse(t *testing.T, conn net.Conn, correlationID int32) produceResponse {
	t.Helper()

	response := readResponse(t, conn)
	if want := int32(4 + len(response.body)); response.messageSize != want {
		t.Errorf("message_size = %d, want %d", response.messageSize, want)
	}
	if response.correlationID != correlationID {
		t.Errorf("correlation_id = %d, want %d", response.correlationID, correlationID)
	}

	body := decodeProduceResponse(t, response.bodyAfterHeaderV1(t))
	if body.throttleTimeMs != 0 {
		t.Errorf("throttle_time_ms = %d, want 0", body.throttleTimeMs)
	}
	return body
}

// produce sends a Produce request with the given records over a new
// connection to the broker at addr, and returns the decoded response.
func produce(t *testing.T, addr string, topics ...produceTopic) produceResponse {
	t.Helper()

	const correlationID = 2077508643

	conn := dial(t, addr)
	send(t, conn, newProduceRequest(correlationID, topics...))

	body := expectProduceResponse(t, conn, correlationID)
	expectNoMoreData(t, conn)

	return body
}

// expectProduceError checks that partition is how the broker answers when it
// couldn't add records to a partition.
func expectProduceError(t *testing.T, partition producedPartition, index int32, errorCode int16) {
	t.Helper()

	want := producedPartition{
		index:           index,
		errorCode:       errorCode,
		baseOffset:      -1,
		logAppendTimeMs: -1,
		logStartOffset:  -1,
	}
	if !reflect.DeepEqual(partition, want) {
		t.Errorf("partition response = %+v, want %+v", partition, want)
	}
}

func TestDecodeProduceRequest(t *testing.T) {
	// The fields that come before the topics of a Produce request.
	const preamble = "00" + // transactional_id: null
		"0001" + // acks: 1
		"00001388" // timeout_ms: 5000

	tests := []struct {
		name  string
		input string
		want  produceRequest
	}{
		{
			name: "no topics",
			input: preamble +
				"01" + // topic_data: empty
				"00", // tag buffer
			want: produceRequest{acks: 1},
		},
		{
			name: "one topic with one partition",
			input: preamble +
				"02" + // topic_data: 1 element
				"04 666f6f" + // name: "foo"
				"02" + // partition_data: 1 element
				"00000000" + // index: 0
				"05 deadbeef" + // records: 4 bytes
				"00" + // tag buffer
				"00" + // tag buffer
				"00", // tag buffer
			want: produceRequest{acks: 1, topics: []produceTopic{
				{name: "foo", partitions: []producePartition{{index: 0, records: []byte{0xde, 0xad, 0xbe, 0xef}}}},
			}},
		},
		{
			name: "several topics and partitions",
			input: preamble +
				"03" + // topic_data: 2 elements
				"04 666f6f" + // name: "foo"
				"03" + // partition_data: 2 elements
				"00000000 03 aabb 00" +
				"00000005 02 cc 00" +
				"00" +
				"04 626172" + // name: "bar"
				"02" + // partition_data: 1 element
				"00000001 04 ddeeff 00" +
				"00" +
				"00",
			want: produceRequest{acks: 1, topics: []produceTopic{
				{name: "foo", partitions: []producePartition{
					{index: 0, records: []byte{0xaa, 0xbb}},
					{index: 5, records: []byte{0xcc}},
				}},
				{name: "bar", partitions: []producePartition{
					{index: 1, records: []byte{0xdd, 0xee, 0xff}},
				}},
			}},
		},
		{
			name:  "acks, transactional ID and tagged fields",
			input: "05 74786e31" + "ffff" + "00000000" + "02 04 666f6f 02 00000000 02 aa 01 00 01 bb 00 01 07 02 cccc",
			want: produceRequest{acks: -1, topics: []produceTopic{
				{name: "foo", partitions: []producePartition{{index: 0, records: []byte{0xaa}}}},
			}},
		},
		{
			name:  "no acknowledgement",
			input: "00" + "0000" + "00001388" + "01 00",
			want:  produceRequest{acks: 0},
		},
		{
			name:  "null and empty records",
			input: preamble + "02 04 666f6f 03 00000000 00 00 00000001 01 00 00 00",
			want: produceRequest{acks: 1, topics: []produceTopic{
				{name: "foo", partitions: []producePartition{
					{index: 0, records: nil},
					{index: 1, records: []byte{}},
				}},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newDecoder(mustDecodeHex(t, tt.input))

			got, err := decodeProduceRequest(d)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("request = %+v, want %+v", got, tt.want)
			}
			if len(d.buf) != 0 {
				t.Errorf("expected the whole body to be consumed, %d bytes left", len(d.buf))
			}
		})
	}

	// The request builder of the tests must produce what the decoder expects.
	t.Run("request built by newProduceRequest", func(t *testing.T) {
		want := produceRequest{acks: 1, topics: []produceTopic{
			{name: "foo", partitions: []producePartition{
				{index: 0, records: []byte{0x01, 0x02}},
				{index: 3, records: bytes.Repeat([]byte{0xab}, 300)},
			}},
			{name: "bar"},
		}}

		d := newDecoder(newProduceRequest(1, want.topics...)[4:])
		if _, err := decodeRequestHeader(d); err != nil {
			t.Fatalf("failed to decode request header: %v", err)
		}

		got, err := decodeProduceRequest(d)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("request = %+v, want %+v", got, want)
		}
		if len(d.buf) != 0 {
			t.Errorf("expected the whole body to be consumed, %d bytes left", len(d.buf))
		}
	})

	errorTests := []struct {
		name  string
		input string
	}{
		{name: "empty body", input: ""},
		{name: "truncated transactional ID", input: "05 7478"},
		{name: "missing acks", input: "00"},
		{name: "truncated timeout", input: "00 0001 0000"},
		{name: "missing topics", input: preamble},
		{name: "null topics", input: preamble + "00 00"},
		{name: "null topic name", input: preamble + "02 00 01 00 00"},
		{name: "truncated topic name", input: preamble + "02 04 666f"},
		{name: "missing partitions", input: preamble + "02 04 666f6f"},
		{name: "truncated partition index", input: preamble + "02 04 666f6f 02 0000"},
		{name: "records shorter than their length", input: preamble + "02 04 666f6f 02 00000000 05 dead"},
		{name: "fewer partitions than announced", input: preamble + "02 04 666f6f 03 00000000 02 aa 00 00 00"},
		{name: "missing tag buffer", input: preamble + "01"},
	}

	for _, tt := range errorTests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := decodeProduceRequest(newDecoder(mustDecodeHex(t, tt.input))); err == nil {
				t.Error("expected an error, got nil")
			}
		})
	}
}

func TestEncodeProduceResponse(t *testing.T) {
	tests := []struct {
		name   string
		topics []produceTopicResponse
		want   string
	}{
		{
			name: "unknown topic or partition",
			topics: []produceTopicResponse{{
				name:       "foo",
				partitions: []producePartitionResponse{produceError(0, 3)},
			}},
			want: "02" + // responses: 1 element
				"04 666f6f" + // name: "foo"
				"02" + // partition_responses: 1 element
				"00000000" + // index: 0
				"0003" + // error_code: 3 (UNKNOWN_TOPIC_OR_PARTITION)
				"ffffffffffffffff" + // base_offset: -1
				"ffffffffffffffff" + // log_append_time_ms: -1
				"ffffffffffffffff" + // log_start_offset: -1
				"01" + // record_errors: empty
				"00" + // error_message: null
				"00" + // tag buffer
				"00" + // tag buffer
				"00000000" + // throttle_time_ms: 0
				"00", // tag buffer
		},
		{
			name: "records added to a partition",
			topics: []produceTopicResponse{{
				name:       "foo",
				partitions: []producePartitionResponse{{index: 2, baseOffset: 5, logStartOffset: 1}},
			}},
			want: "02" + // responses: 1 element
				"04 666f6f" + // name: "foo"
				"02" + // partition_responses: 1 element
				"00000002" + // index: 2
				"0000" + // error_code: 0
				"0000000000000005" + // base_offset: 5
				"ffffffffffffffff" + // log_append_time_ms: -1
				"0000000000000001" + // log_start_offset: 1
				"01" + // record_errors: empty
				"00" + // error_message: null
				"00" + // tag buffer
				"00" + // tag buffer
				"00000000" + // throttle_time_ms: 0
				"00", // tag buffer
		},
		{
			name: "several topics and partitions",
			topics: []produceTopicResponse{
				{name: "foo", partitions: []producePartitionResponse{produceError(0, 3), produceError(7, 3)}},
				{name: "ba", partitions: []producePartitionResponse{produceError(1, 3)}},
				{name: "empty"},
			},
			want: "04" +
				"04 666f6f" +
				"03" +
				"00000000 0003 ffffffffffffffff ffffffffffffffff ffffffffffffffff 01 00 00" +
				"00000007 0003 ffffffffffffffff ffffffffffffffff ffffffffffffffff 01 00 00" +
				"00" +
				"03 6261" +
				"02" +
				"00000001 0003 ffffffffffffffff ffffffffffffffff ffffffffffffffff 01 00 00" +
				"00" +
				"06 656d707479" +
				"01" +
				"00" +
				"00000000 00",
		},
		{
			name:   "no topics",
			topics: nil,
			want:   "01 00000000 00",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := encodeProduceResponse(tt.topics)
			if want := mustDecodeHex(t, tt.want); !bytes.Equal(got, want) {
				t.Errorf("body = % x, want % x", got, want)
			}

			// The body must also be a valid Produce response.
			decoded := decodeProduceResponse(t, got)
			if len(decoded.topics) != len(tt.topics) {
				t.Fatalf("decoded %d topics, want %d", len(decoded.topics), len(tt.topics))
			}
			for i, topic := range tt.topics {
				if decoded.topics[i].name != topic.name || len(decoded.topics[i].partitions) != len(topic.partitions) {
					t.Errorf("decoded topics[%d] = %+v, want %+v", i, decoded.topics[i], topic)
				}
			}
		})
	}
}

// Stage zf2: Produce requests for a partition or a topic that doesn't exist
// are answered with the error code UNKNOWN_TOPIC_OR_PARTITION.
func TestProduceToInvalidTopicOrPartition(t *testing.T) {
	logDir := t.TempDir()
	writeClusterMetadataLog(t, logDir, testTopic{name: "foo", id: testTopicID(1), partitions: []int32{0}})
	addr := startTestServerWithLogDir(t, logDir)

	records := encodeRecordBatch(testBatch{values: [][]byte{[]byte("Hello World!")}})

	// Like the CodeCrafters tester, the test sends two requests over the same
	// connection: the first one for a partition that doesn't exist in a
	// topic that does, the second one for a topic that doesn't exist.
	requests := []struct {
		name      string
		topic     string
		partition int32
	}{
		{name: "valid topic but invalid partition", topic: "foo", partition: 5},
		{name: "invalid topic", topic: "unknown-topic", partition: 0},
	}

	conn := dial(t, addr)
	for i, request := range requests {
		correlationID := int32(1000 + i)

		send(t, conn, newProduceRequest(correlationID, produceTopic{
			name:       request.topic,
			partitions: []producePartition{{index: request.partition, records: records}},
		}))

		// The message length and the correlation ID are valid, and the
		// throttle_time_ms field is 0 (checked by expectProduceResponse).
		body := expectProduceResponse(t, conn, correlationID)

		// The topics field has 1 element, whose name matches the topic name
		// in the request.
		if len(body.topics) != 1 {
			t.Fatalf("%s: topics has %d elements, want 1", request.name, len(body.topics))
		}
		topic := body.topics[0]
		if topic.name != request.topic {
			t.Errorf("%s: name = %q, want %q", request.name, topic.name, request.topic)
		}

		// The partitions field has 1 element: its error_code is 3
		// (UNKNOWN_TOPIC_OR_PARTITION), its index matches the partition in
		// the request, and its base_offset, log_append_time_ms and
		// log_start_offset are all -1.
		if len(topic.partitions) != 1 {
			t.Fatalf("%s: partitions has %d elements, want 1", request.name, len(topic.partitions))
		}
		expectProduceError(t, topic.partitions[0], request.partition, 3)
	}

	expectNoMoreData(t, conn)
}

// Stage gg1: a Produce request for a topic and a partition that exist is
// answered without an error.
func TestProduceToValidTopicAndPartition(t *testing.T) {
	logDir := t.TempDir()
	writeClusterMetadataLog(t, logDir,
		testTopic{name: "bar", id: testTopicID(1), partitions: []int32{0}},
		testTopic{name: "foo", id: testTopicID(2), partitions: []int32{0, 1}},
	)
	addr := startTestServerWithLogDir(t, logDir)

	records := encodeRecordBatch(testBatch{values: [][]byte{[]byte("Hello World!")}})

	for _, tt := range []struct {
		topic     string
		partition int32
	}{
		{topic: "foo", partition: 0},
		{topic: "foo", partition: 1},
		{topic: "bar", partition: 0},
	} {
		t.Run(fmt.Sprintf("%s-%d", tt.topic, tt.partition), func(t *testing.T) {
			body := produce(t, addr, produceTopic{
				name:       tt.topic,
				partitions: []producePartition{{index: tt.partition, records: records}},
			})

			// The topics field has 1 element, whose name matches the topic
			// name in the request.
			if len(body.topics) != 1 {
				t.Fatalf("topics has %d elements, want 1", len(body.topics))
			}
			topic := body.topics[0]
			if topic.name != tt.topic {
				t.Errorf("name = %q, want %q", topic.name, tt.topic)
			}

			// The partitions field has 1 element.
			if len(topic.partitions) != 1 {
				t.Fatalf("partitions has %d elements, want 1", len(topic.partitions))
			}
			want := producedPartition{
				// The index matches the partition in the request.
				index: tt.partition,
				// The error_code is 0 (NO_ERROR).
				errorCode: 0,
				// The base_offset is 0: this is the first record of the
				// partition.
				baseOffset: 0,
				// The log_append_time_ms is -1.
				logAppendTimeMs: -1,
				// The log_start_offset is 0.
				logStartOffset: 0,
			}
			if got := topic.partitions[0]; !reflect.DeepEqual(got, want) {
				t.Errorf("partition response = %+v, want %+v", got, want)
			}
		})
	}
}

// helloBatch builds a record batch like the ones that the CodeCrafters tester
// produces: its records have the given values, and its base offset is 0, as
// it is for all the batches that producers send.
func helloBatch(values ...string) []byte {
	batch := testBatch{timestamp: 1726045973899}
	for _, value := range values {
		batch.values = append(batch.values, []byte(value))
	}
	return encodeRecordBatch(batch)
}

// expectProduced checks that the response to a Produce request says that the
// records of the request were added to a single partition, at the given
// offset.
func expectProduced(t *testing.T, body produceResponse, topic string, partition int32, baseOffset int64) {
	t.Helper()

	if len(body.topics) != 1 {
		t.Fatalf("topics has %d elements, want 1", len(body.topics))
	}
	if body.topics[0].name != topic {
		t.Errorf("name = %q, want %q", body.topics[0].name, topic)
	}
	if len(body.topics[0].partitions) != 1 {
		t.Fatalf("partitions has %d elements, want 1", len(body.topics[0].partitions))
	}

	want := producedPartition{
		index:           partition,
		errorCode:       0,
		baseOffset:      baseOffset,
		logAppendTimeMs: -1,
		logStartOffset:  0,
	}
	if got := body.topics[0].partitions[0]; !reflect.DeepEqual(got, want) {
		t.Errorf("partition response = %+v, want %+v", got, want)
	}
}

// Stage ls8: the record of a Produce request is persisted to the log file of
// its partition, in the record batch format.
func TestProduceSingleRecord(t *testing.T) {
	// The log of the partition may not exist yet, or be an empty file.
	logSetups := map[string]func(t *testing.T, logDir string){
		"no log file": func(t *testing.T, logDir string) {},
		"empty log file": func(t *testing.T, logDir string) {
			writeLogSegment(t, logDir, "foo", 0, nil)
		},
	}

	for name, setUpLog := range logSetups {
		t.Run(name, func(t *testing.T) {
			logDir := t.TempDir()
			writeClusterMetadataLog(t, logDir,
				testTopic{name: "bar", id: testTopicID(1), partitions: []int32{0}},
				testTopic{name: "foo", id: testTopicID(2), partitions: []int32{0}},
			)
			setUpLog(t, logDir)
			addr := startTestServerWithLogDir(t, logDir)

			batch := helloBatch("Hello World!")

			body := produce(t, addr, produceTopic{
				name:       "foo",
				partitions: []producePartition{{index: 0, records: batch}},
			})

			// The error_code is 0, the base_offset is 0 (this is the first
			// record in the partition), the log_append_time_ms is -1 and the
			// log_start_offset is 0.
			expectProduced(t, body, "foo", 0, 0)

			// The record is persisted to the log file of the partition, at
			// <log-dir>/<topic-name>-<partition-index>/00000000000000000000.log,
			// as the record batch of the request.
			if got := readLogSegment(t, logDir, "foo", 0); !bytes.Equal(got, batch) {
				t.Errorf("log file = % x, want the record batch of the request: % x", got, batch)
			}

			// Nothing else was written to the log directory.
			if entries := listDir(t, logDir); !reflect.DeepEqual(entries, []string{"__cluster_metadata-0", "foo-0"}) {
				t.Errorf("log directory contains %q, want only the metadata log and the log of foo-0", entries)
			}
			if entries := listDir(t, filepath.Join(logDir, "foo-0")); !reflect.DeepEqual(entries, []string{"00000000000000000000.log"}) {
				t.Errorf("partition directory contains %q, want a single segment", entries)
			}
		})
	}
}

// Stage yd8: a record batch with several records is stored as a single unit
// in the log file of its partition. Its records get sequential offsets,
// starting at the base offset of the batch.
func TestProduceMultipleRecords(t *testing.T) {
	logDir := t.TempDir()
	writeClusterMetadataLog(t, logDir,
		testTopic{name: "bar", id: testTopicID(1), partitions: []int32{0}},
		testTopic{name: "foo", id: testTopicID(2), partitions: []int32{0}},
	)
	addr := startTestServerWithLogDir(t, logDir)

	messages := []string{"Hello Earth!", "Hello World!", "Hello Kafka!"}
	batch := helloBatch(messages...)

	body := produce(t, addr, produceTopic{
		name:       "foo",
		partitions: []producePartition{{index: 0, records: batch}},
	})

	// The error_code is 0, the base_offset is 0 (the base offset for the
	// batch), the log_append_time_ms is -1 and the log_start_offset is 0.
	expectProduced(t, body, "foo", 0, 0)

	// The record batch sent in the request is written to the log file.
	log := readLogSegment(t, logDir, "foo", 0)
	if !bytes.Equal(log, batch) {
		t.Errorf("log file = % x, want the record batch of the request: % x", log, batch)
	}

	// The log holds one batch, whose records have the offsets 0, 1 and 2.
	batches := mustDecodeBatches(t, log)
	if len(batches) != 1 {
		t.Fatalf("the log file holds %d batches, want 1", len(batches))
	}
	if batches[0].baseOffset != 0 || batches[0].lastOffset() != 2 {
		t.Errorf("the batch has the offsets %d-%d, want 0-2", batches[0].baseOffset, batches[0].lastOffset())
	}
	records := mustDecodeRecords(t, batches[0])
	if len(records) != len(messages) {
		t.Fatalf("the batch holds %d records, want %d", len(records), len(messages))
	}
	for i, message := range messages {
		if records[i].offsetDelta != int32(i) || string(records[i].value) != message {
			t.Errorf("record %d = {offset delta: %d, value: %q}, want {%d, %q}",
				i, records[i].offsetDelta, records[i].value, i, message)
		}
	}

	// A consumer gets the three records back, and sees that the partition
	// ends at the offset 3.
	fetched := fetch(t, addr, fetchTopic{topicID: testTopicID(2), partitions: []fetchPartition{{index: 0}}})
	if len(fetched.topics) != 1 || len(fetched.topics[0].partitions) != 1 {
		t.Fatalf("unexpected Fetch response: %+v", fetched)
	}
	partition := fetched.topics[0].partitions[0]
	if partition.errorCode != 0 || partition.highWatermark != 3 {
		t.Errorf("fetched partition: error_code = %d, high_watermark = %d; want 0, 3", partition.errorCode, partition.highWatermark)
	}
	if got, want := fetchedValues(t, partition), [][]string{messages}; !reflect.DeepEqual(got, want) {
		t.Errorf("fetched messages = %q, want %q", got, want)
	}

	// The next batch starts after the last record of this one: every record
	// took an offset.
	body = produce(t, addr, produceTopic{
		name:       "foo",
		partitions: []producePartition{{index: 0, records: helloBatch("Hello again!", "And again!")}},
	})
	expectProduced(t, body, "foo", 0, 3)

	batches = mustDecodeBatches(t, readLogSegment(t, logDir, "foo", 0))
	if len(batches) != 2 || batches[1].baseOffset != 3 || batches[1].lastOffset() != 4 {
		t.Errorf("unexpected batches in the log file after the second request: %+v", batches)
	}
}

// Stage ct4: a Produce request for several partitions of a topic writes the
// record batch of each partition to the log file of that partition, and is
// answered with a result for every partition.
func TestProduceToMultiplePartitions(t *testing.T) {
	logDir := t.TempDir()
	writeClusterMetadataLog(t, logDir,
		testTopic{name: "bar", id: testTopicID(1), partitions: []int32{0}},
		testTopic{name: "foo", id: testTopicID(2), partitions: []int32{0, 1, 2}},
	)
	addr := startTestServerWithLogDir(t, logDir)

	// One record batch with a single record for each partition, in an order
	// that is not the one of the partition indexes.
	request := []producePartition{
		{index: 1, records: helloBatch("Hello partition 1!")},
		{index: 0, records: helloBatch("Hello partition 0!")},
		{index: 2, records: helloBatch("Hello partition 2!")},
	}

	body := produce(t, addr, produceTopic{name: "foo", partitions: request})

	// The topics field has 1 element, whose name matches the topic name in
	// the request.
	if len(body.topics) != 1 {
		t.Fatalf("topics has %d elements, want 1", len(body.topics))
	}
	topic := body.topics[0]
	if topic.name != "foo" {
		t.Errorf("name = %q, want %q", topic.name, "foo")
	}

	// The partitions field has one element for each partition in the request.
	if len(topic.partitions) != len(request) {
		t.Fatalf("partitions has %d elements, want %d", len(topic.partitions), len(request))
	}
	for i, partition := range request {
		want := producedPartition{
			// The index matches the partition index in the request.
			index: partition.index,
			// The error_code is 0 (NO_ERROR).
			errorCode: 0,
			// The base_offset is 0: this is the first record in each
			// partition.
			baseOffset: 0,
			// The log_append_time_ms is -1.
			logAppendTimeMs: -1,
			// The log_start_offset is 0.
			logStartOffset: 0,
		}
		if got := topic.partitions[i]; !reflect.DeepEqual(got, want) {
			t.Errorf("partitions[%d] = %+v, want %+v", i, got, want)
		}

		// Each record batch is persisted to the log file of its partition.
		if got := readLogSegment(t, logDir, "foo", partition.index); !bytes.Equal(got, partition.records) {
			t.Errorf("log file of partition %d = % x, want the record batch sent for it: % x", partition.index, got, partition.records)
		}
	}

	// Only the logs of the three partitions were written.
	if entries, want := listDir(t, logDir), []string{"__cluster_metadata-0", "foo-0", "foo-1", "foo-2"}; !reflect.DeepEqual(entries, want) {
		t.Errorf("log directory contains %q, want %q", entries, want)
	}

	// Each partition has offsets of its own: a second request continues
	// every partition where the first one left it.
	body = produce(t, addr, produceTopic{name: "foo", partitions: []producePartition{
		{index: 2, records: helloBatch("More for partition 2!", "And more!")},
		{index: 0, records: helloBatch("More for partition 0!")},
	}})
	if len(body.topics) != 1 || len(body.topics[0].partitions) != 2 {
		t.Fatalf("unexpected response to the second request: %+v", body)
	}
	for i, index := range []int32{2, 0} {
		want := producedPartition{index: index, baseOffset: 1, logAppendTimeMs: -1}
		if got := body.topics[0].partitions[i]; !reflect.DeepEqual(got, want) {
			t.Errorf("second request: partitions[%d] = %+v, want %+v", i, got, want)
		}
	}

	for index, wantOffsets := range map[int32][][2]int64{
		0: {{0, 0}, {1, 1}},
		1: {{0, 0}},
		2: {{0, 0}, {1, 2}},
	} {
		batches := mustDecodeBatches(t, readLogSegment(t, logDir, "foo", index))
		if len(batches) != len(wantOffsets) {
			t.Errorf("the log of partition %d holds %d batches, want %d", index, len(batches), len(wantOffsets))
			continue
		}
		for i, want := range wantOffsets {
			if batches[i].baseOffset != want[0] || batches[i].lastOffset() != want[1] {
				t.Errorf("partition %d: batch %d has the offsets %d-%d, want %d-%d",
					index, i, batches[i].baseOffset, batches[i].lastOffset(), want[0], want[1])
			}
		}
	}
}

// Stage ov0: a Produce request for several partitions of several topics
// writes the record batch of each partition to the log file of that
// partition, and is answered with a result for every topic and partition.
func TestProduceToMultipleTopics(t *testing.T) {
	logDir := t.TempDir()
	writeClusterMetadataLog(t, logDir,
		testTopic{name: "apple", id: testTopicID(1), partitions: []int32{0, 1, 2}},
		testTopic{name: "mango", id: testTopicID(2), partitions: []int32{0}},
		testTopic{name: "zebra", id: testTopicID(3), partitions: []int32{0, 1}},
	)
	addr := startTestServerWithLogDir(t, logDir)

	// One record batch with a single record for each partition of each
	// topic. Neither the topics nor the partitions are sorted.
	request := []produceTopic{
		{name: "zebra", partitions: []producePartition{
			{index: 1, records: helloBatch("Hello zebra-1!")},
			{index: 0, records: helloBatch("Hello zebra-0!")},
		}},
		{name: "apple", partitions: []producePartition{
			{index: 0, records: helloBatch("Hello apple-0!")},
			{index: 2, records: helloBatch("Hello apple-2!")},
			{index: 1, records: helloBatch("Hello apple-1!")},
		}},
		{name: "mango", partitions: []producePartition{
			{index: 0, records: helloBatch("Hello mango-0!")},
		}},
	}

	body := produce(t, addr, request...)

	// The topics field has one element for each topic in the request.
	if len(body.topics) != len(request) {
		t.Fatalf("topics has %d elements, want %d", len(body.topics), len(request))
	}
	for i, wantTopic := range request {
		topic := body.topics[i]

		// The name matches the topic name in the request.
		if topic.name != wantTopic.name {
			t.Errorf("topics[%d]: name = %q, want %q", i, topic.name, wantTopic.name)
		}

		// The partitions field has one element for each partition of that
		// topic in the request.
		if len(topic.partitions) != len(wantTopic.partitions) {
			t.Fatalf("topics[%d]: partitions has %d elements, want %d", i, len(topic.partitions), len(wantTopic.partitions))
		}
		for j, partition := range wantTopic.partitions {
			want := producedPartition{
				// The index matches the partition index in the request.
				index: partition.index,
				// The error_code is 0 (NO_ERROR).
				errorCode: 0,
				// The base_offset is 0: this is the first record in each
				// partition.
				baseOffset: 0,
				// The log_append_time_ms is -1.
				logAppendTimeMs: -1,
				// The log_start_offset is 0.
				logStartOffset: 0,
			}
			if got := topic.partitions[j]; !reflect.DeepEqual(got, want) {
				t.Errorf("topics[%d].partitions[%d] = %+v, want %+v", i, j, got, want)
			}

			// Each record batch is persisted to the log file of its
			// partition.
			if got := readLogSegment(t, logDir, wantTopic.name, partition.index); !bytes.Equal(got, partition.records) {
				t.Errorf("log file of %s-%d = % x, want the record batch sent for it: % x",
					wantTopic.name, partition.index, got, partition.records)
			}
		}
	}

	// Only the logs of the six partitions were written.
	wantEntries := []string{"__cluster_metadata-0", "apple-0", "apple-1", "apple-2", "mango-0", "zebra-0", "zebra-1"}
	if entries := listDir(t, logDir); !reflect.DeepEqual(entries, wantEntries) {
		t.Errorf("log directory contains %q, want %q", entries, wantEntries)
	}

	// Consumers find each record in the partition that it was produced to.
	fetched := fetch(t, addr,
		fetchTopic{topicID: testTopicID(1), partitions: []fetchPartition{{index: 0}, {index: 1}, {index: 2}}},
		fetchTopic{topicID: testTopicID(2), partitions: []fetchPartition{{index: 0}}},
		fetchTopic{topicID: testTopicID(3), partitions: []fetchPartition{{index: 0}, {index: 1}}},
	)
	wantMessages := [][]string{
		{"Hello apple-0!", "Hello apple-1!", "Hello apple-2!"},
		{"Hello mango-0!"},
		{"Hello zebra-0!", "Hello zebra-1!"},
	}
	if len(fetched.topics) != len(wantMessages) {
		t.Fatalf("Fetch response has %d topics, want %d", len(fetched.topics), len(wantMessages))
	}
	for i, messages := range wantMessages {
		if len(fetched.topics[i].partitions) != len(messages) {
			t.Fatalf("Fetch response: topics[%d] has %d partitions, want %d", i, len(fetched.topics[i].partitions), len(messages))
		}
		for j, message := range messages {
			partition := fetched.topics[i].partitions[j]
			if got, want := fetchedValues(t, partition), [][]string{{message}}; partition.errorCode != 0 || !reflect.DeepEqual(got, want) {
				t.Errorf("Fetch response: topics[%d].partitions[%d] = {error_code: %d, messages: %q}, want %q",
					i, j, partition.errorCode, got, want)
			}
		}
	}
}

// A request may mix topics that exist with ones that don't: the records for
// the existing ones are stored.
func TestProduceToMultipleTopicsWithFailures(t *testing.T) {
	logDir := t.TempDir()
	writeClusterMetadataLog(t, logDir,
		testTopic{name: "apple", id: testTopicID(1), partitions: []int32{0}},
		testTopic{name: "zebra", id: testTopicID(2), partitions: []int32{0}},
	)
	addr := startTestServerWithLogDir(t, logDir)

	appleBatch, zebraBatch := helloBatch("Hello apple!"), helloBatch("Hello zebra!")

	body := produce(t, addr,
		produceTopic{name: "zebra", partitions: []producePartition{{index: 0, records: zebraBatch}, {index: 1, records: helloBatch("lost")}}},
		produceTopic{name: "nope", partitions: []producePartition{{index: 0, records: helloBatch("lost")}}},
		produceTopic{name: "apple", partitions: []producePartition{{index: 0, records: appleBatch}}},
	)

	if len(body.topics) != 3 {
		t.Fatalf("topics has %d elements, want 3", len(body.topics))
	}
	for i, want := range []struct {
		name       string
		partitions int
	}{{"zebra", 2}, {"nope", 1}, {"apple", 1}} {
		if body.topics[i].name != want.name || len(body.topics[i].partitions) != want.partitions {
			t.Fatalf("topics[%d] = %+v, want the topic %q with %d partitions", i, body.topics[i], want.name, want.partitions)
		}
	}

	success := func(index int32) producedPartition {
		return producedPartition{index: index, logAppendTimeMs: -1}
	}
	if got := body.topics[0].partitions[0]; !reflect.DeepEqual(got, success(0)) {
		t.Errorf("zebra-0 = %+v, want %+v", got, success(0))
	}
	expectProduceError(t, body.topics[0].partitions[1], 1, 3)
	expectProduceError(t, body.topics[1].partitions[0], 0, 3)
	if got := body.topics[2].partitions[0]; !reflect.DeepEqual(got, success(0)) {
		t.Errorf("apple-0 = %+v, want %+v", got, success(0))
	}

	if got := readLogSegment(t, logDir, "zebra", 0); !bytes.Equal(got, zebraBatch) {
		t.Errorf("log file of zebra-0 = % x, want % x", got, zebraBatch)
	}
	if got := readLogSegment(t, logDir, "apple", 0); !bytes.Equal(got, appleBatch) {
		t.Errorf("log file of apple-0 = % x, want % x", got, appleBatch)
	}
	if entries, want := listDir(t, logDir), []string{"__cluster_metadata-0", "apple-0", "zebra-0"}; !reflect.DeepEqual(entries, want) {
		t.Errorf("log directory contains %q, want %q", entries, want)
	}
}

// A partition that can't be written to doesn't keep the records of the other
// partitions of the request from being stored.
func TestProduceToMultiplePartitionsWithFailures(t *testing.T) {
	logDir := t.TempDir()
	writeClusterMetadataLog(t, logDir, testTopic{name: "foo", id: testTopicID(1), partitions: []int32{0, 1}})
	addr := startTestServerWithLogDir(t, logDir)

	stored := helloBatch("stored")

	body := produce(t, addr, produceTopic{name: "foo", partitions: []producePartition{
		{index: 7, records: helloBatch("unknown partition")},
		{index: 0, records: stored},
		{index: 1, records: []byte("not a record batch")},
	}})

	if len(body.topics) != 1 || len(body.topics[0].partitions) != 3 {
		t.Fatalf("unexpected response: %+v", body)
	}
	partitions := body.topics[0].partitions

	expectProduceError(t, partitions[0], 7, 3)
	if want := (producedPartition{index: 0, logAppendTimeMs: -1}); !reflect.DeepEqual(partitions[1], want) {
		t.Errorf("partitions[1] = %+v, want %+v", partitions[1], want)
	}
	expectProduceError(t, partitions[2], 1, 87)

	if got := readLogSegment(t, logDir, "foo", 0); !bytes.Equal(got, stored) {
		t.Errorf("log file of partition 0 = % x, want % x", got, stored)
	}
	if entries, want := listDir(t, logDir), []string{"__cluster_metadata-0", "foo-0"}; !reflect.DeepEqual(entries, want) {
		t.Errorf("log directory contains %q, want %q", entries, want)
	}
}

// listDir returns the names of the entries of a directory, sorted.
func listDir(t *testing.T, dir string) []string {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("failed to list directory: %v", err)
	}

	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

// Records that were produced can be fetched: the batch that Fetch returns is
// the one that Produce stored.
func TestProduceThenFetch(t *testing.T) {
	logDir := t.TempDir()
	writeClusterMetadataLog(t, logDir, testTopic{name: "foo", id: testTopicID(1), partitions: []int32{0}})
	addr := startTestServerWithLogDir(t, logDir)

	batch := helloBatch("Hello World!")
	expectProduced(t, produce(t, addr, produceTopic{
		name:       "foo",
		partitions: []producePartition{{index: 0, records: batch}},
	}), "foo", 0, 0)

	body := fetch(t, addr, fetchTopic{topicID: testTopicID(1), partitions: []fetchPartition{{index: 0}}})

	if len(body.topics) != 1 || len(body.topics[0].partitions) != 1 {
		t.Fatalf("unexpected Fetch response: %+v", body)
	}
	partition := body.topics[0].partitions[0]
	if partition.errorCode != 0 || partition.highWatermark != 1 {
		t.Errorf("fetched partition: error_code = %d, high_watermark = %d; want 0, 1", partition.errorCode, partition.highWatermark)
	}
	if !bytes.Equal(partition.records, batch) {
		t.Errorf("fetched records = % x, want the produced batch: % x", partition.records, batch)
	}
	if got, want := fetchedValues(t, partition), [][]string{{"Hello World!"}}; !reflect.DeepEqual(got, want) {
		t.Errorf("fetched messages = %q, want %q", got, want)
	}
}

// Records are appended to the log of their partition: they get the offsets
// that follow the ones of the records that are already there.
func TestProduceAppendsToLog(t *testing.T) {
	t.Run("consecutive requests", func(t *testing.T) {
		logDir := t.TempDir()
		writeClusterMetadataLog(t, logDir, testTopic{name: "foo", id: testTopicID(1), partitions: []int32{0}})
		addr := startTestServerWithLogDir(t, logDir)

		messages := []string{"Hello Earth!", "Hello World!", "Hello Kafka!"}
		for offset, message := range messages {
			body := produce(t, addr, produceTopic{
				name:       "foo",
				partitions: []producePartition{{index: 0, records: helloBatch(message)}},
			})
			expectProduced(t, body, "foo", 0, int64(offset))
		}

		batches, err := decodeRecordBatches(readLogSegment(t, logDir, "foo", 0))
		if err != nil {
			t.Fatalf("the log file is not a sequence of record batches: %v", err)
		}
		if len(batches) != len(messages) {
			t.Fatalf("the log file holds %d batches, want %d", len(batches), len(messages))
		}
		for offset, message := range messages {
			batch := batches[offset]

			// The batch got the next offset of the log...
			if batch.baseOffset != int64(offset) {
				t.Errorf("batch %d: base offset = %d, want %d", offset, batch.baseOffset, offset)
			}
			// ... and is otherwise stored as the producer sent it.
			if sent := helloBatch(message); !bytes.Equal(batch.raw[8:], sent[8:]) {
				t.Errorf("batch %d = % x, want % x after its base offset", offset, batch.raw[8:], sent[8:])
			}
			if records := mustDecodeRecords(t, batch); len(records) != 1 || string(records[0].value) != message {
				t.Errorf("batch %d holds the records %+v, want a single record with the value %q", offset, records, message)
			}
		}
	})

	t.Run("log that already holds records", func(t *testing.T) {
		existing := encodeRecordBatch(testBatch{baseOffset: 0, values: [][]byte{[]byte("a"), []byte("b"), []byte("c")}})

		logDir := t.TempDir()
		writeClusterMetadataLog(t, logDir, testTopic{name: "foo", id: testTopicID(1), partitions: []int32{0}})
		writeLogSegment(t, logDir, "foo", 0, existing)
		addr := startTestServerWithLogDir(t, logDir)

		body := produce(t, addr, produceTopic{
			name:       "foo",
			partitions: []producePartition{{index: 0, records: helloBatch("d")}},
		})
		expectProduced(t, body, "foo", 0, 3)

		// The records that were there are untouched; the new batch follows
		// them, with the offset 3.
		log := readLogSegment(t, logDir, "foo", 0)
		if !bytes.HasPrefix(log, existing) {
			t.Fatalf("the log file no longer starts with the records that it held")
		}
		batches, err := decodeRecordBatches(log)
		if err != nil {
			t.Fatalf("the log file is not a sequence of record batches: %v", err)
		}
		if len(batches) != 2 || batches[1].baseOffset != 3 || batches[1].lastOffset() != 3 {
			t.Errorf("unexpected batches in the log file: %+v", batches)
		}
	})

	t.Run("several batches in one request", func(t *testing.T) {
		logDir := t.TempDir()
		writeClusterMetadataLog(t, logDir, testTopic{name: "foo", id: testTopicID(1), partitions: []int32{0}})
		addr := startTestServerWithLogDir(t, logDir)

		records := append(helloBatch("a", "b"), helloBatch("c")...)
		body := produce(t, addr, produceTopic{
			name:       "foo",
			partitions: []producePartition{{index: 0, records: records}},
		})
		expectProduced(t, body, "foo", 0, 0)

		batches, err := decodeRecordBatches(readLogSegment(t, logDir, "foo", 0))
		if err != nil {
			t.Fatalf("the log file is not a sequence of record batches: %v", err)
		}
		if len(batches) != 2 || batches[0].baseOffset != 0 || batches[0].lastOffset() != 1 || batches[1].baseOffset != 2 {
			t.Errorf("unexpected batches in the log file: %+v", batches)
		}

		// The next request continues after the last record.
		body = produce(t, addr, produceTopic{
			name:       "foo",
			partitions: []producePartition{{index: 0, records: helloBatch("d")}},
		})
		expectProduced(t, body, "foo", 0, 3)
	})
}

// Records that are not valid record batches are rejected with the error code
// INVALID_RECORD (87), and leave the log of the partition as it was.
func TestProduceInvalidRecords(t *testing.T) {
	valid := helloBatch("Hello World!")

	// withByte returns a copy of the valid batch in which one byte is changed.
	withByte := func(offset int, value byte) []byte {
		b := bytes.Clone(valid)
		b[offset] = value
		return b
	}

	tests := []struct {
		name    string
		records []byte
	}{
		{name: "null records", records: nil},
		{name: "empty records", records: []byte{}},
		{name: "garbage", records: []byte("these are not record batches")},
		{name: "truncated batch", records: valid[:len(valid)-3]},
		{name: "trailing bytes", records: append(bytes.Clone(valid), 0x00, 0x01)},
		{name: "unsupported magic", records: withByte(16, 1)},
		{name: "control batch", records: withByte(22, 0x20)},
		{name: "last offset delta that doesn't match the record count", records: withByte(26, 5)},
		{name: "negative last offset delta", records: withByte(23, 0xff)},
		{name: "more records announced than present", records: withByte(60, 2)},
		{name: "corrupt record", records: withByte(61, 0x7e)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logDir := t.TempDir()
			writeClusterMetadataLog(t, logDir, testTopic{name: "foo", id: testTopicID(1), partitions: []int32{0}})
			writeLogSegment(t, logDir, "foo", 0, valid)
			addr := startTestServerWithLogDir(t, logDir)

			body := produce(t, addr, produceTopic{
				name:       "foo",
				partitions: []producePartition{{index: 0, records: tt.records}},
			})

			if len(body.topics) != 1 || len(body.topics[0].partitions) != 1 {
				t.Fatalf("unexpected response: %+v", body)
			}
			expectProduceError(t, body.topics[0].partitions[0], 0, 87)

			if got := readLogSegment(t, logDir, "foo", 0); !bytes.Equal(got, valid) {
				t.Errorf("the log file changed: % x, want % x", got, valid)
			}
		})
	}
}

// Records for a topic or a partition that doesn't exist are not stored.
func TestProduceToUnknownPartitionWritesNothing(t *testing.T) {
	logDir := t.TempDir()
	writeClusterMetadataLog(t, logDir, testTopic{name: "foo", id: testTopicID(1), partitions: []int32{0}})
	addr := startTestServerWithLogDir(t, logDir)

	body := produce(t, addr,
		produceTopic{name: "foo", partitions: []producePartition{{index: 1, records: helloBatch("lost")}}},
		produceTopic{name: "nope", partitions: []producePartition{{index: 0, records: helloBatch("lost")}}},
	)

	if len(body.topics) != 2 || len(body.topics[0].partitions) != 1 || len(body.topics[1].partitions) != 1 {
		t.Fatalf("unexpected response: %+v", body)
	}
	expectProduceError(t, body.topics[0].partitions[0], 1, 3)
	expectProduceError(t, body.topics[1].partitions[0], 0, 3)

	if entries := listDir(t, logDir); !reflect.DeepEqual(entries, []string{"__cluster_metadata-0"}) {
		t.Errorf("log directory contains %q, want only the metadata log", entries)
	}
}

// A partition whose log can't be written to is reported with the error code
// KAFKA_STORAGE_ERROR (56).
func TestProduceToUnwritableLog(t *testing.T) {
	logDir := t.TempDir()
	writeClusterMetadataLog(t, logDir, testTopic{name: "foo", id: testTopicID(1), partitions: []int32{0, 1}})

	// The log of partition 0 is corrupt, and a file is in the way of the
	// directory of partition 1.
	writeLogSegment(t, logDir, "foo", 0, []byte{0x00, 0x01, 0x02})
	writeFile(t, filepath.Join(logDir, "foo-1"), []byte("not a directory"))
	addr := startTestServerWithLogDir(t, logDir)

	body := produce(t, addr, produceTopic{name: "foo", partitions: []producePartition{
		{index: 0, records: helloBatch("lost")},
		{index: 1, records: helloBatch("lost")},
	}})

	if len(body.topics) != 1 || len(body.topics[0].partitions) != 2 {
		t.Fatalf("unexpected response: %+v", body)
	}
	expectProduceError(t, body.topics[0].partitions[0], 0, 56)
	expectProduceError(t, body.topics[0].partitions[1], 1, 56)
}

// Producers that write to the same partition at the same time each get their
// own offsets, and none of their records is lost or damaged. Run with -race,
// this also checks that producing and fetching concurrently is free of data
// races.
func TestConcurrentProducers(t *testing.T) {
	const (
		producers         = 8
		recordsPerRequest = 3
		requests          = 5
	)

	logDir := t.TempDir()
	writeClusterMetadataLog(t, logDir, testTopic{name: "foo", id: testTopicID(1), partitions: []int32{0}})
	addr := startTestServerWithLogDir(t, logDir)

	var mu sync.Mutex
	baseOffsets := map[int64]bool{}

	t.Run("group", func(t *testing.T) {
		for producer := 0; producer < producers; producer++ {
			t.Run(fmt.Sprintf("producer %d", producer), func(t *testing.T) {
				t.Parallel()

				conn := dial(t, addr)
				for request := int32(0); request < requests; request++ {
					var values []string
					for record := 0; record < recordsPerRequest; record++ {
						values = append(values, fmt.Sprintf("producer %d, request %d, record %d", producer, request, record))
					}

					send(t, conn, newProduceRequest(request, produceTopic{
						name:       "foo",
						partitions: []producePartition{{index: 0, records: helloBatch(values...)}},
					}))
					body := expectProduceResponse(t, conn, request)

					if len(body.topics) != 1 || len(body.topics[0].partitions) != 1 || body.topics[0].partitions[0].errorCode != 0 {
						t.Fatalf("unexpected response: %+v", body)
					}

					mu.Lock()
					baseOffsets[body.topics[0].partitions[0].baseOffset] = true
					mu.Unlock()
				}
			})
		}

		// Meanwhile, a consumer keeps reading the partition: it must never
		// see a batch that is only partly written.
		t.Run("consumer", func(t *testing.T) {
			t.Parallel()

			for i := 0; i < 20; i++ {
				body := fetch(t, addr, fetchTopic{topicID: testTopicID(1), partitions: []fetchPartition{{index: 0}}})
				if len(body.topics) != 1 || len(body.topics[0].partitions) != 1 {
					t.Fatalf("unexpected Fetch response: %+v", body)
				}
				if partition := body.topics[0].partitions[0]; partition.errorCode != 0 {
					t.Fatalf("fetch failed with the error code %d", partition.errorCode)
				} else if _, err := decodeRecordBatches(partition.records); err != nil {
					t.Fatalf("fetched records are not whole record batches: %v", err)
				}
			}
		})
	})

	// Every request got a base offset of its own: 0, 3, 6, ...
	const totalRequests = producers * requests
	if len(baseOffsets) != totalRequests {
		t.Errorf("got %d distinct base offsets, want %d", len(baseOffsets), totalRequests)
	}
	for i := int64(0); i < totalRequests; i++ {
		if !baseOffsets[i*recordsPerRequest] {
			t.Errorf("no request got the base offset %d", i*recordsPerRequest)
		}
	}

	// The log holds all the batches, with consecutive offsets and intact
	// records.
	batches, err := decodeRecordBatches(readLogSegment(t, logDir, "foo", 0))
	if err != nil {
		t.Fatalf("the log file is not a sequence of record batches: %v", err)
	}
	if len(batches) != totalRequests {
		t.Fatalf("the log file holds %d batches, want %d", len(batches), totalRequests)
	}
	seen := map[string]bool{}
	for i, batch := range batches {
		if want := int64(i * recordsPerRequest); batch.baseOffset != want {
			t.Errorf("batch %d: base offset = %d, want %d", i, batch.baseOffset, want)
		}
		for _, record := range mustDecodeRecords(t, batch) {
			seen[string(record.value)] = true
		}
	}
	if want := totalRequests * recordsPerRequest; len(seen) != want {
		t.Errorf("the log file holds %d distinct records, want %d", len(seen), want)
	}
}

// Each partition of a Produce request gets its own answer: some may be valid
// while others are not.
func TestProduceToValidAndInvalidPartitions(t *testing.T) {
	logDir := t.TempDir()
	writeClusterMetadataLog(t, logDir,
		testTopic{name: "foo", id: testTopicID(1), partitions: []int32{0, 1}},
		testTopic{name: "empty", id: testTopicID(2)},
	)
	addr := startTestServerWithLogDir(t, logDir)

	records := encodeRecordBatch(testBatch{values: [][]byte{[]byte("Hello World!")}})

	body := produce(t, addr,
		produceTopic{name: "foo", partitions: []producePartition{
			{index: 1, records: records},
			{index: 2, records: records},
			{index: 0, records: records},
			{index: -1, records: records},
		}},
		produceTopic{name: "nope", partitions: []producePartition{{index: 0, records: records}}},
		produceTopic{name: "empty", partitions: []producePartition{{index: 0, records: records}}},
		// Topic names are case-sensitive.
		produceTopic{name: "FOO", partitions: []producePartition{{index: 0, records: records}}},
	)

	type partitionResult struct {
		index     int32
		errorCode int16
	}
	want := []struct {
		name       string
		partitions []partitionResult
	}{
		{name: "foo", partitions: []partitionResult{{1, 0}, {2, 3}, {0, 0}, {-1, 3}}},
		{name: "nope", partitions: []partitionResult{{0, 3}}},
		{name: "empty", partitions: []partitionResult{{0, 3}}},
		{name: "FOO", partitions: []partitionResult{{0, 3}}},
	}

	if len(body.topics) != len(want) {
		t.Fatalf("topics has %d elements, want %d", len(body.topics), len(want))
	}
	for i, wantTopic := range want {
		got := body.topics[i]
		if got.name != wantTopic.name {
			t.Errorf("topics[%d]: name = %q, want %q", i, got.name, wantTopic.name)
		}
		if len(got.partitions) != len(wantTopic.partitions) {
			t.Fatalf("topics[%d]: partitions has %d elements, want %d", i, len(got.partitions), len(wantTopic.partitions))
		}
		for j, wantPartition := range wantTopic.partitions {
			if wantPartition.errorCode != 0 {
				expectProduceError(t, got.partitions[j], wantPartition.index, wantPartition.errorCode)
				continue
			}

			wantSuccess := producedPartition{index: wantPartition.index, logAppendTimeMs: -1}
			if !reflect.DeepEqual(got.partitions[j], wantSuccess) {
				t.Errorf("topics[%d].partitions[%d] = %+v, want %+v", i, j, got.partitions[j], wantSuccess)
			}
		}
	}
}

// A producer that asks for no acknowledgement (acks=0) doesn't read
// responses, so the broker doesn't send any: the next response on the
// connection is the one to the next request.
func TestProduceWithoutAcknowledgement(t *testing.T) {
	logDir := t.TempDir()
	writeClusterMetadataLog(t, logDir, testTopic{name: "foo", id: testTopicID(1), partitions: []int32{0}})
	addr := startTestServerWithLogDir(t, logDir)

	conn := dial(t, addr)

	send(t, conn, newProduceRequestWithAcks(1, 0, produceTopic{
		name:       "foo",
		partitions: []producePartition{{index: 0, records: encodeRecordBatch(testBatch{values: [][]byte{[]byte("fire and forget")}})}},
	}))

	send(t, conn, newApiVersionsRequest(2))
	expectApiVersionsResponse(t, conn, 2)

	expectNoMoreData(t, conn)
}

// A cluster metadata log that can't be read is not mistaken for a cluster
// without topics: the request fails, and the connection is closed.
func TestProduceWithCorruptMetadataLog(t *testing.T) {
	logDir := t.TempDir()
	writeLogSegment(t, logDir, "__cluster_metadata", 0, mustDecodeHex(t, sazTopicBatchHex)[:100])
	addr := startTestServerWithLogDir(t, logDir)

	conn := dial(t, addr)
	send(t, conn, newProduceRequest(1, produceTopic{name: "saz", partitions: []producePartition{{index: 1}}}))

	expectConnectionClosed(t, conn)
}

// The response to a Produce request has the topics and partitions of the
// request, in the same order.
func TestProduceResponseMirrorsRequest(t *testing.T) {
	addr := startTestServer(t)

	request := []produceTopic{
		{name: "zebra", partitions: []producePartition{{index: 2}, {index: 0}}},
		{name: "apple", partitions: []producePartition{{index: 1}}},
	}
	body := produce(t, addr, request...)

	if len(body.topics) != len(request) {
		t.Fatalf("topics has %d elements, want %d", len(body.topics), len(request))
	}
	for i, want := range request {
		got := body.topics[i]
		if got.name != want.name {
			t.Errorf("topics[%d]: name = %q, want %q", i, got.name, want.name)
		}
		if len(got.partitions) != len(want.partitions) {
			t.Fatalf("topics[%d]: partitions has %d elements, want %d", i, len(got.partitions), len(want.partitions))
		}
		for j, partition := range want.partitions {
			expectProduceError(t, got.partitions[j], partition.index, 3)
		}
	}
}

// The only way to tell a producer that asks for no acknowledgement that its
// request failed is to close the connection.
func TestProduceWithoutAcknowledgementFails(t *testing.T) {
	addr := startTestServer(t)
	conn := dial(t, addr)

	send(t, conn, newProduceRequestWithAcks(1, 0, produceTopic{
		name:       "unknown-topic",
		partitions: []producePartition{{index: 0, records: encodeRecordBatch(testBatch{values: [][]byte{[]byte("lost")}})}},
	}))

	expectConnectionClosed(t, conn)
}

// The broker only supports version 11 of Produce: requests for other
// versions, and requests that it can't parse, make it close the connection.
func TestInvalidProduceRequestClosesConnection(t *testing.T) {
	validBody := newProduceRequest(1, produceTopic{name: "foo", partitions: []producePartition{{index: 0}}})[4+2+2+4+2+len("kafka-cli")+1:]

	tests := []struct {
		name    string
		request []byte
	}{
		{name: "version 10", request: encodeRequest(0, 10, 1, validBody)},
		{name: "version 12", request: encodeRequest(0, 12, 1, validBody)},
		{name: "version 0", request: encodeRequest(0, 0, 1, validBody)},
		{name: "no body", request: encodeRequest(0, 11, 1, nil)},
		{name: "truncated body", request: encodeRequest(0, 11, 1, validBody[:len(validBody)-2])},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr := startTestServer(t)
			conn := dial(t, addr)

			send(t, conn, tt.request)
			expectConnectionClosed(t, conn)
		})
	}
}
