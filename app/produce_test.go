package main

import (
	"bytes"
	"encoding/binary"
	"net"
	"reflect"
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

// A producer that asks for no acknowledgement (acks=0) doesn't read
// responses, so the broker doesn't send any. The only way to tell such a
// producer that its request failed is to close the connection.
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
