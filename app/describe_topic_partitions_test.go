package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"reflect"
	"testing"
)

// describeFooRequestHex is the DescribeTopicPartitions request for the topic
// "foo" from the "List for an unknown topic" stage instructions.
const describeFooRequestHex = "00000020" + // message_size: 32
	"004b" + // request_api_key:     75 (DescribeTopicPartitions)
	"0000" + // request_api_version: 0
	"00000007" + // correlation_id:      7
	"00096b61666b612d636c69" + // client_id:           "kafka-cli"
	"00" + // tag buffer
	"02" + // topics array: 1 element
	"04666f6f" + // topic_name: "foo"
	"00" + // tag buffer
	"00000064" + // response_partition_limit: 100
	"ff" + // cursor: null
	"00" // tag buffer

// newDescribeTopicPartitionsRequest builds a DescribeTopicPartitions (v0)
// request for the given topics, like the one the CodeCrafters tester sends.
func newDescribeTopicPartitionsRequest(correlationID int32, topicNames ...string) []byte {
	var body []byte
	body = binary.AppendUvarint(body, uint64(len(topicNames)+1)) // topics
	for _, name := range topicNames {
		body = appendCompactString(body, name)
		body = append(body, 0) // tag buffer
	}
	body = binary.BigEndian.AppendUint32(body, 100) // response_partition_limit
	body = append(body, 0xff)                       // cursor: null
	body = append(body, 0)                          // tag buffer

	return encodeRequest(75, 0, correlationID, body)
}

// describeTopicPartitionsResponse is a decoded DescribeTopicPartitions
// response body (v0).
type describeTopicPartitionsResponse struct {
	throttleTimeMs int32
	topics         []describedTopic
	nextCursor     *topicPartitionCursor // nil when the cursor is null
}

type describedTopic struct {
	errorCode            int16
	name                 string
	id                   uuid
	isInternal           bool
	partitions           []describedPartition
	authorizedOperations int32
}

type describedPartition struct {
	errorCode              int16
	partitionIndex         int32
	leaderID               int32
	leaderEpoch            int32
	replicaNodes           []int32
	isrNodes               []int32
	eligibleLeaderReplicas []int32
	lastKnownELR           []int32
	offlineReplicas        []int32
}

// decodeDescribeTopicPartitionsResponse decodes a DescribeTopicPartitions
// response body (v0) and fails the test if the body is malformed or followed
// by extra bytes.
func decodeDescribeTopicPartitionsResponse(t *testing.T, body []byte) describeTopicPartitionsResponse {
	t.Helper()

	d := newDecoder(body)

	int32Array := func() []int32 {
		var values []int32
		for i, n := 0, d.compactArrayLength(); i < n && d.err == nil; i++ {
			values = append(values, d.int32())
		}
		return values
	}

	var response describeTopicPartitionsResponse
	response.throttleTimeMs = d.int32()

	for i, n := 0, d.compactArrayLength(); i < n && d.err == nil; i++ {
		var topic describedTopic
		topic.errorCode = d.int16()
		topic.name = d.compactString()
		copy(topic.id[:], d.bytes(16))
		topic.isInternal = d.int8() != 0

		for j, m := 0, d.compactArrayLength(); j < m && d.err == nil; j++ {
			topic.partitions = append(topic.partitions, describedPartition{
				errorCode:              d.int16(),
				partitionIndex:         d.int32(),
				leaderID:               d.int32(),
				leaderEpoch:            d.int32(),
				replicaNodes:           int32Array(),
				isrNodes:               int32Array(),
				eligibleLeaderReplicas: int32Array(),
				lastKnownELR:           int32Array(),
				offlineReplicas:        int32Array(),
			})
			d.taggedFields()
		}

		topic.authorizedOperations = d.int32()
		d.taggedFields()

		response.topics = append(response.topics, topic)
	}

	// next_cursor is a nullable struct: a negative first byte means null.
	if d.int8() >= 0 && d.err == nil {
		response.nextCursor = &topicPartitionCursor{
			topicName:      d.compactString(),
			partitionIndex: d.int32(),
		}
		d.taggedFields()
	}
	d.taggedFields()

	if d.err != nil {
		t.Fatalf("failed to decode DescribeTopicPartitions response body % x: %v", body, d.err)
	}
	if len(d.buf) != 0 {
		t.Fatalf("%d extra bytes after the DescribeTopicPartitions response body: % x", len(d.buf), d.buf)
	}

	return response
}

func TestDecodeDescribeTopicPartitionsRequest(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  describeTopicPartitionsRequest
	}{
		{
			name:  "example from the stage instructions",
			input: "02 04666f6f 00 00000064 ff 00",
			want: describeTopicPartitionsRequest{
				topicNames:             []string{"foo"},
				responsePartitionLimit: 100,
			},
		},
		{
			name:  "several topics",
			input: "04 04666f6f 00 04626172 00 0470617a 00 00000001 ff 00",
			want: describeTopicPartitionsRequest{
				topicNames:             []string{"foo", "bar", "paz"},
				responsePartitionLimit: 1,
			},
		},
		{
			name:  "no topics",
			input: "01 00000064 ff 00",
			want: describeTopicPartitionsRequest{
				responsePartitionLimit: 100,
			},
		},
		{
			name:  "with a cursor",
			input: "02 04666f6f 00 00000064 01 04666f6f 00000002 00 00",
			want: describeTopicPartitionsRequest{
				topicNames:             []string{"foo"},
				responsePartitionLimit: 100,
				cursor:                 &topicPartitionCursor{topicName: "foo", partitionIndex: 2},
			},
		},
		{
			name:  "tagged fields are skipped",
			input: "02 04666f6f 01 00 01 aa 00000064 ff 01 05 02 bbcc",
			want: describeTopicPartitionsRequest{
				topicNames:             []string{"foo"},
				responsePartitionLimit: 100,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newDecoder(mustDecodeHex(t, tt.input))

			got, err := decodeDescribeTopicPartitionsRequest(d)
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

	errorTests := []struct {
		name  string
		input string
	}{
		{name: "empty body", input: ""},
		{name: "null topics array", input: "00 00000064 ff 00"},
		{name: "fewer topics than announced", input: "03 04666f6f 00"},
		{name: "null topic name", input: "02 00 00 00000064 ff 00"},
		{name: "truncated topic name", input: "02 04666f"},
		{name: "missing response_partition_limit", input: "02 04666f6f 00"},
		{name: "truncated response_partition_limit", input: "02 04666f6f 00 0000"},
		{name: "missing cursor", input: "02 04666f6f 00 00000064"},
		{name: "truncated cursor", input: "02 04666f6f 00 00000064 01 04666f6f 0000"},
		{name: "missing tag buffer", input: "02 04666f6f 00 00000064 ff"},
	}

	for _, tt := range errorTests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := decodeDescribeTopicPartitionsRequest(newDecoder(mustDecodeHex(t, tt.input))); err == nil {
				t.Error("expected an error, got nil")
			}
		})
	}
}

func TestEncodeDescribeTopicPartitionsResponse(t *testing.T) {
	tests := []struct {
		name   string
		topics []topicDescription
		want   string
	}{
		{
			// The response body from the "List for an unknown topic" stage
			// instructions.
			name:   "unknown topic",
			topics: []topicDescription{{errorCode: 3, name: "foo"}},
			want: "00000000" + // throttle_time_ms: 0
				"02" + // topics array: 1 element
				"0003" + // error_code: 3 (UNKNOWN_TOPIC_OR_PARTITION)
				"04" + // name length: 3 (compact string)
				"666f6f" + // topic_name: "foo"
				"00000000000000000000000000000000" + // topic_id
				"00" + // is_internal: false
				"01" + // partitions array: 0 elements (empty)
				"00000000" + // topic_authorized_operations: 0
				"00" + // TAG_BUFFER: empty
				"ff" + // next_cursor: -1 (null)
				"00", // TAG_BUFFER: empty
		},
		{
			// The response body from the "List for a single partition" stage
			// instructions.
			name: "topic with one partition",
			topics: []topicDescription{{
				errorCode: 0,
				name:      "foo",
				id:        uuid{0xa1, 0xb2, 0xc3, 0xd4, 0xe5, 0xf6, 0xa7, 0xb8, 0xc9, 0xd0, 0xe1, 0xf2, 0xa3, 0xb4, 0xc5, 0xd6},
				partitions: []partitionMetadata{
					{index: 0, leader: 1, leaderEpoch: 0, replicas: []int32{1}, isr: []int32{1}},
				},
			}},
			want: "00000000" + // throttle_time_ms: 0
				"02" + // topics array: 1 element
				"0000" + // error_code: 0 (no error)
				"04" + // name length: 3 (compact string)
				"666f6f" + // name: "foo"
				"a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6" + // topic_id
				"00" + // is_internal: false
				"02" + // partitions array: 1 element
				"0000" + // error_code: 0 (no error)
				"00000000" + // partition_index: 0
				"00000001" + // leader_id: 1
				"00000000" + // leader_epoch: 0
				"02" + "00000001" + // replica_nodes: broker 1
				"02" + "00000001" + // isr_nodes: broker 1
				"01" + // eligible_leader_replicas: 0 elements (empty)
				"01" + // last_known_elr: 0 elements (empty)
				"01" + // offline_replicas: 0 elements (empty)
				"00" + // TAG_BUFFER: empty
				"00000000" + // topic_authorized_operations: 0
				"00" + // TAG_BUFFER: empty
				"ff" + // next_cursor: -1 (null)
				"00", // TAG_BUFFER: empty
		},
		{
			name: "partitions with several replicas",
			topics: []topicDescription{{
				name: "t",
				id:   testTopicID(1),
				partitions: []partitionMetadata{
					{index: 0, leader: 2, leaderEpoch: 7, replicas: []int32{2, 3, 1}, isr: []int32{2, 3}},
					{index: 1, leader: -1, leaderEpoch: 8},
				},
			}},
			want: "00000000 02" +
				"0000 02 74 00000000000040008000000000000001 00" +
				"03" +
				"0000 00000000 00000002 00000007 04 00000002 00000003 00000001 03 00000002 00000003 01 01 01 00" +
				"0000 00000001 ffffffff 00000008 01 01 01 01 01 00" +
				"00000000 00" +
				"ff 00",
		},
		{
			name: "several topics",
			topics: []topicDescription{
				{errorCode: 3, name: "a"},
				{errorCode: 3, name: "bc"},
			},
			want: "00000000 03" +
				"0003 02 61 00000000000000000000000000000000 00 01 00000000 00" +
				"0003 03 6263 00000000000000000000000000000000 00 01 00000000 00" +
				"ff 00",
		},
		{
			name:   "no topics",
			topics: nil,
			want:   "00000000 01 ff 00",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := encodeDescribeTopicPartitionsResponse(tt.topics)
			if want := mustDecodeHex(t, tt.want); !bytes.Equal(got, want) {
				t.Errorf("body = % x, want % x", got, want)
			}

			// The body must also be a valid DescribeTopicPartitions response.
			decoded := decodeDescribeTopicPartitionsResponse(t, got)
			if len(decoded.topics) != len(tt.topics) {
				t.Fatalf("decoded %d topics, want %d", len(decoded.topics), len(tt.topics))
			}
			for i, topic := range tt.topics {
				got := decoded.topics[i]
				if got.name != topic.name || got.errorCode != topic.errorCode || got.id != topic.id {
					t.Errorf("decoded topics[%d] = %+v, want %+v", i, got, topic)
				}
				if len(got.partitions) != len(topic.partitions) {
					t.Errorf("decoded topics[%d] has %d partitions, want %d", i, len(got.partitions), len(topic.partitions))
				}
			}
		})
	}
}

// expectUnknownTopic checks that topic is how the broker describes a topic
// that doesn't exist.
func expectUnknownTopic(t *testing.T, topic describedTopic, name string) {
	t.Helper()

	if topic.errorCode != 3 {
		t.Errorf("error_code = %d, want 3 (UNKNOWN_TOPIC_OR_PARTITION)", topic.errorCode)
	}
	if topic.name != name {
		t.Errorf("topic_name = %q, want %q", topic.name, name)
	}
	if topic.id != (uuid{}) {
		t.Errorf("topic_id = %v, want 00000000-0000-0000-0000-000000000000", topic.id)
	}
	if topic.isInternal {
		t.Error("is_internal = true, want false")
	}
	if len(topic.partitions) != 0 {
		t.Errorf("partitions = %+v, want none", topic.partitions)
	}
}

// Stage vt6: a DescribeTopicPartitions request for a topic that doesn't exist
// is answered with the error code UNKNOWN_TOPIC_OR_PARTITION for that topic.
func TestDescribeUnknownTopic(t *testing.T) {
	t.Run("example from the stage instructions", func(t *testing.T) {
		addr := startTestServer(t)
		conn := dial(t, addr)

		send(t, conn, mustDecodeHex(t, describeFooRequestHex))

		response := readResponse(t, conn)

		// The message_size field correctly represents the size of the header
		// and body.
		if want := int32(4 + len(response.body)); response.messageSize != want {
			t.Errorf("message_size = %d, want %d", response.messageSize, want)
		}

		// The correlation ID matches the one in the request header.
		if response.correlationID != 7 {
			t.Errorf("correlation_id = %d, want 7", response.correlationID)
		}

		// The response uses response header v1 (with TAG_BUFFER), and its
		// body is the one shown in the stage instructions.
		wantBody := mustDecodeHex(t, "00000000 02 0003 04666f6f 00000000000000000000000000000000 00 01 00000000 00 ff 00")
		if body := response.bodyAfterHeaderV1(t); !bytes.Equal(body, wantBody) {
			t.Errorf("response body = % x, want % x", body, wantBody)
		}

		expectNoMoreData(t, conn)
	})

	for _, name := range []string{"foo", "unknown-topic-saz", "a", "topic.with_dots-and_underscores"} {
		t.Run(name, func(t *testing.T) {
			addr := startTestServer(t)
			conn := dial(t, addr)

			send(t, conn, newDescribeTopicPartitionsRequest(1234, name))

			response := readResponse(t, conn)
			if want := int32(4 + len(response.body)); response.messageSize != want {
				t.Errorf("message_size = %d, want %d", response.messageSize, want)
			}
			if response.correlationID != 1234 {
				t.Errorf("correlation_id = %d, want 1234", response.correlationID)
			}

			// The response body is a valid DescribeTopicPartitions (v0)
			// response.
			body := decodeDescribeTopicPartitionsResponse(t, response.bodyAfterHeaderV1(t))
			if body.throttleTimeMs != 0 {
				t.Errorf("throttle_time_ms = %d, want 0", body.throttleTimeMs)
			}
			if len(body.topics) != 1 {
				t.Fatalf("got %d topics, want 1", len(body.topics))
			}
			expectUnknownTopic(t, body.topics[0], name)

			// The cursor is null.
			if body.nextCursor != nil {
				t.Errorf("next_cursor = %+v, want null", *body.nextCursor)
			}

			expectNoMoreData(t, conn)
		})
	}
}

// describeTopics sends a DescribeTopicPartitions request for the given topics
// over a new connection to the broker at addr, and returns the decoded
// response after checking its message_size, correlation ID and header.
func describeTopics(t *testing.T, addr string, topicNames ...string) describeTopicPartitionsResponse {
	t.Helper()

	const correlationID = 1915044735

	conn := dial(t, addr)
	send(t, conn, newDescribeTopicPartitionsRequest(correlationID, topicNames...))

	response := readResponse(t, conn)
	if want := int32(4 + len(response.body)); response.messageSize != want {
		t.Errorf("message_size = %d, want %d", response.messageSize, want)
	}
	if response.correlationID != correlationID {
		t.Errorf("correlation_id = %d, want %d", response.correlationID, correlationID)
	}

	body := decodeDescribeTopicPartitionsResponse(t, response.bodyAfterHeaderV1(t))
	if body.throttleTimeMs != 0 {
		t.Errorf("throttle_time_ms = %d, want 0", body.throttleTimeMs)
	}
	if body.nextCursor != nil {
		t.Errorf("next_cursor = %+v, want null", *body.nextCursor)
	}

	expectNoMoreData(t, conn)

	return body
}

// expectKnownTopic checks that topic is how the broker describes an existing
// topic with the given ID and partitions, each of which is led by broker 1,
// its only replica (which is how the test fixtures create partitions).
func expectKnownTopic(t *testing.T, topic describedTopic, name string, id uuid, partitions ...int32) {
	t.Helper()

	if topic.errorCode != 0 {
		t.Errorf("topic %q: error_code = %d, want 0", name, topic.errorCode)
	}
	if topic.name != name {
		t.Errorf("topic_name = %q, want %q", topic.name, name)
	}
	if topic.id != id {
		t.Errorf("topic %q: topic_id = %v, want %v", name, topic.id, id)
	}
	if topic.isInternal {
		t.Errorf("topic %q: is_internal = true, want false", name)
	}

	if len(topic.partitions) != len(partitions) {
		t.Fatalf("topic %q: got %d partitions, want %d", name, len(topic.partitions), len(partitions))
	}
	for i, index := range partitions {
		want := describedPartition{
			errorCode:      0,
			partitionIndex: index,
			leaderID:       1,
			leaderEpoch:    0,
			replicaNodes:   []int32{1},
			isrNodes:       []int32{1},
		}
		if got := topic.partitions[i]; !reflect.DeepEqual(got, want) {
			t.Errorf("topic %q: partitions[%d] = %+v, want %+v", name, i, got, want)
		}
	}
}

// Stage ea7: a DescribeTopicPartitions request for a topic that exists with a
// single partition is answered with the ID of the topic and its partition,
// both read from the cluster metadata log.
func TestDescribeTopicWithSinglePartition(t *testing.T) {
	logDir := t.TempDir()
	writeClusterMetadataLog(t, logDir,
		testTopic{name: "bar", id: testTopicID(0x80), partitions: []int32{0}},
		testTopic{name: "foo", id: testTopicID(0x81), partitions: []int32{0}},
		testTopic{name: "paz", id: testTopicID(0x82), partitions: []int32{0, 1}},
	)
	addr := startTestServerWithLogDir(t, logDir)

	t.Run("topic with a single partition", func(t *testing.T) {
		body := describeTopics(t, addr, "foo")

		if len(body.topics) != 1 {
			t.Fatalf("got %d topics, want 1", len(body.topics))
		}
		expectKnownTopic(t, body.topics[0], "foo", testTopicID(0x81), 0)
	})

	t.Run("another topic of the same log", func(t *testing.T) {
		body := describeTopics(t, addr, "bar")

		if len(body.topics) != 1 {
			t.Fatalf("got %d topics, want 1", len(body.topics))
		}
		expectKnownTopic(t, body.topics[0], "bar", testTopicID(0x80), 0)
	})

	t.Run("response bytes", func(t *testing.T) {
		conn := dial(t, addr)
		send(t, conn, newDescribeTopicPartitionsRequest(7, "foo"))

		// The response from the stage instructions, with the topic ID of
		// the cluster metadata log. (The instructions announce 74 bytes, but
		// the header and body they show add up to 69.)
		want := "00000045" + // message_size: 69 bytes
			"00000007" + // correlation_id
			"00" + // TAG_BUFFER: empty (response header v1)
			"00000000" + // throttle_time_ms: 0
			"02" + // topics array: 1 element
			"0000" + // error_code: 0 (no error)
			"04666f6f" + // name: "foo"
			"00000000000040008000000000000081" + // topic_id
			"00" + // is_internal: false
			"02" + // partitions array: 1 element
			"0000" + // error_code: 0 (no error)
			"00000000" + // partition_index: 0
			"00000001" + // leader_id: 1
			"00000000" + // leader_epoch: 0
			"0200000001" + // replica_nodes: broker 1
			"0200000001" + // isr_nodes: broker 1
			"01" + // eligible_leader_replicas: empty
			"01" + // last_known_elr: empty
			"01" + // offline_replicas: empty
			"00" + // TAG_BUFFER: empty
			"00000000" + // topic_authorized_operations: 0
			"00" + // TAG_BUFFER: empty
			"ff" + // next_cursor: -1 (null)
			"00" // TAG_BUFFER: empty

		got := make([]byte, len(want)/2)
		if _, err := io.ReadFull(conn, got); err != nil {
			t.Fatalf("failed to read response: %v", err)
		}
		if !bytes.Equal(got, mustDecodeHex(t, want)) {
			t.Errorf("response = % x, want % x", got, mustDecodeHex(t, want))
		}

		expectNoMoreData(t, conn)
	})

	t.Run("unknown topic next to existing ones", func(t *testing.T) {
		body := describeTopics(t, addr, "unknown")

		if len(body.topics) != 1 {
			t.Fatalf("got %d topics, want 1", len(body.topics))
		}
		expectUnknownTopic(t, body.topics[0], "unknown")
	})

	t.Run("topic names are case-sensitive", func(t *testing.T) {
		body := describeTopics(t, addr, "FOO")

		if len(body.topics) != 1 {
			t.Fatalf("got %d topics, want 1", len(body.topics))
		}
		expectUnknownTopic(t, body.topics[0], "FOO")
	})
}

// Stage ku4: a topic with several partitions is described with one entry per
// partition, each with its own partition index.
func TestDescribeTopicWithMultiplePartitions(t *testing.T) {
	logDir := t.TempDir()
	writeClusterMetadataLog(t, logDir,
		testTopic{name: "one", id: testTopicID(0x10), partitions: []int32{0}},
		testTopic{name: "two", id: testTopicID(0x20), partitions: []int32{0, 1}},
		testTopic{name: "three", id: testTopicID(0x30), partitions: []int32{0, 1, 2}},
		testTopic{name: "unordered", id: testTopicID(0x40), partitions: []int32{3, 1, 0, 2}},
		testTopic{name: "none", id: testTopicID(0x50)},
	)
	addr := startTestServerWithLogDir(t, logDir)

	tests := []struct {
		topic      string
		id         uuid
		partitions []int32
	}{
		{topic: "two", id: testTopicID(0x20), partitions: []int32{0, 1}},
		{topic: "three", id: testTopicID(0x30), partitions: []int32{0, 1, 2}},

		// Partitions are listed by index, whatever their order in the log.
		{topic: "unordered", id: testTopicID(0x40), partitions: []int32{0, 1, 2, 3}},

		// The neighbours of the topics above keep their own partitions.
		{topic: "one", id: testTopicID(0x10), partitions: []int32{0}},
		{topic: "none", id: testTopicID(0x50), partitions: nil},
	}

	for _, tt := range tests {
		t.Run(tt.topic, func(t *testing.T) {
			body := describeTopics(t, addr, tt.topic)

			if len(body.topics) != 1 {
				t.Fatalf("got %d topics, want 1", len(body.topics))
			}
			expectKnownTopic(t, body.topics[0], tt.topic, tt.id, tt.partitions...)
		})
	}

	t.Run("response bytes", func(t *testing.T) {
		conn := dial(t, addr)
		send(t, conn, newDescribeTopicPartitionsRequest(7, "two"))

		want := "00000061" + // message_size: 97 bytes
			"00000007" + // correlation_id
			"00" + // TAG_BUFFER: empty (response header v1)
			"00000000" + // throttle_time_ms: 0
			"02" + // topics array: 1 element
			"0000" + // error_code: 0 (no error)
			"0474776f" + // name: "two"
			"00000000000040008000000000000020" + // topic_id
			"00" + // is_internal: false
			"03" + // partitions array: 2 elements

			"0000" + // error_code: 0 (no error)
			"00000000" + // partition_index: 0
			"00000001" + // leader_id: 1
			"00000000" + // leader_epoch: 0
			"0200000001" + // replica_nodes: broker 1
			"0200000001" + // isr_nodes: broker 1
			"01" + // eligible_leader_replicas: empty
			"01" + // last_known_elr: empty
			"01" + // offline_replicas: empty
			"00" + // TAG_BUFFER: empty

			"0000" + // error_code: 0 (no error)
			"00000001" + // partition_index: 1
			"00000001" + // leader_id: 1
			"00000000" + // leader_epoch: 0
			"0200000001" + // replica_nodes: broker 1
			"0200000001" + // isr_nodes: broker 1
			"01" + // eligible_leader_replicas: empty
			"01" + // last_known_elr: empty
			"01" + // offline_replicas: empty
			"00" + // TAG_BUFFER: empty

			"00000000" + // topic_authorized_operations: 0
			"00" + // TAG_BUFFER: empty
			"ff" + // next_cursor: -1 (null)
			"00" // TAG_BUFFER: empty

		got := make([]byte, len(want)/2)
		if _, err := io.ReadFull(conn, got); err != nil {
			t.Fatalf("failed to read response: %v", err)
		}
		if !bytes.Equal(got, mustDecodeHex(t, want)) {
			t.Errorf("response = % x, want % x", got, mustDecodeHex(t, want))
		}

		expectNoMoreData(t, conn)
	})
}

// Stage wq2: a request for several topics is answered with an entry for each
// of them, sorted alphabetically by topic name.
func TestDescribeMultipleTopics(t *testing.T) {
	logDir := t.TempDir()
	writeClusterMetadataLog(t, logDir,
		testTopic{name: "zebra", id: testTopicID(0x01), partitions: []int32{0, 1}},
		testTopic{name: "apple", id: testTopicID(0x02), partitions: []int32{0}},
		testTopic{name: "mango", id: testTopicID(0x03), partitions: []int32{0, 1}},
		testTopic{name: "Zebra", id: testTopicID(0x04), partitions: []int32{0}},
		testTopic{name: "apple-2", id: testTopicID(0x05), partitions: []int32{0}},
	)
	addr := startTestServerWithLogDir(t, logDir)

	// expected describes a topic of the response: a nil ID stands for a topic
	// that the broker doesn't know.
	type expected struct {
		name       string
		id         *uuid
		partitions []int32
	}
	known := func(name string, id byte, partitions ...int32) expected {
		topicID := testTopicID(id)
		return expected{name: name, id: &topicID, partitions: partitions}
	}
	unknown := func(name string) expected {
		return expected{name: name}
	}

	tests := []struct {
		name    string
		request []string
		want    []expected
	}{
		{
			name:    "example from the stage instructions",
			request: []string{"zebra", "apple"},
			want:    []expected{known("apple", 0x02, 0), known("zebra", 0x01, 0, 1)},
		},
		{
			name:    "already sorted",
			request: []string{"apple", "zebra"},
			want:    []expected{known("apple", 0x02, 0), known("zebra", 0x01, 0, 1)},
		},
		{
			name:    "three topics",
			request: []string{"zebra", "mango", "apple"},
			want:    []expected{known("apple", 0x02, 0), known("mango", 0x03, 0, 1), known("zebra", 0x01, 0, 1)},
		},
		{
			name:    "single topic",
			request: []string{"mango"},
			want:    []expected{known("mango", 0x03, 0, 1)},
		},
		{
			name:    "uppercase letters sort before lowercase ones",
			request: []string{"zebra", "apple", "Zebra"},
			want:    []expected{known("Zebra", 0x04, 0), known("apple", 0x02, 0), known("zebra", 0x01, 0, 1)},
		},
		{
			name:    "a name sorts before the names it is a prefix of",
			request: []string{"apple-2", "apple"},
			want:    []expected{known("apple", 0x02, 0), known("apple-2", 0x05, 0)},
		},
		{
			name:    "unknown topics are sorted along with the known ones",
			request: []string{"zebra", "nope", "apple", "aaa"},
			want:    []expected{unknown("aaa"), known("apple", 0x02, 0), unknown("nope"), known("zebra", 0x01, 0, 1)},
		},
		{
			name:    "only unknown topics",
			request: []string{"y", "x"},
			want:    []expected{unknown("x"), unknown("y")},
		},
		{
			name:    "a topic requested several times is described once",
			request: []string{"zebra", "apple", "zebra", "apple", "zebra"},
			want:    []expected{known("apple", 0x02, 0), known("zebra", 0x01, 0, 1)},
		},
		{
			name:    "no topics",
			request: nil,
			want:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := describeTopics(t, addr, tt.request...)

			var names []string
			for _, topic := range body.topics {
				names = append(names, topic.name)
			}
			var wantNames []string
			for _, topic := range tt.want {
				wantNames = append(wantNames, topic.name)
			}
			if !reflect.DeepEqual(names, wantNames) {
				t.Fatalf("topics = %q, want %q", names, wantNames)
			}

			for i, want := range tt.want {
				if want.id == nil {
					expectUnknownTopic(t, body.topics[i], want.name)
				} else {
					expectKnownTopic(t, body.topics[i], want.name, *want.id, want.partitions...)
				}
			}
		})
	}
}

// The cluster metadata log of the CodeCrafters tester, as documented in the
// protocol inspector linked from the stage instructions.
func TestDescribeTopicOfCodeCraftersLog(t *testing.T) {
	logDir := t.TempDir()
	writeLogSegment(t, logDir, "__cluster_metadata", 0, mustDecodeHex(t, featureLevelBatchHex+sazTopicBatchHex))
	addr := startTestServerWithLogDir(t, logDir)

	body := describeTopics(t, addr, "saz")

	if len(body.topics) != 1 {
		t.Fatalf("got %d topics, want 1", len(body.topics))
	}
	expectKnownTopic(t, body.topics[0], "saz", sazTopicID, 1)
}

// A cluster metadata log that can't be read is not mistaken for a cluster
// without topics: the request fails, and the connection is closed.
func TestDescribeTopicWithCorruptMetadataLog(t *testing.T) {
	logDir := t.TempDir()
	writeLogSegment(t, logDir, "__cluster_metadata", 0, mustDecodeHex(t, sazTopicBatchHex)[:100])
	addr := startTestServerWithLogDir(t, logDir)

	conn := dial(t, addr)
	send(t, conn, newDescribeTopicPartitionsRequest(1, "saz"))

	expectConnectionClosed(t, conn)
}

// ApiVersions and DescribeTopicPartitions requests can be mixed on the same
// connection. Each response uses the header version of its own API: v0 for
// ApiVersions, v1 for DescribeTopicPartitions.
func TestMixedRequestsOnOneConnection(t *testing.T) {
	addr := startTestServer(t)
	conn := dial(t, addr)

	send(t, conn, newApiVersionsRequest(1))
	expectApiVersionsResponse(t, conn, 1)

	send(t, conn, newDescribeTopicPartitionsRequest(2, "foo"))
	response := readResponse(t, conn)
	if response.correlationID != 2 {
		t.Errorf("correlation_id = %d, want 2", response.correlationID)
	}
	body := decodeDescribeTopicPartitionsResponse(t, response.bodyAfterHeaderV1(t))
	if len(body.topics) != 1 {
		t.Fatalf("got %d topics, want 1", len(body.topics))
	}
	expectUnknownTopic(t, body.topics[0], "foo")

	send(t, conn, newApiVersionsRequest(3))
	expectApiVersionsResponse(t, conn, 3)

	expectNoMoreData(t, conn)
}

// The broker closes the connection, without a response, when it gets a
// request that it doesn't support or whose body it can't parse.
func TestUnsupportedRequestClosesConnection(t *testing.T) {
	tests := []struct {
		name    string
		request []byte
	}{
		{
			name:    "unknown API key",
			request: encodeRequest(9999, 0, 1, nil),
		},
		{
			name:    "negative API key",
			request: encodeRequest(-1, 0, 1, nil),
		},
		{
			name:    "unsupported DescribeTopicPartitions version",
			request: encodeRequest(75, 1, 1, mustDecodeHex(t, "02 04666f6f 00 00000064 ff 00")),
		},
		{
			name:    "DescribeTopicPartitions without a body",
			request: encodeRequest(75, 0, 1, nil),
		},
		{
			name:    "truncated DescribeTopicPartitions body",
			request: encodeRequest(75, 0, 1, mustDecodeHex(t, "02 04666f6f 00 0000")),
		},
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
