package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
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
				if decoded.topics[i].name != topic.name || decoded.topics[i].errorCode != topic.errorCode {
					t.Errorf("decoded topics[%d] = %+v, want %+v", i, decoded.topics[i], topic)
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

			response := make([]byte, 64)
			n, err := conn.Read(response)
			if n > 0 {
				t.Fatalf("expected no response, got % x", response[:n])
			}
			if err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("expected the connection to be closed, got error: %v", err)
			}
		})
	}
}
