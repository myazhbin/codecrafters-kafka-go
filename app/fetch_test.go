package main

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"
)

// newFetchRequest builds a Fetch (v16) request for the given topics, like the
// one the CodeCrafters tester sends.
func newFetchRequest(correlationID int32, topics ...fetchTopic) []byte {
	var body []byte
	body = binary.BigEndian.AppendUint32(body, 500)      // max_wait_ms
	body = binary.BigEndian.AppendUint32(body, 1)        // min_bytes
	body = binary.BigEndian.AppendUint32(body, 52428800) // max_bytes
	body = append(body, 0)                               // isolation_level
	body = binary.BigEndian.AppendUint32(body, 0)        // session_id
	body = binary.BigEndian.AppendUint32(body, 0)        // session_epoch

	body = binary.AppendUvarint(body, uint64(len(topics)+1)) // topics
	for _, topic := range topics {
		body = append(body, topic.topicID[:]...)

		body = binary.AppendUvarint(body, uint64(len(topic.partitions)+1)) // partitions
		for _, partition := range topic.partitions {
			body = binary.BigEndian.AppendUint32(body, uint32(partition.index))
			body = binary.BigEndian.AppendUint32(body, 0xffffffff) // current_leader_epoch: -1
			body = binary.BigEndian.AppendUint64(body, uint64(partition.fetchOffset))
			body = binary.BigEndian.AppendUint32(body, 0xffffffff)         // last_fetched_epoch: -1
			body = binary.BigEndian.AppendUint64(body, 0xffffffffffffffff) // log_start_offset: -1
			body = binary.BigEndian.AppendUint32(body, 1048576)            // partition_max_bytes
			body = append(body, 0)                                         // tag buffer
		}
		body = append(body, 0) // tag buffer
	}

	body = append(body, 1) // forgotten_topics_data: empty
	body = append(body, 1) // rack_id: ""
	body = append(body, 0) // tag buffer

	return encodeRequest(1, 16, correlationID, body)
}

// fetchResponse is a decoded Fetch response body (v16).
type fetchResponse struct {
	throttleTimeMs int32
	errorCode      int16
	sessionID      int32
	topics         []fetchedTopic
}

type fetchedTopic struct {
	topicID    uuid
	partitions []fetchedPartition
}

type fetchedPartition struct {
	index                int32
	errorCode            int16
	highWatermark        int64
	lastStableOffset     int64
	logStartOffset       int64
	abortedTransactions  int // number of entries; -1 for a null array
	preferredReadReplica int32
	records              []byte // nil for null records
}

// decodeFetchResponse decodes a Fetch response body (v16) and fails the test
// if the body is malformed or followed by extra bytes.
func decodeFetchResponse(t *testing.T, body []byte) fetchResponse {
	t.Helper()

	d := newDecoder(body)

	response := fetchResponse{
		throttleTimeMs: d.int32(),
		errorCode:      d.int16(),
		sessionID:      d.int32(),
	}

	for i, n := 0, d.compactArrayLength(); i < n && d.err == nil; i++ {
		topic := fetchedTopic{topicID: d.uuid()}

		for j, m := 0, d.compactArrayLength(); j < m && d.err == nil; j++ {
			partition := fetchedPartition{
				index:            d.int32(),
				errorCode:        d.int16(),
				highWatermark:    d.int64(),
				lastStableOffset: d.int64(),
				logStartOffset:   d.int64(),
			}

			// aborted_transactions is a nullable array: a length of 0 stands
			// for null, a length of n + 1 for n entries.
			partition.abortedTransactions = int(d.uvarint()) - 1
			for k := 0; k < partition.abortedTransactions && d.err == nil; k++ {
				d.int64() // producer_id
				d.int64() // first_offset
				d.taggedFields()
			}

			partition.preferredReadReplica = d.int32()

			// records are COMPACT_RECORDS: a length of 0 stands for null, a
			// length of n + 1 for n bytes of record batches.
			if n := d.uvarint(); n > 0 {
				partition.records = bytes.Clone(d.bytes(int(n - 1)))
				if partition.records == nil {
					partition.records = []byte{}
				}
			}
			d.taggedFields()

			topic.partitions = append(topic.partitions, partition)
		}
		d.taggedFields()

		response.topics = append(response.topics, topic)
	}
	d.taggedFields()

	if d.err != nil {
		t.Fatalf("failed to decode Fetch response body % x: %v", body, d.err)
	}
	if len(d.buf) != 0 {
		t.Fatalf("%d extra bytes after the Fetch response body: % x", len(d.buf), d.buf)
	}

	return response
}

// fetch sends a Fetch request for the given topics over a new connection to
// the broker at addr, and returns the decoded response after checking its
// message_size, correlation ID and header.
func fetch(t *testing.T, addr string, topics ...fetchTopic) fetchResponse {
	t.Helper()

	const correlationID = 1497788063

	conn := dial(t, addr)
	send(t, conn, newFetchRequest(correlationID, topics...))

	response := readResponse(t, conn)
	if want := int32(4 + len(response.body)); response.messageSize != want {
		t.Errorf("message_size = %d, want %d", response.messageSize, want)
	}
	if response.correlationID != correlationID {
		t.Errorf("correlation_id = %d, want %d", response.correlationID, correlationID)
	}

	body := decodeFetchResponse(t, response.bodyAfterHeaderV1(t))

	expectNoMoreData(t, conn)

	return body
}

func TestDecodeFetchRequest(t *testing.T) {
	// The fields that come before the topics of a Fetch request.
	const preamble = "000001f4" + // max_wait_ms: 500
		"00000001" + // min_bytes: 1
		"03200000" + // max_bytes: 52428800
		"00" + // isolation_level: 0
		"00000000" + // session_id: 0
		"00000000" // session_epoch: 0

	// A partition of a topic of a Fetch request, without its index.
	const partitionFields = "ffffffff" + // current_leader_epoch: -1
		"0000000000000000" + // fetch_offset: 0
		"ffffffff" + // last_fetched_epoch: -1
		"ffffffffffffffff" + // log_start_offset: -1
		"00100000" + // partition_max_bytes: 1048576
		"00" // tag buffer

	tests := []struct {
		name  string
		input string
		want  fetchRequest
	}{
		{
			name: "no topics",
			input: preamble +
				"01" + // topics: empty
				"01" + // forgotten_topics_data: empty
				"01" + // rack_id: ""
				"00", // tag buffer
			want: fetchRequest{},
		},
		{
			name: "one topic with one partition",
			input: preamble +
				"02" + // topics: 1 element
				"00000000000040008000000000000091" + // topic_id
				"02" + // partitions: 1 element
				"00000000" + partitionFields + // partition 0
				"00" + // tag buffer
				"01 01 00",
			want: fetchRequest{topics: []fetchTopic{
				{topicID: sazTopicID, partitions: []fetchPartition{{index: 0, fetchOffset: 0}}},
			}},
		},
		{
			name: "fetch offset",
			input: preamble +
				"02" +
				"00000000000040008000000000000091" +
				"02" +
				"00000003" + // partition: 3
				"00000005" + // current_leader_epoch: 5
				"0000000100000002" + // fetch_offset: 4294967298
				"00000004" + // last_fetched_epoch: 4
				"0000000000000001" + // log_start_offset: 1
				"00000400" + // partition_max_bytes: 1024
				"00" +
				"00" +
				"01 01 00",
			want: fetchRequest{topics: []fetchTopic{
				{topicID: sazTopicID, partitions: []fetchPartition{{index: 3, fetchOffset: 4294967298}}},
			}},
		},
		{
			name: "several topics and partitions",
			input: preamble +
				"03" + // topics: 2 elements
				"00000000000040008000000000000001" +
				"03" + // partitions: 2 elements
				"00000000" + partitionFields +
				"00000001" + partitionFields +
				"00" +
				"00000000000040008000000000000002" +
				"02" + // partitions: 1 element
				"00000007" + partitionFields +
				"00" +
				"01 01 00",
			want: fetchRequest{topics: []fetchTopic{
				{topicID: testTopicID(1), partitions: []fetchPartition{{index: 0}, {index: 1}}},
				{topicID: testTopicID(2), partitions: []fetchPartition{{index: 7}}},
			}},
		},
		{
			name: "topic without partitions",
			input: preamble +
				"02 00000000000040008000000000000001 01 00" +
				"01 01 00",
			want: fetchRequest{topics: []fetchTopic{{topicID: testTopicID(1)}}},
		},
		{
			name: "forgotten topics, rack ID and tagged fields are skipped",
			input: preamble +
				"01" + // topics: empty
				"03" + // forgotten_topics_data: 2 elements
				"00000000000040008000000000000001 03 00000000 00000001 00" +
				"00000000000040008000000000000002 01 00" +
				"07 7261636b2d31" + // rack_id: "rack-1"
				"02" + // tagged fields: 2
				"00 04 03616263" + // tag 0 (cluster ID): "abc"
				"01 0d 00000001 0000000000000002 00", // tag 1 (replica state)
			want: fetchRequest{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newDecoder(mustDecodeHex(t, tt.input))

			got, err := decodeFetchRequest(d)
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
	t.Run("request built by newFetchRequest", func(t *testing.T) {
		want := fetchRequest{topics: []fetchTopic{
			{topicID: testTopicID(1), partitions: []fetchPartition{{index: 0, fetchOffset: 5}, {index: 2}}},
			{topicID: testTopicID(2)},
		}}

		d := newDecoder(newFetchRequest(1, want.topics...)[4:])
		if _, err := decodeRequestHeader(d); err != nil {
			t.Fatalf("failed to decode request header: %v", err)
		}

		got, err := decodeFetchRequest(d)
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
		{name: "truncated preamble", input: "000001f4 00000001 0320"},
		{name: "missing topics", input: preamble},
		{name: "null topics", input: preamble + "00 01 01 00"},
		{name: "truncated topic ID", input: preamble + "02 0000000000004000"},
		{name: "missing partitions", input: preamble + "02 00000000000040008000000000000001"},
		{name: "truncated partition", input: preamble + "02 00000000000040008000000000000001 02 00000000 ffffffff 0000"},
		{name: "fewer topics than announced", input: preamble + "03 00000000000040008000000000000001 01 00 01 01 00"},
		{name: "missing forgotten topics", input: preamble + "01"},
		{name: "missing rack ID", input: preamble + "01 01"},
		{name: "null rack ID", input: preamble + "01 01 00 00"},
		{name: "missing tag buffer", input: preamble + "01 01 01"},
	}

	for _, tt := range errorTests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := decodeFetchRequest(newDecoder(mustDecodeHex(t, tt.input))); err == nil {
				t.Error("expected an error, got nil")
			}
		})
	}
}

func TestEncodeFetchResponse(t *testing.T) {
	tests := []struct {
		name   string
		topics []fetchTopicResponse
		want   string
	}{
		{
			name:   "no topics",
			topics: nil,
			want: "00000000" + // throttle_time_ms: 0
				"0000" + // error_code: 0
				"00000000" + // session_id: 0
				"01" + // responses: empty
				"00", // tag buffer
		},
		{
			name: "unknown topic",
			topics: []fetchTopicResponse{{
				topicID:    sazTopicID,
				partitions: []fetchPartitionResponse{fetchError(0, 100)},
			}},
			want: "00000000" + // throttle_time_ms: 0
				"0000" + // error_code: 0
				"00000000" + // session_id: 0
				"02" + // responses: 1 element
				"00000000000040008000000000000091" + // topic_id
				"02" + // partitions: 1 element
				"00000000" + // partition_index: 0
				"0064" + // error_code: 100 (UNKNOWN_TOPIC_ID)
				"ffffffffffffffff" + // high_watermark: -1
				"ffffffffffffffff" + // last_stable_offset: -1
				"ffffffffffffffff" + // log_start_offset: -1
				"01" + // aborted_transactions: empty
				"ffffffff" + // preferred_read_replica: -1
				"01" + // records: empty
				"00" + // tag buffer
				"00" + // tag buffer
				"00", // tag buffer
		},
		{
			name: "empty partition",
			topics: []fetchTopicResponse{{
				topicID:    sazTopicID,
				partitions: []fetchPartitionResponse{{index: 0}},
			}},
			want: "00000000" + // throttle_time_ms: 0
				"0000" + // error_code: 0
				"00000000" + // session_id: 0
				"02" + // responses: 1 element
				"00000000000040008000000000000091" + // topic_id
				"02" + // partitions: 1 element
				"00000000" + // partition_index: 0
				"0000" + // error_code: 0
				"0000000000000000" + // high_watermark: 0
				"0000000000000000" + // last_stable_offset: 0
				"0000000000000000" + // log_start_offset: 0
				"01" + // aborted_transactions: empty
				"ffffffff" + // preferred_read_replica: -1
				"01" + // records: empty
				"00" + // tag buffer
				"00" + // tag buffer
				"00", // tag buffer
		},
		{
			name: "partition with records",
			topics: []fetchTopicResponse{{
				topicID: sazTopicID,
				partitions: []fetchPartitionResponse{{
					index:          2,
					highWatermark:  7,
					logStartOffset: 3,
					records:        []byte{0xde, 0xad, 0xbe, 0xef},
				}},
			}},
			want: "00000000 0000 00000000" +
				"02" + // responses: 1 element
				"00000000000040008000000000000091" + // topic_id
				"02" + // partitions: 1 element
				"00000002" + // partition_index: 2
				"0000" + // error_code: 0
				"0000000000000007" + // high_watermark: 7
				"0000000000000007" + // last_stable_offset: 7
				"0000000000000003" + // log_start_offset: 3
				"01" + // aborted_transactions: empty
				"ffffffff" + // preferred_read_replica: -1
				"05 deadbeef" + // records: 4 bytes
				"00 00 00",
		},
		{
			name: "several topics and partitions",
			topics: []fetchTopicResponse{
				{
					topicID: testTopicID(1),
					partitions: []fetchPartitionResponse{
						fetchError(0, 100),
						fetchError(5, 100),
					},
				},
				{topicID: testTopicID(2)},
			},
			want: "00000000 0000 00000000" +
				"03" +
				"00000000000040008000000000000001" +
				"03" +
				"00000000 0064 ffffffffffffffff ffffffffffffffff ffffffffffffffff 01 ffffffff 01 00" +
				"00000005 0064 ffffffffffffffff ffffffffffffffff ffffffffffffffff 01 ffffffff 01 00" +
				"00" +
				"00000000000040008000000000000002" +
				"01" +
				"00" +
				"00",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := encodeFetchResponse(tt.topics)
			if want := mustDecodeHex(t, tt.want); !bytes.Equal(got, want) {
				t.Errorf("body = % x, want % x", got, want)
			}

			// The body must also be a valid Fetch response.
			decoded := decodeFetchResponse(t, got)
			if len(decoded.topics) != len(tt.topics) {
				t.Fatalf("decoded %d topics, want %d", len(decoded.topics), len(tt.topics))
			}
			for i, topic := range tt.topics {
				if decoded.topics[i].topicID != topic.topicID || len(decoded.topics[i].partitions) != len(topic.partitions) {
					t.Errorf("decoded topics[%d] = %+v, want %+v", i, decoded.topics[i], topic)
				}
			}
		})
	}
}

// Stage dh6: a Fetch request with an empty array of topics is answered with
// an empty array of responses.
func TestFetchWithNoTopics(t *testing.T) {
	addr := startTestServer(t)

	t.Run("response fields", func(t *testing.T) {
		body := fetch(t, addr)

		// The error code is 0 (No Error).
		if body.errorCode != 0 {
			t.Errorf("error_code = %d, want 0", body.errorCode)
		}

		// The throttle_time_ms field is 0.
		if body.throttleTimeMs != 0 {
			t.Errorf("throttle_time_ms = %d, want 0", body.throttleTimeMs)
		}

		if body.sessionID != 0 {
			t.Errorf("session_id = %d, want 0", body.sessionID)
		}

		// The responses field has 0 elements.
		if len(body.topics) != 0 {
			t.Errorf("responses has %d elements, want 0", len(body.topics))
		}
	})

	t.Run("response bytes", func(t *testing.T) {
		conn := dial(t, addr)
		send(t, conn, newFetchRequest(7))

		response := readResponse(t, conn)

		want := mustDecodeHex(t, "00"+ // TAG_BUFFER: empty (response header v1)
			"00000000"+ // throttle_time_ms: 0
			"0000"+ // error_code: 0
			"00000000"+ // session_id: 0
			"01"+ // responses: empty
			"00") // tag buffer
		if response.messageSize != 17 || response.correlationID != 7 || !bytes.Equal(response.body, want) {
			t.Errorf("response = {message_size: %d, correlation_id: %d, body: % x}, want {17, 7, % x}",
				response.messageSize, response.correlationID, response.body, want)
		}

		expectNoMoreData(t, conn)
	})

	t.Run("mixed with other requests on one connection", func(t *testing.T) {
		conn := dial(t, addr)

		send(t, conn, newApiVersionsRequest(1))
		expectApiVersionsResponse(t, conn, 1)

		send(t, conn, newFetchRequest(2))
		response := readResponse(t, conn)
		if response.correlationID != 2 {
			t.Errorf("correlation_id = %d, want 2", response.correlationID)
		}
		if body := decodeFetchResponse(t, response.bodyAfterHeaderV1(t)); body.errorCode != 0 || len(body.topics) != 0 {
			t.Errorf("unexpected Fetch response: %+v", body)
		}

		send(t, conn, newApiVersionsRequest(3))
		expectApiVersionsResponse(t, conn, 3)
	})
}

// Stage hn6: a Fetch request for a topic ID that doesn't exist is answered
// with the error code UNKNOWN_TOPIC_ID for the requested partition.
func TestFetchUnknownTopic(t *testing.T) {
	unknownTopicID := uuid{0x71, 0xa5, 0x9a, 0x51, 0x89, 0x68, 0x4f, 0x8b, 0x93, 0x7e, 0x00, 0x00, 0x00, 0x00, 0x05, 0x47}

	t.Run("single topic", func(t *testing.T) {
		addr := startTestServer(t)

		body := fetch(t, addr, fetchTopic{topicID: unknownTopicID, partitions: []fetchPartition{{index: 0}}})

		// The error code of the response is 0 (No Error), its
		// throttle_time_ms is 0.
		if body.errorCode != 0 {
			t.Errorf("error_code = %d, want 0", body.errorCode)
		}
		if body.throttleTimeMs != 0 {
			t.Errorf("throttle_time_ms = %d, want 0", body.throttleTimeMs)
		}

		// The responses field has 1 element, whose topic_id matches what was
		// sent in the request.
		if len(body.topics) != 1 {
			t.Fatalf("responses has %d elements, want 1", len(body.topics))
		}
		topic := body.topics[0]
		if topic.topicID != unknownTopicID {
			t.Errorf("topic_id = %v, want %v", topic.topicID, unknownTopicID)
		}

		// The partitions array has 1 element, whose partition_index is 0 and
		// whose error_code is 100 (UNKNOWN_TOPIC_ID).
		if len(topic.partitions) != 1 {
			t.Fatalf("partitions has %d elements, want 1", len(topic.partitions))
		}
		expectFetchError(t, topic.partitions[0], 0, 100)
	})

	t.Run("several topics and partitions", func(t *testing.T) {
		addr := startTestServer(t)

		request := []fetchTopic{
			{topicID: testTopicID(2), partitions: []fetchPartition{{index: 0}, {index: 3}}},
			{topicID: testTopicID(1), partitions: []fetchPartition{{index: 1}}},
		}
		body := fetch(t, addr, request...)

		if body.errorCode != 0 {
			t.Errorf("error_code = %d, want 0", body.errorCode)
		}

		// The response has the topics and partitions of the request, in the
		// same order.
		if len(body.topics) != len(request) {
			t.Fatalf("responses has %d elements, want %d", len(body.topics), len(request))
		}
		for i, want := range request {
			got := body.topics[i]
			if got.topicID != want.topicID {
				t.Errorf("responses[%d]: topic_id = %v, want %v", i, got.topicID, want.topicID)
			}
			if len(got.partitions) != len(want.partitions) {
				t.Fatalf("responses[%d]: partitions has %d elements, want %d", i, len(got.partitions), len(want.partitions))
			}
			for j, partition := range want.partitions {
				expectFetchError(t, got.partitions[j], partition.index, 100)
			}
		}
	})
}

// Stage cm4: a Fetch request for a topic that exists but has no messages is
// answered without an error and without records.
func TestFetchEmptyTopic(t *testing.T) {
	topicID := testTopicID(0x51)

	// The log of a partition without messages may be an empty file, or not
	// exist at all.
	logSetups := map[string]func(t *testing.T, logDir string){
		"empty log file": func(t *testing.T, logDir string) {
			writeLogSegment(t, logDir, "empty", 0, nil)
		},
		"no log file": func(t *testing.T, logDir string) {},
	}

	for name, setUpLog := range logSetups {
		t.Run(name, func(t *testing.T) {
			logDir := t.TempDir()
			writeClusterMetadataLog(t, logDir,
				testTopic{name: "other", id: testTopicID(0x50), partitions: []int32{0}},
				testTopic{name: "empty", id: topicID, partitions: []int32{0}},
			)
			setUpLog(t, logDir)
			addr := startTestServerWithLogDir(t, logDir)

			body := fetch(t, addr, fetchTopic{topicID: topicID, partitions: []fetchPartition{{index: 0}}})

			// The error code of the response is 0 (No Error), its
			// throttle_time_ms is 0.
			if body.errorCode != 0 {
				t.Errorf("error_code = %d, want 0", body.errorCode)
			}
			if body.throttleTimeMs != 0 {
				t.Errorf("throttle_time_ms = %d, want 0", body.throttleTimeMs)
			}

			// The responses field has 1 element, whose topic_id matches what
			// was sent in the request.
			if len(body.topics) != 1 {
				t.Fatalf("responses has %d elements, want 1", len(body.topics))
			}
			topic := body.topics[0]
			if topic.topicID != topicID {
				t.Errorf("topic_id = %v, want %v", topic.topicID, topicID)
			}

			// The partitions array has 1 element, whose partition_index is 0,
			// whose error_code is 0 (No Error) and that has no records.
			if len(topic.partitions) != 1 {
				t.Fatalf("partitions has %d elements, want 1", len(topic.partitions))
			}
			partition := topic.partitions[0]
			if partition.index != 0 {
				t.Errorf("partition_index = %d, want 0", partition.index)
			}
			if partition.errorCode != 0 {
				t.Errorf("error_code = %d, want 0", partition.errorCode)
			}
			if len(partition.records) != 0 {
				t.Errorf("got %d bytes of records, want none: % x", len(partition.records), partition.records)
			}

			// An empty partition starts and ends at the offset 0.
			if partition.highWatermark != 0 || partition.lastStableOffset != 0 || partition.logStartOffset != 0 {
				t.Errorf("high_watermark = %d, last_stable_offset = %d, log_start_offset = %d; want 0, 0, 0",
					partition.highWatermark, partition.lastStableOffset, partition.logStartOffset)
			}
		})
	}
}

// Stage eg2: a Fetch request for a topic with a single message is answered
// with the record batch of that message, exactly as it is stored on disk.
func TestFetchSingleMessage(t *testing.T) {
	topicID := testTopicID(0x61)
	log := encodeRecordBatch(testBatch{
		baseOffset: 0,
		timestamp:  1726045973899,
		values:     [][]byte{[]byte("Hello World!")},
	})

	logDir := t.TempDir()
	writeClusterMetadataLog(t, logDir,
		testTopic{name: "bar", id: testTopicID(0x60), partitions: []int32{0}},
		testTopic{name: "foo", id: topicID, partitions: []int32{0}},
	)
	writeLogSegment(t, logDir, "foo", 0, log)
	addr := startTestServerWithLogDir(t, logDir)

	body := fetch(t, addr, fetchTopic{topicID: topicID, partitions: []fetchPartition{{index: 0}}})

	// The error code of the response is 0 (No Error), its throttle_time_ms
	// is 0.
	if body.errorCode != 0 {
		t.Errorf("error_code = %d, want 0", body.errorCode)
	}
	if body.throttleTimeMs != 0 {
		t.Errorf("throttle_time_ms = %d, want 0", body.throttleTimeMs)
	}

	// The responses field has 1 element, whose topic_id matches what was
	// sent in the request.
	if len(body.topics) != 1 {
		t.Fatalf("responses has %d elements, want 1", len(body.topics))
	}
	topic := body.topics[0]
	if topic.topicID != topicID {
		t.Errorf("topic_id = %v, want %v", topic.topicID, topicID)
	}

	// The partitions array has 1 element, whose partition_index is 0 and
	// whose error_code is 0 (No Error).
	if len(topic.partitions) != 1 {
		t.Fatalf("partitions has %d elements, want 1", len(topic.partitions))
	}
	partition := topic.partitions[0]
	if partition.index != 0 {
		t.Errorf("partition_index = %d, want 0", partition.index)
	}
	if partition.errorCode != 0 {
		t.Errorf("error_code = %d, want 0", partition.errorCode)
	}

	// The entire record batch is read from disk: the records of the response
	// are the contents of the log file.
	if !bytes.Equal(partition.records, log) {
		t.Errorf("records = % x, want the contents of the log file: % x", partition.records, log)
	}

	// The records array has 1 element: a batch with the message.
	batches, err := decodeRecordBatches(partition.records)
	if err != nil {
		t.Fatalf("failed to decode the fetched records: %v", err)
	}
	if len(batches) != 1 {
		t.Fatalf("fetched %d record batches, want 1", len(batches))
	}
	records := mustDecodeRecords(t, batches[0])
	if len(records) != 1 || string(records[0].value) != "Hello World!" {
		t.Errorf("fetched records = %+v, want a single record with the value \"Hello World!\"", records)
	}

	// The partition holds one record, at the offset 0.
	if partition.highWatermark != 1 || partition.lastStableOffset != 1 || partition.logStartOffset != 0 {
		t.Errorf("high_watermark = %d, last_stable_offset = %d, log_start_offset = %d; want 1, 1, 0",
			partition.highWatermark, partition.lastStableOffset, partition.logStartOffset)
	}
}

// Each partition has its own log: the records of a fetched partition come
// from the directory named after its topic and its index.
func TestFetchReadsTheLogOfTheRequestedPartition(t *testing.T) {
	logs := map[string][]byte{
		"orders-0":      encodeRecordBatch(testBatch{values: [][]byte{[]byte("orders 0")}}),
		"orders-1":      encodeRecordBatch(testBatch{values: [][]byte{[]byte("orders 1")}}),
		"orders-10":     encodeRecordBatch(testBatch{values: [][]byte{[]byte("orders 10")}}),
		"my-topic.v1-0": encodeRecordBatch(testBatch{values: [][]byte{[]byte("my-topic.v1 0")}}),
	}

	logDir := t.TempDir()
	writeClusterMetadataLog(t, logDir,
		testTopic{name: "orders", id: testTopicID(1), partitions: []int32{0, 1, 10}},
		testTopic{name: "my-topic.v1", id: testTopicID(2), partitions: []int32{0}},
	)
	writeLogSegment(t, logDir, "orders", 0, logs["orders-0"])
	writeLogSegment(t, logDir, "orders", 1, logs["orders-1"])
	writeLogSegment(t, logDir, "orders", 10, logs["orders-10"])
	writeLogSegment(t, logDir, "my-topic.v1", 0, logs["my-topic.v1-0"])
	addr := startTestServerWithLogDir(t, logDir)

	body := fetch(t, addr,
		fetchTopic{topicID: testTopicID(2), partitions: []fetchPartition{{index: 0}}},
		fetchTopic{topicID: testTopicID(1), partitions: []fetchPartition{{index: 10}, {index: 0}, {index: 1}}},
	)

	want := []struct {
		topicID    uuid
		partitions []int32
		logs       []string
	}{
		{topicID: testTopicID(2), partitions: []int32{0}, logs: []string{"my-topic.v1-0"}},
		{topicID: testTopicID(1), partitions: []int32{10, 0, 1}, logs: []string{"orders-10", "orders-0", "orders-1"}},
	}

	if len(body.topics) != len(want) {
		t.Fatalf("responses has %d elements, want %d", len(body.topics), len(want))
	}
	for i, wantTopic := range want {
		got := body.topics[i]
		if got.topicID != wantTopic.topicID {
			t.Errorf("responses[%d]: topic_id = %v, want %v", i, got.topicID, wantTopic.topicID)
		}
		if len(got.partitions) != len(wantTopic.partitions) {
			t.Fatalf("responses[%d]: partitions has %d elements, want %d", i, len(got.partitions), len(wantTopic.partitions))
		}
		for j, index := range wantTopic.partitions {
			partition := got.partitions[j]
			if partition.index != index || partition.errorCode != 0 {
				t.Errorf("responses[%d].partitions[%d] = {index: %d, error_code: %d}, want {index: %d, error_code: 0}",
					i, j, partition.index, partition.errorCode, index)
			}
			if !bytes.Equal(partition.records, logs[wantTopic.logs[j]]) {
				t.Errorf("responses[%d].partitions[%d]: records are not the contents of the log %s", i, j, wantTopic.logs[j])
			}
		}
	}
}

// Record batches are passed on as they are stored, without looking at their
// records: a batch with compressed records can be fetched like any other.
func TestFetchCompressedBatch(t *testing.T) {
	log := encodeRecordBatch(testBatch{attributes: 0x01, values: [][]byte{[]byte("pretend this is gzip")}})

	logDir := t.TempDir()
	writeClusterMetadataLog(t, logDir, testTopic{name: "foo", id: testTopicID(1), partitions: []int32{0}})
	writeLogSegment(t, logDir, "foo", 0, log)
	addr := startTestServerWithLogDir(t, logDir)

	body := fetch(t, addr, fetchTopic{topicID: testTopicID(1), partitions: []fetchPartition{{index: 0}}})

	if len(body.topics) != 1 || len(body.topics[0].partitions) != 1 {
		t.Fatalf("unexpected response: %+v", body)
	}
	partition := body.topics[0].partitions[0]
	if partition.errorCode != 0 || !bytes.Equal(partition.records, log) {
		t.Errorf("partition = {error_code: %d, records: % x}, want the contents of the log file without an error",
			partition.errorCode, partition.records)
	}
}

// A partition whose log can't be read is reported with the error code
// KAFKA_STORAGE_ERROR (56); the other partitions of the request are served.
func TestFetchCorruptPartitionLog(t *testing.T) {
	validLog := encodeRecordBatch(testBatch{values: [][]byte{[]byte("Hello World!")}})

	logDir := t.TempDir()
	writeClusterMetadataLog(t, logDir, testTopic{name: "foo", id: testTopicID(1), partitions: []int32{0, 1}})
	writeLogSegment(t, logDir, "foo", 0, validLog[:len(validLog)-5])
	writeLogSegment(t, logDir, "foo", 1, validLog)
	addr := startTestServerWithLogDir(t, logDir)

	body := fetch(t, addr, fetchTopic{topicID: testTopicID(1), partitions: []fetchPartition{{index: 0}, {index: 1}}})

	if body.errorCode != 0 {
		t.Errorf("error_code = %d, want 0", body.errorCode)
	}
	if len(body.topics) != 1 || len(body.topics[0].partitions) != 2 {
		t.Fatalf("unexpected response: %+v", body)
	}

	expectFetchError(t, body.topics[0].partitions[0], 0, 56)

	healthy := body.topics[0].partitions[1]
	if healthy.index != 1 || healthy.errorCode != 0 || !bytes.Equal(healthy.records, validLog) {
		t.Errorf("partition 1 = {index: %d, error_code: %d, records: % x}, want the contents of its log without an error",
			healthy.index, healthy.errorCode, healthy.records)
	}
}

// A Fetch request may name partitions that exist next to ones that don't:
// each of them gets its own answer.
func TestFetchKnownAndUnknownPartitions(t *testing.T) {
	logDir := t.TempDir()
	writeClusterMetadataLog(t, logDir,
		testTopic{name: "one", id: testTopicID(1), partitions: []int32{0}},
		testTopic{name: "two", id: testTopicID(2), partitions: []int32{0, 1}},
	)
	addr := startTestServerWithLogDir(t, logDir)

	body := fetch(t, addr,
		fetchTopic{topicID: testTopicID(2), partitions: []fetchPartition{{index: 1}, {index: 2}, {index: 0}}},
		fetchTopic{topicID: testTopicID(9), partitions: []fetchPartition{{index: 0}}},
		fetchTopic{topicID: testTopicID(1), partitions: []fetchPartition{{index: 0}, {index: -1}}},
	)

	if body.errorCode != 0 {
		t.Errorf("error_code = %d, want 0", body.errorCode)
	}

	type partitionResult struct {
		index     int32
		errorCode int16
	}
	want := []struct {
		topicID    uuid
		partitions []partitionResult
	}{
		{
			topicID: testTopicID(2),
			// The topic exists, but it has no partition 2:
			// UNKNOWN_TOPIC_OR_PARTITION (3).
			partitions: []partitionResult{{index: 1, errorCode: 0}, {index: 2, errorCode: 3}, {index: 0, errorCode: 0}},
		},
		{
			topicID: testTopicID(9),
			// The topic doesn't exist: UNKNOWN_TOPIC_ID (100).
			partitions: []partitionResult{{index: 0, errorCode: 100}},
		},
		{
			topicID:    testTopicID(1),
			partitions: []partitionResult{{index: 0, errorCode: 0}, {index: -1, errorCode: 3}},
		},
	}

	if len(body.topics) != len(want) {
		t.Fatalf("responses has %d elements, want %d", len(body.topics), len(want))
	}
	for i, wantTopic := range want {
		got := body.topics[i]
		if got.topicID != wantTopic.topicID {
			t.Errorf("responses[%d]: topic_id = %v, want %v", i, got.topicID, wantTopic.topicID)
		}
		if len(got.partitions) != len(wantTopic.partitions) {
			t.Fatalf("responses[%d]: partitions has %d elements, want %d", i, len(got.partitions), len(wantTopic.partitions))
		}
		for j, wantPartition := range wantTopic.partitions {
			partition := got.partitions[j]
			if partition.index != wantPartition.index || partition.errorCode != wantPartition.errorCode {
				t.Errorf("responses[%d].partitions[%d] = {index: %d, error_code: %d}, want %+v",
					i, j, partition.index, partition.errorCode, wantPartition)
			}

			// Failed fetches have no offsets to report.
			wantOffset := int64(0)
			if wantPartition.errorCode != 0 {
				wantOffset = -1
			}
			if partition.highWatermark != wantOffset || partition.logStartOffset != wantOffset {
				t.Errorf("responses[%d].partitions[%d]: high_watermark = %d, log_start_offset = %d; want %d for both",
					i, j, partition.highWatermark, partition.logStartOffset, wantOffset)
			}
		}
	}
}

// A cluster metadata log that can't be read is not mistaken for a cluster
// without topics: the request fails, and the connection is closed.
func TestFetchWithCorruptMetadataLog(t *testing.T) {
	logDir := t.TempDir()
	writeLogSegment(t, logDir, "__cluster_metadata", 0, mustDecodeHex(t, sazTopicBatchHex)[:100])
	addr := startTestServerWithLogDir(t, logDir)

	conn := dial(t, addr)
	send(t, conn, newFetchRequest(1, fetchTopic{topicID: sazTopicID, partitions: []fetchPartition{{index: 1}}}))

	expectConnectionClosed(t, conn)
}

// expectFetchError checks that partition is how the broker answers a fetch
// that failed with the given error code: without any records.
func expectFetchError(t *testing.T, partition fetchedPartition, index int32, errorCode int16) {
	t.Helper()

	if partition.index != index {
		t.Errorf("partition_index = %d, want %d", partition.index, index)
	}
	if partition.errorCode != errorCode {
		t.Errorf("partition %d: error_code = %d, want %d", index, partition.errorCode, errorCode)
	}
	if len(partition.records) != 0 {
		t.Errorf("partition %d: got %d bytes of records, want none", index, len(partition.records))
	}
}

// The broker only supports version 16 of Fetch: requests for other versions,
// and requests that it can't parse, make it close the connection.
func TestInvalidFetchRequestClosesConnection(t *testing.T) {
	validBody := newFetchRequest(1)[4+2+2+4+2+len("kafka-cli")+1:]

	tests := []struct {
		name    string
		request []byte
	}{
		{name: "version 15", request: encodeRequest(1, 15, 1, validBody)},
		{name: "version 17", request: encodeRequest(1, 17, 1, validBody)},
		{name: "version 0", request: encodeRequest(1, 0, 1, validBody)},
		{name: "no body", request: encodeRequest(1, 16, 1, nil)},
		{name: "truncated body", request: encodeRequest(1, 16, 1, validBody[:len(validBody)-2])},
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
