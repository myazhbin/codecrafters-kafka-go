package main

import "fmt"

// fetchRequest is the part of the body of a Fetch request (v16) that the
// broker makes use of.
type fetchRequest struct {
	topics []fetchTopic
}

// fetchTopic is a topic that a Fetch request asks records of.
type fetchTopic struct {
	topicID    uuid
	partitions []fetchPartition
}

// fetchPartition is a partition that a Fetch request asks records of.
type fetchPartition struct {
	index       int32
	fetchOffset int64 // the offset of the first record that the client wants
}

// decodeFetchRequest decodes the body of a Fetch request (v16).
func decodeFetchRequest(d *decoder) (fetchRequest, error) {
	var request fetchRequest

	d.int32() // max_wait_ms
	d.int32() // min_bytes
	d.int32() // max_bytes
	d.int8()  // isolation_level
	d.int32() // session_id
	d.int32() // session_epoch

	topicCount := d.compactArrayLength()
	for i := 0; i < topicCount && d.err == nil; i++ {
		topic := fetchTopic{topicID: d.uuid()}

		partitionCount := d.compactArrayLength()
		for j := 0; j < partitionCount && d.err == nil; j++ {
			partition := fetchPartition{index: d.int32()}
			d.int32() // current_leader_epoch
			partition.fetchOffset = d.int64()
			d.int32() // last_fetched_epoch
			d.int64() // log_start_offset
			d.int32() // partition_max_bytes
			d.taggedFields()

			topic.partitions = append(topic.partitions, partition)
		}
		d.taggedFields()

		request.topics = append(request.topics, topic)
	}

	// forgotten_topics_data: the partitions to remove from a fetch session.
	forgottenTopicCount := d.compactArrayLength()
	for i := 0; i < forgottenTopicCount && d.err == nil; i++ {
		d.uuid()              // topic_id
		d.compactInt32Array() // partitions
		d.taggedFields()
	}

	d.compactString() // rack_id
	d.taggedFields()

	return request, d.err
}

// encodeFetchResponse encodes the body of a Fetch response (v16).
func encodeFetchResponse() []byte {
	var e encoder
	e.int32(0)              // throttle_time_ms
	e.int16(errorCodeNone)  // error_code
	e.int32(0)              // session_id: the fetch is not part of a session
	e.compactArrayLength(0) // responses
	e.emptyTaggedFields()

	return e.buf
}

// handleFetch builds the body of the response to a Fetch request, whose body
// is read from d.
func (b *broker) handleFetch(d *decoder) ([]byte, error) {
	if _, err := decodeFetchRequest(d); err != nil {
		return nil, fmt.Errorf("parsing Fetch request: %w", err)
	}

	return encodeFetchResponse(), nil
}
