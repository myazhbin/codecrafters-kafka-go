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

// fetchTopicResponse is what a Fetch response says about one topic.
type fetchTopicResponse struct {
	topicID    uuid
	partitions []fetchPartitionResponse
}

// fetchPartitionResponse is what a Fetch response says about one partition.
type fetchPartitionResponse struct {
	index     int32
	errorCode int16
}

// encodeFetchResponse encodes the body of a Fetch response (v16).
func encodeFetchResponse(topics []fetchTopicResponse) []byte {
	var e encoder
	e.int32(0)             // throttle_time_ms
	e.int16(errorCodeNone) // error_code
	e.int32(0)             // session_id: the fetch is not part of a session

	e.compactArrayLength(len(topics)) // responses
	for _, topic := range topics {
		e.uuid(topic.topicID)

		e.compactArrayLength(len(topic.partitions))
		for _, partition := range topic.partitions {
			e.int32(partition.index)
			e.int16(partition.errorCode)
			e.int64(-1)             // high_watermark: unknown
			e.int64(-1)             // last_stable_offset: unknown
			e.int64(-1)             // log_start_offset: unknown
			e.compactArrayLength(0) // aborted_transactions
			e.int32(-1)             // preferred_read_replica: none
			e.compactBytes(nil)     // records
			e.emptyTaggedFields()
		}
		e.emptyTaggedFields()
	}
	e.emptyTaggedFields()

	return e.buf
}

// handleFetch builds the body of the response to a Fetch request, whose body
// is read from d.
func (b *broker) handleFetch(d *decoder) ([]byte, error) {
	request, err := decodeFetchRequest(d)
	if err != nil {
		return nil, fmt.Errorf("parsing Fetch request: %w", err)
	}

	// The broker can't serve the records of any topic yet: all the requested
	// partitions are reported as belonging to an unknown topic.
	topics := make([]fetchTopicResponse, 0, len(request.topics))
	for _, topic := range request.topics {
		response := fetchTopicResponse{topicID: topic.topicID}
		for _, partition := range topic.partitions {
			response.partitions = append(response.partitions, fetchPartitionResponse{
				index:     partition.index,
				errorCode: errorCodeUnknownTopicID,
			})
		}
		topics = append(topics, response)
	}

	return encodeFetchResponse(topics), nil
}
