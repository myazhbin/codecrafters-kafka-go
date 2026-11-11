package main

import "fmt"

// describeTopicPartitionsRequest is the body of a DescribeTopicPartitions
// request (v0).
type describeTopicPartitionsRequest struct {
	topicNames             []string
	responsePartitionLimit int32
	cursor                 *topicPartitionCursor // nil when the request has no cursor
}

// topicPartitionCursor is the partition at which a paginated listing of topic
// partitions continues.
type topicPartitionCursor struct {
	topicName      string
	partitionIndex int32
}

// decodeDescribeTopicPartitionsRequest decodes the body of a
// DescribeTopicPartitions request (v0).
func decodeDescribeTopicPartitionsRequest(d *decoder) (describeTopicPartitionsRequest, error) {
	var request describeTopicPartitionsRequest

	topicCount := d.compactArrayLength()
	for i := 0; i < topicCount && d.err == nil; i++ {
		request.topicNames = append(request.topicNames, d.compactString())
		d.taggedFields()
	}

	request.responsePartitionLimit = d.int32()

	// The cursor is a nullable struct: it starts with a byte that is negative
	// when the cursor is null, in which case nothing else follows.
	if d.int8() >= 0 && d.err == nil {
		request.cursor = &topicPartitionCursor{
			topicName:      d.compactString(),
			partitionIndex: d.int32(),
		}
		d.taggedFields()
	}

	d.taggedFields()

	return request, d.err
}

// topicDescription is what a DescribeTopicPartitions response says about one
// topic.
type topicDescription struct {
	errorCode  int16
	name       string
	id         uuid
	isInternal bool
}

// encodeDescribeTopicPartitionsResponse encodes the body of a
// DescribeTopicPartitions response (v0).
func encodeDescribeTopicPartitionsResponse(topics []topicDescription) []byte {
	var e encoder
	e.int32(0) // throttle_time_ms

	e.compactArrayLength(len(topics))
	for _, topic := range topics {
		e.int16(topic.errorCode)
		e.compactString(topic.name)
		e.uuid(topic.id)
		e.boolean(topic.isInternal)
		e.compactArrayLength(0) // partitions
		e.int32(0)              // topic_authorized_operations
		e.emptyTaggedFields()
	}

	e.int8(-1) // next_cursor: null
	e.emptyTaggedFields()

	return e.buf
}

// handleDescribeTopicPartitions builds the body of the response to a
// DescribeTopicPartitions request, whose body is read from d.
func handleDescribeTopicPartitions(d *decoder) ([]byte, error) {
	request, err := decodeDescribeTopicPartitionsRequest(d)
	if err != nil {
		return nil, fmt.Errorf("parsing DescribeTopicPartitions request: %w", err)
	}

	// The broker doesn't know any topic yet: all of them are reported as
	// unknown, with an all-zero topic ID and no partitions.
	topics := make([]topicDescription, 0, len(request.topicNames))
	for _, name := range request.topicNames {
		topics = append(topics, topicDescription{
			errorCode: errorCodeUnknownTopicOrPartition,
			name:      name,
		})
	}

	return encodeDescribeTopicPartitionsResponse(topics), nil
}
