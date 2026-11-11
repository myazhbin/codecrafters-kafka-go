package main

import (
	"fmt"
	"slices"
)

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
	partitions []partitionMetadata
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
		e.boolean(false) // is_internal

		e.compactArrayLength(len(topic.partitions))
		for _, partition := range topic.partitions {
			e.int16(errorCodeNone)
			e.int32(partition.index)
			e.int32(partition.leader)
			e.int32(partition.leaderEpoch)
			e.compactInt32Array(partition.replicas)
			e.compactInt32Array(partition.isr)
			e.compactInt32Array(nil) // eligible_leader_replicas
			e.compactInt32Array(nil) // last_known_elr
			e.compactInt32Array(nil) // offline_replicas
			e.emptyTaggedFields()
		}

		e.int32(0) // topic_authorized_operations
		e.emptyTaggedFields()
	}

	// The broker never splits its answer over several responses, so there is
	// no partition to continue from.
	e.int8(-1) // next_cursor: null
	e.emptyTaggedFields()

	return e.buf
}

// handleDescribeTopicPartitions builds the body of the response to a
// DescribeTopicPartitions request, whose body is read from d.
//
// Topics that exist in the cluster metadata are described with their ID and
// their partitions. The other ones are reported as unknown, with an all-zero
// topic ID and no partitions.
func (b *broker) handleDescribeTopicPartitions(d *decoder) ([]byte, error) {
	request, err := decodeDescribeTopicPartitionsRequest(d)
	if err != nil {
		return nil, fmt.Errorf("parsing DescribeTopicPartitions request: %w", err)
	}

	metadata, err := loadClusterMetadata(b.logDir)
	if err != nil {
		return nil, err
	}

	// Like Kafka, the broker describes the requested topics in alphabetical
	// order, and each of them only once.
	names := slices.Clone(request.topicNames)
	slices.Sort(names)
	names = slices.Compact(names)

	topics := make([]topicDescription, 0, len(names))
	for _, name := range names {
		topic, ok := metadata.topicsByName[name]
		if !ok {
			topics = append(topics, topicDescription{
				errorCode: errorCodeUnknownTopicOrPartition,
				name:      name,
			})
			continue
		}

		topics = append(topics, topicDescription{
			errorCode:  errorCodeNone,
			name:       topic.name,
			id:         topic.id,
			partitions: topic.partitions,
		})
	}

	return encodeDescribeTopicPartitionsResponse(topics), nil
}
