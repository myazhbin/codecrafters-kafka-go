package main

import (
	"errors"
	"fmt"
)

// produceRequest is the part of the body of a Produce request (v11) that the
// broker makes use of.
type produceRequest struct {
	// acks is the number of acknowledgements that the producer wants before
	// its request is considered complete: 1 for the one of the leader of the
	// partition, -1 for the ones of all the in-sync replicas, and 0 for none
	// at all, in which case the producer doesn't even expect a response.
	acks   int16
	topics []produceTopic
}

// produceTopic is a topic that a Produce request has records for.
type produceTopic struct {
	name       string
	partitions []producePartition
}

// producePartition is a partition that a Produce request has records for.
type producePartition struct {
	index   int32
	records []byte // the record batches to append to the partition
}

// decodeProduceRequest decodes the body of a Produce request (v11).
func decodeProduceRequest(d *decoder) (produceRequest, error) {
	var request produceRequest

	d.compactNullableString() // transactional_id
	request.acks = d.int16()
	d.int32() // timeout_ms

	topicCount := d.compactArrayLength()
	for i := 0; i < topicCount && d.err == nil; i++ {
		topic := produceTopic{name: d.compactString()}

		partitionCount := d.compactArrayLength()
		for j := 0; j < partitionCount && d.err == nil; j++ {
			partition := producePartition{
				index:   d.int32(),
				records: d.compactBytes(),
			}
			d.taggedFields()

			topic.partitions = append(topic.partitions, partition)
		}
		d.taggedFields()

		request.topics = append(request.topics, topic)
	}
	d.taggedFields()

	return request, d.err
}

// produceTopicResponse is what a Produce response says about one topic.
type produceTopicResponse struct {
	name       string
	partitions []producePartitionResponse
}

// producePartitionResponse is what a Produce response says about one
// partition.
type producePartitionResponse struct {
	index     int32
	errorCode int16

	// baseOffset is the offset that was given to the first record that the
	// request added to the partition, and logStartOffset the offset of the
	// first record of the partition. Both are -1 when the request failed.
	baseOffset     int64
	logStartOffset int64
}

// produceError returns the response for a partition that the records of a
// Produce request could not be added to.
func produceError(index int32, errorCode int16) producePartitionResponse {
	return producePartitionResponse{
		index:          index,
		errorCode:      errorCode,
		baseOffset:     -1,
		logStartOffset: -1,
	}
}

// encodeProduceResponse encodes the body of a Produce response (v11).
func encodeProduceResponse(topics []produceTopicResponse) []byte {
	var e encoder

	e.compactArrayLength(len(topics)) // responses
	for _, topic := range topics {
		e.compactString(topic.name)

		e.compactArrayLength(len(topic.partitions))
		for _, partition := range topic.partitions {
			e.int32(partition.index)
			e.int16(partition.errorCode)
			e.int64(partition.baseOffset)
			// The timestamps of the records are the ones that the producer
			// set, not the time at which the records were appended.
			e.int64(-1) // log_append_time_ms
			e.int64(partition.logStartOffset)
			e.compactArrayLength(0) // record_errors
			e.uvarint(0)            // error_message: null
			e.emptyTaggedFields()
		}
		e.emptyTaggedFields()
	}

	e.int32(0) // throttle_time_ms
	e.emptyTaggedFields()

	return e.buf
}

// handleProduce builds the body of the response to a Produce request, whose
// body is read from d.
//
// A producer that asks for no acknowledgement (acks=0) doesn't expect a
// response: none is built for its requests. All that the broker can do to
// tell such a producer about a failure is to close the connection, which is
// what returning an error does.
func (b *broker) handleProduce(d *decoder) ([]byte, error) {
	request, err := decodeProduceRequest(d)
	if err != nil {
		return nil, fmt.Errorf("parsing Produce request: %w", err)
	}

	// The broker can't store records yet: all the partitions are reported as
	// unknown.
	failed := false
	topics := make([]produceTopicResponse, 0, len(request.topics))
	for _, topic := range request.topics {
		response := produceTopicResponse{name: topic.name}
		for _, partition := range topic.partitions {
			response.partitions = append(response.partitions, produceError(partition.index, errorCodeUnknownTopicOrPartition))
			failed = true
		}
		topics = append(topics, response)
	}

	if request.acks == 0 {
		if failed {
			return nil, errors.New("Produce request without acknowledgement failed")
		}
		return nil, nil
	}

	return encodeProduceResponse(topics), nil
}
