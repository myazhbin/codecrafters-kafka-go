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

	// highWatermark is the offset that follows the last record of the
	// partition that consumers may read, and logStartOffset the offset of
	// its first record. Both are -1 when the fetch failed.
	highWatermark  int64
	logStartOffset int64

	// records holds the record batches that the client asked for, as they
	// are stored in the log of the partition.
	records []byte
}

// fetchError returns the response for a partition that can't be fetched from.
func fetchError(index int32, errorCode int16) fetchPartitionResponse {
	return fetchPartitionResponse{
		index:          index,
		errorCode:      errorCode,
		highWatermark:  -1,
		logStartOffset: -1,
	}
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
			e.int64(partition.highWatermark)
			// Without transactions, all the records below the high watermark
			// are stable.
			e.int64(partition.highWatermark) // last_stable_offset
			e.int64(partition.logStartOffset)
			e.compactArrayLength(0) // aborted_transactions
			e.int32(-1)             // preferred_read_replica: none
			e.compactBytes(partition.records)
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

	metadata, err := loadClusterMetadata(b.logDir)
	if err != nil {
		return nil, err
	}

	topics := make([]fetchTopicResponse, 0, len(request.topics))
	for _, topic := range request.topics {
		response := fetchTopicResponse{topicID: topic.topicID}
		for _, partition := range topic.partitions {
			response.partitions = append(response.partitions, b.fetchFromPartition(metadata, topic.topicID, partition))
		}
		topics = append(topics, response)
	}

	return encodeFetchResponse(topics), nil
}

// fetchFromPartition answers the part of a Fetch request that is about one
// partition of the topic with the given ID: it reads the record batches of the
// partition from its log on disk, starting at the offset that the client asked
// for. The limits that the request sets on the size of the response are not
// taken into account.
func (b *broker) fetchFromPartition(metadata *clusterMetadata, topicID uuid, partition fetchPartition) fetchPartitionResponse {
	topic, ok := metadata.topicsByID[topicID]
	if !ok {
		return fetchError(partition.index, errorCodeUnknownTopicID)
	}
	if !topic.hasPartition(partition.index) {
		return fetchError(partition.index, errorCodeUnknownTopicOrPartition)
	}

	log, batches, err := readRecordBatches(partitionDir(b.logDir, topic.name, partition.index))
	if err != nil {
		fmt.Printf("Error reading the log of partition %d of topic %s: %v\n", partition.index, topic.name, err)
		return fetchError(partition.index, errorCodeKafkaStorageError)
	}

	// A partition without records starts and ends at the offset 0.
	var logStartOffset, logEndOffset int64
	if len(batches) > 0 {
		logStartOffset = batches[0].baseOffset
		logEndOffset = batches[len(batches)-1].lastOffset() + 1
	}

	// Fetching from the end of the log is fine: there just are no records to
	// return yet.
	if partition.fetchOffset < logStartOffset || partition.fetchOffset > logEndOffset {
		return fetchError(partition.index, errorCodeOffsetOutOfRange)
	}

	// Batches are always returned as a whole: the response starts with the
	// batch that holds the record at the fetch offset, even if that record
	// is not the first one of the batch.
	skipped := 0
	for _, batch := range batches {
		if batch.lastOffset() >= partition.fetchOffset {
			break
		}
		skipped += len(batch.raw)
	}

	return fetchPartitionResponse{
		index:          partition.index,
		highWatermark:  logEndOffset,
		logStartOffset: logStartOffset,
		records:        log[skipped:],
	}
}
