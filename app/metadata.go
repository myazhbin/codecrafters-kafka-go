package main

import (
	"cmp"
	"fmt"
	"slices"
)

// clusterMetadataTopic is the internal topic whose only partition, 0, holds
// the cluster metadata log: the records that describe the topics and
// partitions of the cluster.
const clusterMetadataTopic = "__cluster_metadata"

// metadataFrameVersion is the version of the framing of the records of the
// cluster metadata log that the broker understands.
const metadataFrameVersion = 1

// Types of the records of the cluster metadata log that the broker
// understands. They are the API keys of the record schemas in the Kafka source
// code (metadata/src/main/resources/common/metadata).
const (
	metadataRecordTopic     = 2
	metadataRecordPartition = 3
)

// clusterMetadata is what the broker knows about the cluster: its topics and
// their partitions.
type clusterMetadata struct {
	topicsByName map[string]*topicMetadata
	topicsByID   map[uuid]*topicMetadata
}

// topicMetadata describes a topic.
type topicMetadata struct {
	name       string
	id         uuid
	partitions []partitionMetadata // sorted by partition index
}

// partitionMetadata describes a partition of a topic.
type partitionMetadata struct {
	index       int32
	leader      int32   // ID of the broker that leads the partition
	leaderEpoch int32   // incremented every time the leader changes
	replicas    []int32 // IDs of the brokers that host the partition
	isr         []int32 // IDs of the replicas that are in sync with the leader
}

func newClusterMetadata() *clusterMetadata {
	return &clusterMetadata{
		topicsByName: make(map[string]*topicMetadata),
		topicsByID:   make(map[uuid]*topicMetadata),
	}
}

// loadClusterMetadata builds the metadata of the cluster from the cluster
// metadata log of a broker that stores its logs in logDir. Without such a log,
// the cluster has no topics.
func loadClusterMetadata(logDir string) (*clusterMetadata, error) {
	log, err := readLog(partitionDir(logDir, clusterMetadataTopic, 0))
	if err != nil {
		return nil, fmt.Errorf("reading cluster metadata log: %w", err)
	}

	batches, err := decodeRecordBatches(log)
	if err != nil {
		return nil, fmt.Errorf("reading cluster metadata log: %w", err)
	}

	metadata := newClusterMetadata()
	for _, batch := range batches {
		// Control batches hold records about the log itself, like changes of
		// the leader of the metadata quorum, rather than metadata records.
		if batch.isControl() {
			continue
		}

		for i, record := range batch.records {
			if err := metadata.replay(record.value); err != nil {
				return nil, fmt.Errorf("reading cluster metadata log: record at offset %d: %w", batch.baseOffset+int64(i), err)
			}
		}
	}

	return metadata, nil
}

// replay applies a record of the cluster metadata log to the metadata. Records
// of types other than topic and partition records are ignored.
func (m *clusterMetadata) replay(value []byte) error {
	d := newDecoder(value)

	frameVersion := d.uvarint()
	recordType := d.uvarint()
	recordVersion := d.uvarint()
	if d.err != nil {
		return d.err
	}
	if frameVersion != metadataFrameVersion {
		return fmt.Errorf("unsupported metadata record frame version: %d", frameVersion)
	}

	switch recordType {
	case metadataRecordTopic:
		topic := &topicMetadata{
			name: d.compactString(),
			id:   d.uuid(),
		}
		d.taggedFields()
		if d.err != nil {
			return fmt.Errorf("decoding topic record: %w", d.err)
		}

		m.topicsByName[topic.name] = topic
		m.topicsByID[topic.id] = topic

	case metadataRecordPartition:
		partition := partitionMetadata{index: d.int32()}
		topicID := d.uuid()
		partition.replicas = d.compactInt32Array()
		partition.isr = d.compactInt32Array()
		d.compactInt32Array() // removing replicas
		d.compactInt32Array() // adding replicas
		partition.leader = d.int32()
		partition.leaderEpoch = d.int32()
		d.int32() // partition epoch
		if recordVersion >= 1 {
			// The log directory of each replica.
			for i, n := 0, d.compactArrayLength(); i < n && d.err == nil; i++ {
				d.uuid()
			}
		}
		d.taggedFields()
		if d.err != nil {
			return fmt.Errorf("decoding partition record: %w", d.err)
		}

		topic, ok := m.topicsByID[topicID]
		if !ok {
			return fmt.Errorf("partition record for unknown topic %v", topicID)
		}
		topic.setPartition(partition)
	}

	return nil
}

// setPartition adds a partition to the topic, or replaces the partition that
// has the same index.
func (t *topicMetadata) setPartition(partition partitionMetadata) {
	i, found := slices.BinarySearchFunc(t.partitions, partition.index, func(p partitionMetadata, index int32) int {
		return cmp.Compare(p.index, index)
	})
	if found {
		t.partitions[i] = partition
	} else {
		t.partitions = slices.Insert(t.partitions, i, partition)
	}
}
