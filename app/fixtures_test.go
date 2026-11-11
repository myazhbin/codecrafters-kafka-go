package main

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

// The fixtures in this file build the files that a broker reads from its log
// directory, in the format that Kafka writes them in.

// featureLevelBatchHex and sazTopicBatchHex are the first two record batches
// of the cluster metadata log that the CodeCrafters tester generates, as
// shown in the protocol inspector linked from the stage instructions
// (https://binspec.org/kafka-cluster-metadata). They are written out byte by
// byte, so that they don't depend on the encoders in this file.
const (
	// A batch with one record: a feature level record.
	featureLevelBatchHex = "0000000000000001" + // base offset: 1
		"0000004f" + // batch length: 79
		"00000001" + // partition leader epoch: 1
		"02" + // magic byte: 2
		"b069457c" + // CRC
		"0000" + // attributes
		"00000000" + // last offset delta: 0
		"00000191e05af818" + // base timestamp
		"00000191e05af818" + // max timestamp
		"ffffffffffffffff" + // producer ID: -1
		"ffff" + // producer epoch: -1
		"ffffffff" + // base sequence: -1
		"00000001" + // records count: 1
		"3a" + // record length: 29
		"00" + // attributes
		"00" + // timestamp delta: 0
		"00" + // offset delta: 0
		"01" + // key length: -1 (null)
		"2e" + // value length: 23
		"01" + // frame version: 1
		"0c" + // record type: 12 (feature level record)
		"00" + // record version: 0
		"11" + // name length: 16
		"6d657461646174612e76657273696f6e" + // name: "metadata.version"
		"0014" + // feature level: 20
		"00" + // tagged fields count: 0
		"00" // headers count: 0

	// A batch with two records: the topic record of the topic "saz" and the
	// partition record of its partition 1.
	sazTopicBatchHex = "0000000000000002" + // base offset: 2
		"0000009a" + // batch length: 154
		"00000001" + // partition leader epoch: 1
		"02" + // magic byte: 2
		"c64fa9c1" + // CRC
		"0000" + // attributes
		"00000001" + // last offset delta: 1
		"00000191e05b2d15" + // base timestamp
		"00000191e05b2d15" + // max timestamp
		"ffffffffffffffff" + // producer ID: -1
		"ffff" + // producer epoch: -1
		"ffffffff" + // base sequence: -1
		"00000002" + // records count: 2

		"3c" + // record length: 30
		"00" + // attributes
		"00" + // timestamp delta: 0
		"00" + // offset delta: 0
		"01" + // key length: -1 (null)
		"30" + // value length: 24
		"01" + // frame version: 1
		"02" + // record type: 2 (topic record)
		"00" + // record version: 0
		"04" + // name length: 3
		"73617a" + // name: "saz"
		"00000000000040008000000000000091" + // topic UUID
		"00" + // tagged fields count: 0
		"00" + // headers count: 0

		"9001" + // record length: 72
		"00" + // attributes
		"00" + // timestamp delta: 0
		"02" + // offset delta: 1
		"01" + // key length: -1 (null)
		"8201" + // value length: 65
		"01" + // frame version: 1
		"03" + // record type: 3 (partition record)
		"01" + // record version: 1
		"00000001" + // partition ID: 1
		"00000000000040008000000000000091" + // topic UUID
		"02" + "00000001" + // replicas: [1]
		"02" + "00000001" + // in-sync replicas: [1]
		"01" + // removing replicas: []
		"01" + // adding replicas: []
		"00000001" + // leader: 1
		"00000000" + // leader epoch: 0
		"00000000" + // partition epoch: 0
		"02" + "10000000000040008000000000000001" + // directories: 1 UUID
		"00" + // tagged fields count: 0
		"00" // headers count: 0
)

// sazTopicID is the UUID of the topic "saz" in sazTopicBatchHex.
var sazTopicID = uuid{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x91}

// testBatch describes a record batch to build with encodeRecordBatch.
type testBatch struct {
	baseOffset           int64
	partitionLeaderEpoch int32
	attributes           int16
	timestamp            int64    // used as base and max timestamp
	values               [][]byte // the value of each record; nil for a null value
}

// encodeRecordBatch builds a record batch the way Kafka writes it to a log
// segment. Its records have no key and no headers.
func encodeRecordBatch(batch testBatch) []byte {
	// Everything from the attributes on is covered by the CRC.
	var body []byte
	body = binary.BigEndian.AppendUint16(body, uint16(batch.attributes))
	body = binary.BigEndian.AppendUint32(body, uint32(len(batch.values)-1)) // last offset delta
	body = binary.BigEndian.AppendUint64(body, uint64(batch.timestamp))     // base timestamp
	body = binary.BigEndian.AppendUint64(body, uint64(batch.timestamp))     // max timestamp
	body = binary.BigEndian.AppendUint64(body, 0xffffffffffffffff)          // producer ID: -1
	body = binary.BigEndian.AppendUint16(body, 0xffff)                      // producer epoch: -1
	body = binary.BigEndian.AppendUint32(body, 0xffffffff)                  // base sequence: -1
	body = binary.BigEndian.AppendUint32(body, uint32(len(batch.values)))   // records count
	for i, value := range batch.values {
		var record []byte
		record = append(record, 0)                           // attributes
		record = binary.AppendVarint(record, 0)              // timestamp delta
		record = binary.AppendVarint(record, int64(i))       // offset delta
		record = binary.AppendVarint(record, -1)             // key length: null
		record = appendRecordBytes(record, value)            // value
		record = binary.AppendVarint(record, 0)              // headers count
		body = binary.AppendVarint(body, int64(len(record))) // record length
		body = append(body, record...)
	}

	var b []byte
	b = binary.BigEndian.AppendUint64(b, uint64(batch.baseOffset))
	b = binary.BigEndian.AppendUint32(b, uint32(4+1+4+len(body))) // batch length
	b = binary.BigEndian.AppendUint32(b, uint32(batch.partitionLeaderEpoch))
	b = append(b, 2) // magic byte
	b = binary.BigEndian.AppendUint32(b, crc32.Checksum(body, crc32.MakeTable(crc32.Castagnoli)))
	return append(b, body...)
}

// appendRecordBytes appends a byte sequence of a record: its length as a
// VARINT, where -1 stands for null, followed by the bytes.
func appendRecordBytes(b []byte, value []byte) []byte {
	if value == nil {
		return binary.AppendVarint(b, -1)
	}
	b = binary.AppendVarint(b, int64(len(value)))
	return append(b, value...)
}

// metadataRecord builds the value of a record of the cluster metadata log: the
// frame version, the type and version of the record, and its data.
func metadataRecord(recordType, recordVersion byte, data []byte) []byte {
	return append([]byte{1, recordType, recordVersion}, data...)
}

// featureLevelRecord builds the value of a feature level record (v0).
func featureLevelRecord(name string, level int16) []byte {
	var data []byte
	data = appendCompactString(data, name)
	data = binary.BigEndian.AppendUint16(data, uint16(level))
	data = append(data, 0) // tagged fields
	return metadataRecord(12, 0, data)
}

// topicRecord builds the value of a topic record (v0).
func topicRecord(name string, id uuid) []byte {
	var data []byte
	data = appendCompactString(data, name)
	data = append(data, id[:]...)
	data = append(data, 0) // tagged fields
	return metadataRecord(2, 0, data)
}

// testPartition describes a partition record to build with partitionRecord.
type testPartition struct {
	index       int32
	topicID     uuid
	replicas    []int32
	isr         []int32
	leader      int32
	leaderEpoch int32
}

// partitionRecord builds the value of a partition record (v1).
func partitionRecord(partition testPartition) []byte {
	var data []byte
	data = binary.BigEndian.AppendUint32(data, uint32(partition.index))
	data = append(data, partition.topicID[:]...)
	data = appendCompactInt32Array(data, partition.replicas)
	data = appendCompactInt32Array(data, partition.isr)
	data = appendCompactInt32Array(data, nil) // removing replicas
	data = appendCompactInt32Array(data, nil) // adding replicas
	data = binary.BigEndian.AppendUint32(data, uint32(partition.leader))
	data = binary.BigEndian.AppendUint32(data, uint32(partition.leaderEpoch))
	data = binary.BigEndian.AppendUint32(data, 0) // partition epoch
	data = append(data, 2)                        // directories: 1 element
	data = append(data, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01)
	data = append(data, 0) // tagged fields
	return metadataRecord(3, 1, data)
}

// appendCompactInt32Array appends a COMPACT_ARRAY of INT32 values.
func appendCompactInt32Array(b []byte, values []int32) []byte {
	b = binary.AppendUvarint(b, uint64(len(values)+1))
	for _, v := range values {
		b = binary.BigEndian.AppendUint32(b, uint32(v))
	}
	return b
}

// testTopic describes a topic to put in a cluster metadata log.
type testTopic struct {
	name       string
	id         uuid
	partitions []int32 // the indexes of the partitions of the topic
}

// testTopicID returns a UUID like the ones that the CodeCrafters tester gives
// to its topics: 00000000-0000-4000-8000-0000000000xx.
func testTopicID(n byte) uuid {
	return uuid{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, n}
}

// clusterMetadataLog builds a cluster metadata log like the one that the
// CodeCrafters tester generates: a batch with a feature level record, then one
// batch per topic with its topic record and its partition records. All the
// partitions are led by broker 1, their only replica.
func clusterMetadataLog(topics ...testTopic) []byte {
	const timestamp = 1726045943832

	log := encodeRecordBatch(testBatch{
		baseOffset:           1,
		partitionLeaderEpoch: 1,
		timestamp:            timestamp,
		values:               [][]byte{featureLevelRecord("metadata.version", 20)},
	})

	offset := int64(2)
	for _, topic := range topics {
		values := [][]byte{topicRecord(topic.name, topic.id)}
		for _, index := range topic.partitions {
			values = append(values, partitionRecord(testPartition{
				index:    index,
				topicID:  topic.id,
				replicas: []int32{1},
				isr:      []int32{1},
				leader:   1,
			}))
		}

		log = append(log, encodeRecordBatch(testBatch{
			baseOffset:           offset,
			partitionLeaderEpoch: 1,
			timestamp:            timestamp,
			values:               values,
		})...)
		offset += int64(len(values))
	}

	return log
}

// writeLogSegment writes data as the first segment of the log of a partition
// to a broker's log directory.
func writeLogSegment(t *testing.T, logDir, topic string, partition int32, data []byte) {
	t.Helper()

	writeFile(t, filepath.Join(partitionDir(logDir, topic, partition), "00000000000000000000.log"), data)
}

// writeClusterMetadataLog writes a cluster metadata log that describes topics
// to a broker's log directory.
func writeClusterMetadataLog(t *testing.T, logDir string, topics ...testTopic) {
	t.Helper()

	writeLogSegment(t, logDir, "__cluster_metadata", 0, clusterMetadataLog(topics...))
}

// writeFile writes a file, creating its directory if needed.
func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("failed to create directory: %v", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}
}
