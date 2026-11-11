package main

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// loadTestMetadata loads the cluster metadata from a log directory and fails
// the test if that doesn't work.
func loadTestMetadata(t *testing.T, logDir string) *clusterMetadata {
	t.Helper()

	metadata, err := loadClusterMetadata(logDir)
	if err != nil {
		t.Fatalf("failed to load cluster metadata: %v", err)
	}
	return metadata
}

func TestLoadClusterMetadata(t *testing.T) {
	t.Run("CodeCrafters cluster metadata log", func(t *testing.T) {
		logDir := t.TempDir()
		writeLogSegment(t, logDir, "__cluster_metadata", 0, mustDecodeHex(t, featureLevelBatchHex+sazTopicBatchHex))

		metadata := loadTestMetadata(t, logDir)

		want := &topicMetadata{
			name: "saz",
			id:   sazTopicID,
			partitions: []partitionMetadata{
				{index: 1, leader: 1, leaderEpoch: 0, replicas: []int32{1}, isr: []int32{1}},
			},
		}
		if len(metadata.topicsByName) != 1 || len(metadata.topicsByID) != 1 {
			t.Fatalf("got %d topics by name and %d by ID, want 1 of each", len(metadata.topicsByName), len(metadata.topicsByID))
		}
		if got := metadata.topicsByName["saz"]; !reflect.DeepEqual(got, want) {
			t.Errorf("topic by name = %+v, want %+v", got, want)
		}
		if got := metadata.topicsByID[sazTopicID]; !reflect.DeepEqual(got, want) {
			t.Errorf("topic by ID = %+v, want %+v", got, want)
		}
		if got := sazTopicID.String(); got != "00000000-0000-4000-8000-000000000091" {
			t.Errorf("topic ID = %s, want 00000000-0000-4000-8000-000000000091", got)
		}
	})

	t.Run("several topics and partitions", func(t *testing.T) {
		logDir := t.TempDir()
		writeClusterMetadataLog(t, logDir,
			testTopic{name: "bar", id: testTopicID(0x80), partitions: []int32{0}},
			testTopic{name: "baz", id: testTopicID(0x81), partitions: []int32{0, 1}},
			testTopic{name: "paz", id: testTopicID(0x82), partitions: []int32{0, 1, 2}},
			testTopic{name: "empty", id: testTopicID(0x83)},
		)

		metadata := loadTestMetadata(t, logDir)

		if len(metadata.topicsByName) != 4 || len(metadata.topicsByID) != 4 {
			t.Fatalf("got %d topics by name and %d by ID, want 4 of each", len(metadata.topicsByName), len(metadata.topicsByID))
		}

		for name, want := range map[string]struct {
			id         uuid
			partitions []int32
		}{
			"bar":   {id: testTopicID(0x80), partitions: []int32{0}},
			"baz":   {id: testTopicID(0x81), partitions: []int32{0, 1}},
			"paz":   {id: testTopicID(0x82), partitions: []int32{0, 1, 2}},
			"empty": {id: testTopicID(0x83), partitions: nil},
		} {
			topic, ok := metadata.topicsByName[name]
			if !ok {
				t.Errorf("topic %q is missing", name)
				continue
			}
			if topic.name != name || topic.id != want.id {
				t.Errorf("topic %q = {name: %q, id: %v}, want ID %v", name, topic.name, topic.id, want.id)
			}
			if metadata.topicsByID[want.id] != topic {
				t.Errorf("topic %q is not the topic found by its ID", name)
			}

			var partitions []int32
			for _, partition := range topic.partitions {
				partitions = append(partitions, partition.index)

				if partition.leader != 1 || !reflect.DeepEqual(partition.replicas, []int32{1}) || !reflect.DeepEqual(partition.isr, []int32{1}) {
					t.Errorf("topic %q: unexpected partition %+v", name, partition)
				}
			}
			if !reflect.DeepEqual(partitions, want.partitions) {
				t.Errorf("topic %q has partitions %v, want %v", name, partitions, want.partitions)
			}
		}
	})

	t.Run("no log directory", func(t *testing.T) {
		metadata := loadTestMetadata(t, filepath.Join(t.TempDir(), "missing"))

		if len(metadata.topicsByName) != 0 || len(metadata.topicsByID) != 0 {
			t.Errorf("expected no topics, got %+v", metadata)
		}
	})

	t.Run("no cluster metadata log", func(t *testing.T) {
		metadata := loadTestMetadata(t, t.TempDir())

		if len(metadata.topicsByName) != 0 || len(metadata.topicsByID) != 0 {
			t.Errorf("expected no topics, got %+v", metadata)
		}
	})

	t.Run("empty cluster metadata log", func(t *testing.T) {
		logDir := t.TempDir()
		writeLogSegment(t, logDir, "__cluster_metadata", 0, nil)

		metadata := loadTestMetadata(t, logDir)

		if len(metadata.topicsByName) != 0 {
			t.Errorf("expected no topics, got %+v", metadata)
		}
	})

	t.Run("log split into several segments", func(t *testing.T) {
		logDir := t.TempDir()
		dir := partitionDir(logDir, "__cluster_metadata", 0)
		writeFile(t, filepath.Join(dir, "00000000000000000000.log"), mustDecodeHex(t, featureLevelBatchHex))
		writeFile(t, filepath.Join(dir, "00000000000000000002.log"), mustDecodeHex(t, sazTopicBatchHex))

		metadata := loadTestMetadata(t, logDir)

		topic, ok := metadata.topicsByName["saz"]
		if !ok || len(topic.partitions) != 1 {
			t.Errorf("topic saz = %+v, want it to have one partition", topic)
		}
	})

	t.Run("control batches are skipped", func(t *testing.T) {
		// The record of a control batch isn't a metadata record: replaying
		// it would fail.
		log := encodeRecordBatch(testBatch{baseOffset: 0, attributes: 0x20, values: [][]byte{{0xff, 0xff, 0xff}}})
		log = append(log, encodeRecordBatch(testBatch{baseOffset: 1, values: [][]byte{topicRecord("foo", testTopicID(1))}})...)

		logDir := t.TempDir()
		writeLogSegment(t, logDir, "__cluster_metadata", 0, log)

		metadata := loadTestMetadata(t, logDir)

		if _, ok := metadata.topicsByName["foo"]; !ok || len(metadata.topicsByName) != 1 {
			t.Errorf("expected the topic foo only, got %+v", metadata.topicsByName)
		}
	})

	errorTests := []struct {
		name    string
		log     []byte
		wantErr string
	}{
		{
			name:    "corrupt log",
			log:     mustDecodeHex(t, featureLevelBatchHex)[:40],
			wantErr: "truncated",
		},
		{
			name:    "corrupt record",
			log:     encodeRecordBatch(testBatch{baseOffset: 5, values: [][]byte{topicRecord("foo", testTopicID(1))[:6]}}),
			wantErr: "offset 5",
		},
		{
			name: "partition of an unknown topic",
			log: encodeRecordBatch(testBatch{baseOffset: 7, values: [][]byte{
				topicRecord("foo", testTopicID(1)),
				partitionRecord(testPartition{index: 0, topicID: testTopicID(2)}),
			}}),
			wantErr: "offset 8",
		},
	}

	for _, tt := range errorTests {
		t.Run(tt.name, func(t *testing.T) {
			logDir := t.TempDir()
			writeLogSegment(t, logDir, "__cluster_metadata", 0, tt.log)

			_, err := loadClusterMetadata(logDir)
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %q, want it to mention %q", err, tt.wantErr)
			}
		})
	}
}

func TestClusterMetadataReplay(t *testing.T) {
	t.Run("topic record", func(t *testing.T) {
		metadata := newClusterMetadata()

		// The topic record of the CodeCrafters cluster metadata log.
		if err := metadata.replay(mustDecodeHex(t, "01 02 00 04 73617a 00000000000040008000000000000091 00")); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		topic, ok := metadata.topicsByName["saz"]
		if !ok {
			t.Fatal("topic saz is missing")
		}
		if topic.id != sazTopicID || len(topic.partitions) != 0 {
			t.Errorf("topic = %+v, want ID %v and no partitions", topic, sazTopicID)
		}
		if metadata.topicsByID[sazTopicID] != topic {
			t.Error("the topic can't be found by its ID")
		}
	})

	t.Run("partition record v1", func(t *testing.T) {
		metadata := newClusterMetadata()
		mustReplay(t, metadata, topicRecord("saz", sazTopicID))

		// The partition record of the CodeCrafters cluster metadata log.
		mustReplay(t, metadata, mustDecodeHex(t, "01 03 01 00000001 00000000000040008000000000000091"+
			"02 00000001 02 00000001 01 01 00000001 00000000 00000000 02 10000000000040008000000000000001 00"))

		want := []partitionMetadata{{index: 1, leader: 1, leaderEpoch: 0, replicas: []int32{1}, isr: []int32{1}}}
		if got := metadata.topicsByName["saz"].partitions; !reflect.DeepEqual(got, want) {
			t.Errorf("partitions = %+v, want %+v", got, want)
		}
	})

	t.Run("partition record v0 has no directories", func(t *testing.T) {
		metadata := newClusterMetadata()
		mustReplay(t, metadata, topicRecord("saz", sazTopicID))

		mustReplay(t, metadata, mustDecodeHex(t, "01 03 00 00000002 00000000000040008000000000000091"+
			"04 00000003 00000001 00000002"+ // replicas: [3, 1, 2]
			"03 00000003 00000002"+ // in-sync replicas: [3, 2]
			"01 01"+ // removing and adding replicas: []
			"00000003"+ // leader: 3
			"00000005"+ // leader epoch: 5
			"00000009"+ // partition epoch: 9
			"00")) // tagged fields

		want := []partitionMetadata{{index: 2, leader: 3, leaderEpoch: 5, replicas: []int32{3, 1, 2}, isr: []int32{3, 2}}}
		if got := metadata.topicsByName["saz"].partitions; !reflect.DeepEqual(got, want) {
			t.Errorf("partitions = %+v, want %+v", got, want)
		}
	})

	t.Run("partition record with tagged fields", func(t *testing.T) {
		metadata := newClusterMetadata()
		mustReplay(t, metadata, topicRecord("saz", sazTopicID))

		// Version 2, with the tagged fields for the leader recovery state
		// (tag 0) and the eligible leader replicas (tag 1).
		mustReplay(t, metadata, mustDecodeHex(t, "01 03 02 00000000 00000000000040008000000000000091"+
			"02 00000001 02 00000001 01 01 00000001 00000000 00000000 02 10000000000040008000000000000001"+
			"02 00 01 01 01 05 02 00000001"))

		want := []partitionMetadata{{index: 0, leader: 1, leaderEpoch: 0, replicas: []int32{1}, isr: []int32{1}}}
		if got := metadata.topicsByName["saz"].partitions; !reflect.DeepEqual(got, want) {
			t.Errorf("partitions = %+v, want %+v", got, want)
		}
	})

	t.Run("partitions are kept sorted by index", func(t *testing.T) {
		metadata := newClusterMetadata()
		mustReplay(t, metadata, topicRecord("foo", testTopicID(1)))

		for _, index := range []int32{2, 0, 3, 1} {
			mustReplay(t, metadata, partitionRecord(testPartition{index: index, topicID: testTopicID(1), leader: 1}))
		}

		var indexes []int32
		for _, partition := range metadata.topicsByName["foo"].partitions {
			indexes = append(indexes, partition.index)
		}
		if want := []int32{0, 1, 2, 3}; !reflect.DeepEqual(indexes, want) {
			t.Errorf("partition indexes = %v, want %v", indexes, want)
		}
	})

	t.Run("hasPartition", func(t *testing.T) {
		metadata := newClusterMetadata()
		mustReplay(t, metadata, topicRecord("foo", testTopicID(1)))
		mustReplay(t, metadata, topicRecord("bar", testTopicID(2)))
		for _, index := range []int32{4, 0, 2} {
			mustReplay(t, metadata, partitionRecord(testPartition{index: index, topicID: testTopicID(1), leader: 1}))
		}

		foo := metadata.topicsByName["foo"]
		for index, want := range map[int32]bool{-1: false, 0: true, 1: false, 2: true, 3: false, 4: true, 5: false} {
			if got := foo.hasPartition(index); got != want {
				t.Errorf("foo.hasPartition(%d) = %v, want %v", index, got, want)
			}
		}

		if metadata.topicsByName["bar"].hasPartition(0) {
			t.Error("bar.hasPartition(0) = true, want false for a topic without partitions")
		}
	})

	t.Run("later partition record replaces the earlier one", func(t *testing.T) {
		metadata := newClusterMetadata()
		mustReplay(t, metadata, topicRecord("foo", testTopicID(1)))
		mustReplay(t, metadata, partitionRecord(testPartition{index: 0, topicID: testTopicID(1), leader: 1, replicas: []int32{1}}))
		mustReplay(t, metadata, partitionRecord(testPartition{index: 1, topicID: testTopicID(1), leader: 1, replicas: []int32{1}}))
		mustReplay(t, metadata, partitionRecord(testPartition{index: 0, topicID: testTopicID(1), leader: 2, leaderEpoch: 1, replicas: []int32{2}}))

		want := []partitionMetadata{
			{index: 0, leader: 2, leaderEpoch: 1, replicas: []int32{2}},
			{index: 1, leader: 1, leaderEpoch: 0, replicas: []int32{1}},
		}
		if got := metadata.topicsByName["foo"].partitions; !reflect.DeepEqual(got, want) {
			t.Errorf("partitions = %+v, want %+v", got, want)
		}
	})

	t.Run("partitions go to the topic with the matching ID", func(t *testing.T) {
		metadata := newClusterMetadata()
		mustReplay(t, metadata, topicRecord("foo", testTopicID(1)))
		mustReplay(t, metadata, topicRecord("bar", testTopicID(2)))
		mustReplay(t, metadata, partitionRecord(testPartition{index: 0, topicID: testTopicID(2), leader: 1}))

		if got := len(metadata.topicsByName["foo"].partitions); got != 0 {
			t.Errorf("foo has %d partitions, want 0", got)
		}
		if got := len(metadata.topicsByName["bar"].partitions); got != 1 {
			t.Errorf("bar has %d partitions, want 1", got)
		}
	})

	t.Run("other record types are ignored", func(t *testing.T) {
		metadata := newClusterMetadata()

		records := [][]byte{
			featureLevelRecord("metadata.version", 20),
			metadataRecord(0, 0, []byte{0xde, 0xad}),  // broker registration
			metadataRecord(20, 0, []byte{0x00}),       // no-op
			metadataRecord(127, 3, nil),               // a type that doesn't exist yet
			mustDecodeHex(t, "01 8002 00 aabbccddee"), // a type that takes two bytes
		}
		for _, value := range records {
			mustReplay(t, metadata, value)
		}

		if len(metadata.topicsByName) != 0 || len(metadata.topicsByID) != 0 {
			t.Errorf("expected no topics, got %+v", metadata)
		}
	})

	errorTests := []struct {
		name  string
		value string
	}{
		{name: "null value", value: ""},
		{name: "missing record type", value: "01"},
		{name: "missing record version", value: "01 02"},
		{name: "unsupported frame version", value: "02 02 00 04 73617a 00000000000040008000000000000091 00"},
		{name: "truncated topic name", value: "01 02 00 04 7361"},
		{name: "truncated topic ID", value: "01 02 00 04 73617a 000000000000400080"},
		{name: "topic record without tagged fields", value: "01 02 00 04 73617a 00000000000040008000000000000091"},
		{name: "truncated partition record", value: "01 03 01 00000001 00000000000040008000000000000091 02 00000001"},
		{
			name: "partition record v1 without directories",
			value: "01 03 01 00000001 00000000000040008000000000000091" +
				"02 00000001 02 00000001 01 01 00000001 00000000 00000000",
		},
		{
			name: "partition of an unknown topic",
			value: "01 03 01 00000001 00000000000040008000000000000092" +
				"02 00000001 02 00000001 01 01 00000001 00000000 00000000 02 10000000000040008000000000000001 00",
		},
	}

	for _, tt := range errorTests {
		t.Run(tt.name, func(t *testing.T) {
			metadata := newClusterMetadata()
			mustReplay(t, metadata, topicRecord("saz", sazTopicID))

			if err := metadata.replay(mustDecodeHex(t, tt.value)); err == nil {
				t.Error("expected an error, got nil")
			}
		})
	}
}

// mustReplay applies a record to the metadata and fails the test if the
// record is rejected.
func mustReplay(t *testing.T, metadata *clusterMetadata, value []byte) {
	t.Helper()

	if err := metadata.replay(value); err != nil {
		t.Fatalf("failed to replay record % x: %v", value, err)
	}
}
