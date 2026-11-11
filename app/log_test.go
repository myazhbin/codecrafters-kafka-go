package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestPartitionDir(t *testing.T) {
	tests := []struct {
		logDir    string
		topic     string
		partition int32
		want      string
	}{
		{logDir: "/tmp/kraft-combined-logs", topic: "__cluster_metadata", partition: 0, want: "/tmp/kraft-combined-logs/__cluster_metadata-0"},
		{logDir: "/tmp/kraft-combined-logs", topic: "foo", partition: 0, want: "/tmp/kraft-combined-logs/foo-0"},
		{logDir: "/data", topic: "my-topic", partition: 12, want: "/data/my-topic-12"},
	}

	for _, tt := range tests {
		if got := partitionDir(tt.logDir, tt.topic, tt.partition); got != filepath.FromSlash(tt.want) {
			t.Errorf("partitionDir(%q, %q, %d) = %q, want %q", tt.logDir, tt.topic, tt.partition, got, tt.want)
		}
	}
}

func TestReadLog(t *testing.T) {
	t.Run("missing directory is an empty log", func(t *testing.T) {
		log, err := readLog(filepath.Join(t.TempDir(), "missing-0"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(log) != 0 {
			t.Errorf("log = % x, want it to be empty", log)
		}
	})

	t.Run("empty directory is an empty log", func(t *testing.T) {
		log, err := readLog(t.TempDir())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(log) != 0 {
			t.Errorf("log = % x, want it to be empty", log)
		}
	})

	t.Run("single segment", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "00000000000000000000.log"), []byte("segment"))

		log, err := readLog(dir)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !bytes.Equal(log, []byte("segment")) {
			t.Errorf("log = %q, want %q", log, "segment")
		}
	})

	t.Run("segments are read in offset order, other files are ignored", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "00000000000000000120.log"), []byte("third"))
		writeFile(t, filepath.Join(dir, "00000000000000000000.log"), []byte("first,"))
		writeFile(t, filepath.Join(dir, "00000000000000000035.log"), []byte("second,"))
		writeFile(t, filepath.Join(dir, "00000000000000000000.index"), []byte("index"))
		writeFile(t, filepath.Join(dir, "00000000000000000000.timeindex"), []byte("timeindex"))
		writeFile(t, filepath.Join(dir, "partition.metadata"), []byte("version: 0"))
		if err := os.Mkdir(filepath.Join(dir, "directory.log"), 0o755); err != nil {
			t.Fatalf("failed to create directory: %v", err)
		}

		log, err := readLog(dir)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if want := "first,second,third"; string(log) != want {
			t.Errorf("log = %q, want %q", log, want)
		}
	})

	t.Run("path that is not a directory", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "file")
		writeFile(t, file, []byte("data"))

		if _, err := readLog(file); err == nil {
			t.Error("expected an error, got nil")
		}
	})
}

func TestReadRecordBatches(t *testing.T) {
	t.Run("log with several batches", func(t *testing.T) {
		first := encodeRecordBatch(testBatch{baseOffset: 0, values: [][]byte{[]byte("a"), []byte("b")}})
		second := encodeRecordBatch(testBatch{baseOffset: 2, values: [][]byte{[]byte("c")}})

		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "00000000000000000000.log"), append(bytes.Clone(first), second...))

		log, batches, err := readRecordBatches(dir)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !bytes.Equal(log, append(bytes.Clone(first), second...)) {
			t.Errorf("log = % x, want the contents of the segment", log)
		}
		if len(batches) != 2 {
			t.Fatalf("got %d batches, want 2", len(batches))
		}
		if !bytes.Equal(batches[0].raw, first) || !bytes.Equal(batches[1].raw, second) {
			t.Error("the raw bytes of the batches are not the batches that were written")
		}
		if batches[0].baseOffset != 0 || batches[0].lastOffset() != 1 || batches[1].baseOffset != 2 || batches[1].lastOffset() != 2 {
			t.Errorf("unexpected offsets: %d-%d and %d-%d, want 0-1 and 2-2",
				batches[0].baseOffset, batches[0].lastOffset(), batches[1].baseOffset, batches[1].lastOffset())
		}
	})

	t.Run("missing directory", func(t *testing.T) {
		log, batches, err := readRecordBatches(filepath.Join(t.TempDir(), "missing-0"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(log) != 0 || len(batches) != 0 {
			t.Errorf("got %d bytes and %d batches, want an empty log", len(log), len(batches))
		}
	})

	t.Run("corrupt log", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "00000000000000000000.log"), []byte{0x00, 0x01, 0x02})

		if _, _, err := readRecordBatches(dir); err == nil {
			t.Error("expected an error, got nil")
		}
	})

	t.Run("unreadable log", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "file")
		writeFile(t, file, []byte("data"))

		if _, _, err := readRecordBatches(file); err == nil {
			t.Error("expected an error, got nil")
		}
	})
}

func TestLogSegments(t *testing.T) {
	t.Run("missing directory", func(t *testing.T) {
		segments, err := logSegments(filepath.Join(t.TempDir(), "missing-0"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(segments) != 0 {
			t.Errorf("segments = %q, want none", segments)
		}
	})

	t.Run("segments in offset order, without the other files", func(t *testing.T) {
		dir := t.TempDir()
		for _, name := range []string{
			"00000000000000000120.log",
			"00000000000000000000.log",
			"00000000000000000035.log",
			"00000000000000000000.index",
			"partition.metadata",
		} {
			writeFile(t, filepath.Join(dir, name), nil)
		}
		if err := os.Mkdir(filepath.Join(dir, "directory.log"), 0o755); err != nil {
			t.Fatalf("failed to create directory: %v", err)
		}

		segments, err := logSegments(dir)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		want := []string{
			filepath.Join(dir, "00000000000000000000.log"),
			filepath.Join(dir, "00000000000000000035.log"),
			filepath.Join(dir, "00000000000000000120.log"),
		}
		if len(segments) != len(want) {
			t.Fatalf("segments = %q, want %q", segments, want)
		}
		for i := range want {
			if segments[i] != want[i] {
				t.Errorf("segments[%d] = %q, want %q", i, segments[i], want[i])
			}
		}
	})
}

// mustDecodeBatches decodes record batches and fails the test if that doesn't
// work.
func mustDecodeBatches(t *testing.T, data []byte) []recordBatch {
	t.Helper()

	batches, err := decodeRecordBatches(data)
	if err != nil {
		t.Fatalf("failed to decode record batches: %v", err)
	}
	return batches
}

func TestAppendToLog(t *testing.T) {
	// Producers send their batches with the base offset 0.
	single := encodeRecordBatch(testBatch{values: [][]byte{[]byte("a")}})
	triple := encodeRecordBatch(testBatch{values: [][]byte{[]byte("b"), []byte("c"), []byte("d")}})

	t.Run("new log", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "foo-0")

		baseOffset, logStartOffset, err := appendToLog(dir, mustDecodeBatches(t, single))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if baseOffset != 0 || logStartOffset != 0 {
			t.Errorf("baseOffset = %d, logStartOffset = %d; want 0, 0", baseOffset, logStartOffset)
		}

		// The directory and the first segment are created.
		got, err := os.ReadFile(filepath.Join(dir, "00000000000000000000.log"))
		if err != nil {
			t.Fatalf("failed to read the first segment: %v", err)
		}
		if !bytes.Equal(got, single) {
			t.Errorf("segment = % x, want % x", got, single)
		}
	})

	t.Run("offsets continue after the records of the log", func(t *testing.T) {
		dir := t.TempDir()

		steps := []struct {
			batches        []byte
			wantBaseOffset int64
		}{
			{batches: single, wantBaseOffset: 0},
			{batches: triple, wantBaseOffset: 1},
			{batches: single, wantBaseOffset: 4},
			// Several batches at once: they take the offsets 5 and 6-8.
			{batches: append(bytes.Clone(single), triple...), wantBaseOffset: 5},
			{batches: single, wantBaseOffset: 9},
		}
		for i, step := range steps {
			baseOffset, logStartOffset, err := appendToLog(dir, mustDecodeBatches(t, step.batches))
			if err != nil {
				t.Fatalf("step %d: unexpected error: %v", i, err)
			}
			if baseOffset != step.wantBaseOffset || logStartOffset != 0 {
				t.Errorf("step %d: baseOffset = %d, logStartOffset = %d; want %d, 0", i, baseOffset, logStartOffset, step.wantBaseOffset)
			}
		}

		// Everything went to the same segment, as valid batches whose
		// offsets follow each other.
		if names := listDir(t, dir); len(names) != 1 || names[0] != "00000000000000000000.log" {
			t.Fatalf("directory contains %q, want a single segment", names)
		}
		log, batches, err := readRecordBatches(dir)
		if err != nil {
			t.Fatalf("failed to read the log back: %v", err)
		}

		wantOffsets := [][2]int64{{0, 0}, {1, 3}, {4, 4}, {5, 5}, {6, 8}, {9, 9}}
		if len(batches) != len(wantOffsets) {
			t.Fatalf("the log holds %d batches, want %d", len(batches), len(wantOffsets))
		}
		for i, want := range wantOffsets {
			if batches[i].baseOffset != want[0] || batches[i].lastOffset() != want[1] {
				t.Errorf("batch %d has the offsets %d-%d, want %d-%d", i, batches[i].baseOffset, batches[i].lastOffset(), want[0], want[1])
			}
		}

		// Only the base offsets were changed: the rest of each batch,
		// including its CRC, is what the producer sent.
		var want []byte
		for _, step := range steps {
			want = append(want, step.batches...)
		}
		if len(log) != len(want) {
			t.Fatalf("the log is %d bytes long, want %d", len(log), len(want))
		}
		position := 0
		for i, batch := range batches {
			sent := want[position : position+len(batch.raw)]
			if !bytes.Equal(batch.raw[8:], sent[8:]) {
				t.Errorf("batch %d was changed beyond its base offset", i)
			}
			position += len(batch.raw)
		}
	})

	t.Run("records go to the last segment", func(t *testing.T) {
		dir := t.TempDir()
		first := encodeRecordBatch(testBatch{baseOffset: 10, values: [][]byte{[]byte("x"), []byte("y")}})
		second := encodeRecordBatch(testBatch{baseOffset: 12, values: [][]byte{[]byte("z")}})
		writeFile(t, filepath.Join(dir, "00000000000000000010.log"), first)
		writeFile(t, filepath.Join(dir, "00000000000000000012.log"), second)

		baseOffset, logStartOffset, err := appendToLog(dir, mustDecodeBatches(t, single))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if baseOffset != 13 || logStartOffset != 10 {
			t.Errorf("baseOffset = %d, logStartOffset = %d; want 13, 10", baseOffset, logStartOffset)
		}

		if got, _ := os.ReadFile(filepath.Join(dir, "00000000000000000010.log")); !bytes.Equal(got, first) {
			t.Error("the first segment was changed")
		}
		got, err := os.ReadFile(filepath.Join(dir, "00000000000000000012.log"))
		if err != nil {
			t.Fatalf("failed to read the last segment: %v", err)
		}
		if !bytes.HasPrefix(got, second) || len(got) != len(second)+len(single) {
			t.Errorf("the last segment is %d bytes long, want the %d bytes it had followed by %d new ones", len(got), len(second), len(single))
		}
		if names := listDir(t, dir); len(names) != 2 {
			t.Errorf("directory contains %q, want the two segments it had", names)
		}
	})

	t.Run("the batches that are passed in are not modified", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, filepath.Join(dir, "00000000000000000000.log"), triple)

		sent := bytes.Clone(single)
		if _, _, err := appendToLog(dir, mustDecodeBatches(t, sent)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !bytes.Equal(sent, single) {
			t.Error("appendToLog changed the bytes of the batch it was given")
		}
	})

	t.Run("corrupt log", func(t *testing.T) {
		dir := t.TempDir()
		corrupt := []byte{0x00, 0x01, 0x02}
		writeFile(t, filepath.Join(dir, "00000000000000000000.log"), corrupt)

		if _, _, err := appendToLog(dir, mustDecodeBatches(t, single)); err == nil {
			t.Error("expected an error, got nil")
		}
		if got, _ := os.ReadFile(filepath.Join(dir, "00000000000000000000.log")); !bytes.Equal(got, corrupt) {
			t.Error("the corrupt log was written to")
		}
	})

	t.Run("directory that can't be created", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "file")
		writeFile(t, file, []byte("data"))

		if _, _, err := appendToLog(filepath.Join(file, "foo-0"), mustDecodeBatches(t, single)); err == nil {
			t.Error("expected an error, got nil")
		}
	})
}
