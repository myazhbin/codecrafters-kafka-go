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
