package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// logSegmentSuffix is the extension of the files that hold the record batches
// of a log. A log is split into segments, each of which is named after the
// offset of its first record, like 00000000000000000000.log.
const logSegmentSuffix = ".log"

// partitionDir returns the directory in which a broker that stores its logs in
// logDir keeps the log of a partition.
func partitionDir(logDir, topic string, partition int32) string {
	return filepath.Join(logDir, fmt.Sprintf("%s-%d", topic, partition))
}

// readLog returns the contents of the log stored in dir: the record batches of
// all its segments, in offset order. A directory that doesn't exist is an
// empty log.
func readLog(dir string) ([]byte, error) {
	// ReadDir sorts the entries by name, which sorts the segments by offset:
	// their names are offsets padded with zeros to the same length.
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var log []byte
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), logSegmentSuffix) {
			continue
		}

		segment, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		log = append(log, segment...)
	}

	return log, nil
}

// readRecordBatches reads the log stored in dir and returns its contents along
// with the record batches that it consists of.
func readRecordBatches(dir string) ([]byte, []recordBatch, error) {
	log, err := readLog(dir)
	if err != nil {
		return nil, nil, err
	}

	batches, err := decodeRecordBatches(log)
	if err != nil {
		return nil, nil, err
	}

	return log, batches, nil
}
