package main

import (
	"encoding/binary"
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

// logSegments returns the paths of the segment files of the log stored in
// dir, in offset order. A directory that doesn't exist is an empty log.
func logSegments(dir string) ([]string, error) {
	// ReadDir sorts the entries by name, which sorts the segments by offset:
	// their names are offsets padded with zeros to the same length.
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var segments []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), logSegmentSuffix) {
			segments = append(segments, filepath.Join(dir, entry.Name()))
		}
	}
	return segments, nil
}

// readLog returns the contents of the log stored in dir: the record batches of
// all its segments, in offset order.
func readLog(dir string) ([]byte, error) {
	segments, err := logSegments(dir)
	if err != nil {
		return nil, err
	}

	var log []byte
	for _, segment := range segments {
		data, err := os.ReadFile(segment)
		if err != nil {
			return nil, err
		}
		log = append(log, data...)
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

// appendToLog appends record batches to the log stored in dir, which is
// created if it doesn't exist yet. The records get the offsets that follow
// the ones of the records that the log already holds: the base offset that
// the producer gave to each batch is replaced.
//
// It returns the offset of the first record that was appended, and the offset
// of the first record of the log.
func appendToLog(dir string, batches []recordBatch) (baseOffset, logStartOffset int64, err error) {
	_, existing, err := readRecordBatches(dir)
	if err != nil {
		return 0, 0, err
	}

	// A log without records starts and ends at the offset 0.
	var logEndOffset int64
	if len(existing) > 0 {
		logStartOffset = existing[0].baseOffset
		logEndOffset = existing[len(existing)-1].lastOffset() + 1
	}

	// The base offset is the first field of a batch. It is not covered by
	// the CRC of the batch, so that it can be set without redoing the CRC.
	var data []byte
	nextOffset := logEndOffset
	for _, batch := range batches {
		data = binary.BigEndian.AppendUint64(data, uint64(nextOffset))
		data = append(data, batch.raw[8:]...)
		nextOffset += int64(batch.lastOffsetDelta) + 1
	}

	// Records are appended to the last segment of the log. A log without
	// segments gets a first one, named after the offset of its first record.
	segments, err := logSegments(dir)
	if err != nil {
		return 0, 0, err
	}
	segment := filepath.Join(dir, fmt.Sprintf("%020d%s", logEndOffset, logSegmentSuffix))
	if len(segments) > 0 {
		segment = segments[len(segments)-1]
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, 0, err
	}
	file, err := os.OpenFile(segment, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return 0, 0, err
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return 0, 0, err
	}
	if err := file.Close(); err != nil {
		return 0, 0, err
	}

	return logEndOffset, logStartOffset, nil
}
