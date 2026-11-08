package wal

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/HKNain/distributed-storage-system/internal/models"
)

type Writer struct {
	mu       sync.Mutex
	file     *os.File
	sequence uint64
	failed   error
	closed   bool
}

// OpenWriter creates or opens an existing WAL file.
func OpenWriter(path string) (*Writer, error) {
	if path == "" {
		return nil, errors.New("WAL path cannot be empty")
	}

	dir := filepath.Dir(path)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create WAL directory: %w", err)
	}

	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_RDWR,
		0644,
	)
	if err != nil {
		return nil, fmt.Errorf("open WAL file: %w", err)
	}

	writer := &Writer{
		file: file,
	}

	// Recover the last valid sequence number.
	if err := writer.recoverSequence(); err != nil {
		file.Close()
		return nil, fmt.Errorf("recover WAL sequence: %w", err)
	}

	return writer, nil
}

// Append validates, sequences, writes and synchronizes a WAL record.
func (w *Writer) Append(record models.Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return errors.New("WAL writer is closed")
	}

	if w.failed != nil {
		return fmt.Errorf("WAL writer is in failed state: %w", w.failed)
	}

	record.Version = models.CurrentVersion
	record.Sequence = w.sequence + 1

	if record.Timestamp.IsZero() {
		record.Timestamp = time.Now().UTC()
	}

	if err := record.Validate(); err != nil {
		return fmt.Errorf("invalid WAL record: %w", err)
	}

	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal WAL record: %w", err)
	}

	// Every record occupies one complete line.
	data = append(data, '\n')

	n, err := w.file.Write(data)
	if err != nil {
		w.failed = fmt.Errorf("write WAL record: %w", err)
		return w.failed
	}

	if n != len(data) {
		w.failed = errors.New("incomplete WAL write")
		return w.failed
	}

	// Ensure the record reaches durable storage before returning.
	if err := w.file.Sync(); err != nil {
		w.failed = fmt.Errorf("sync WAL record: %w", err)
		return w.failed
	}

	w.sequence = record.Sequence

	return nil
}

// recoverSequence restores the sequence number from existing records.
// An incomplete final line is discarded.
func (w *Writer) recoverSequence() error {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	reader := bufio.NewReader(w.file)

	var (
		lastSequence uint64
		validOffset  int64
	)

	for {
		line, err := reader.ReadBytes('\n')

		if err == io.EOF {
			// A non-empty line without a newline is an incomplete
			// trailing record, so discard it.
			if len(line) > 0 {
				if truncateErr := w.file.Truncate(validOffset); truncateErr != nil {
					return fmt.Errorf("truncate incomplete WAL record: %w", truncateErr)
				}
			}
			break
		}

		if err != nil {
			return fmt.Errorf("read WAL file: %w", err)
		}

		var record models.Record

		if err := json.Unmarshal(line, &record); err != nil {
			return fmt.Errorf(
				"corrupted WAL record at offset %d: %w",
				validOffset,
				err,
			)
		}

		if err := record.Validate(); err != nil {
			return fmt.Errorf(
				"invalid WAL record at offset %d: %w",
				validOffset,
				err,
			)
		}

		if record.Sequence != lastSequence+1 {
			return fmt.Errorf(
				"invalid WAL sequence: expected %d, got %d",
				lastSequence+1,
				record.Sequence,
			)
		}

		lastSequence = record.Sequence
		validOffset += int64(len(line))
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync recovered WAL: %w", err)
	}

	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	w.sequence = lastSequence

	return nil
}

// Close synchronizes and closes the WAL file.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	w.closed = true

	syncErr := w.file.Sync()
	closeErr := w.file.Close()

	if syncErr != nil {
		return fmt.Errorf("sync WAL on close: %w", syncErr)
	}

	if closeErr != nil {
		return fmt.Errorf("close WAL: %w", closeErr)
	}

	return nil
}
