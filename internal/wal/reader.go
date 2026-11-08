
package wal

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"

	"github.com/HKNain/distributed-storage-system/internal/models"
)

// ReadAll returns all valid WAL records in sequence order.
// It does not modify the WAL file.
func (w *Writer) ReadAll() ([]models.Record, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil, fmt.Errorf("WAL writer is closed")
	}

	info, err := w.file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat WAL file: %w", err)
	}

	reader := bufio.NewReader(io.NewSectionReader(w.file, 0, info.Size()))

	var records []models.Record
	var expectedSequence uint64 = 1

	for {
		line, err := reader.ReadBytes('\n')

		if err == io.EOF {
			if len(line) == 0 {
				break
			}

			return nil, fmt.Errorf("WAL contains an incomplete record at the end")
		}

		if err != nil {
			return nil, fmt.Errorf("read WAL record: %w", err)
		}

		// Ignore empty lines, but do not silently skip malformed records.
		if len(line) == 1 && line[0] == '\n' {
			continue
		}

		var record models.Record

		if err := json.Unmarshal(line, &record); err != nil {
			return nil, fmt.Errorf("decode WAL record: %w", err)
		}

		if err := record.Validate(); err != nil {
			return nil, fmt.Errorf(
				"invalid WAL record at sequence %d: %w",
				record.Sequence,
				err,
			)
		}

		if record.Sequence != expectedSequence {
			return nil, fmt.Errorf(
				"WAL sequence mismatch: expected %d, got %d",
				expectedSequence,
				record.Sequence,
			)
		}

		records = append(records, record)
		expectedSequence++
	}

	return records, nil
}
