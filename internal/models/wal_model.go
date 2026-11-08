package models

import (
	"encoding/hex"
	"errors"
	"strings"
	"time"
)

const CurrentVersion = 1

type EventType string

const (
	EventUploadStarted       EventType = "UPLOAD_STARTED"
	EventChunkWriteStarted   EventType = "CHUNK_WRITE_STARTED"
	EventChunkWriteCommitted EventType = "CHUNK_WRITE_COMMITTED"
	EventChunkWriteFailed    EventType = "CHUNK_WRITE_FAILED"
	EventUploadCompleted     EventType = "UPLOAD_COMPLETED"
	EventUploadFailed        EventType = "UPLOAD_FAILED"
)

type Record struct {
	Version   int       `json:"version"`
	Sequence  uint64    `json:"sequence"`
	Timestamp time.Time `json:"timestamp"`
	Event     EventType `json:"event"`

	// Upload information
	FileID      string `json:"file_id"`
	StagingPath string `json:"staging_path,omitempty"`
	FileSize    int64  `json:"file_size,omitempty"`

	// Chunk information
	ChunkID    string `json:"chunk_id,omitempty"`
	ChunkIndex int    `json:"chunk_index"`
	ChunkSize  int64  `json:"chunk_size,omitempty"`
	Checksum   string `json:"checksum,omitempty"`

	// Storage node information
	NodeID string `json:"node_id,omitempty"`

	// Completion information
	ChunkCount int `json:"chunk_count,omitempty"`

	// Failure information
	Error string `json:"error,omitempty"`
}

func (r Record) Validate() error {
	if r.Version != CurrentVersion {
		return errors.New("unsupported WAL record version")
	}

	if r.Sequence == 0 {
		return errors.New("WAL sequence cannot be zero")
	}

	if r.Timestamp.IsZero() {
		return errors.New("WAL timestamp is required")
	}

	if strings.TrimSpace(r.FileID) == "" {
		return errors.New("file ID is required")
	}

	switch r.Event {

	case EventUploadStarted:
		if r.StagingPath == "" {
			return errors.New("staging path is required")
		}

		if r.FileSize < 0 {
			return errors.New("file size cannot be negative")
		}

	case EventChunkWriteStarted,
		EventChunkWriteCommitted,
		EventChunkWriteFailed:

		if r.ChunkID == "" {
			return errors.New("chunk ID is required")
		}

		if r.NodeID == "" {
			return errors.New("storage node ID is required")
		}

		if r.ChunkIndex < 0 {
			return errors.New("chunk index cannot be negative")
		}

		if r.ChunkSize <= 0 {
			return errors.New("chunk size must be positive")
		}

		if !isValidChecksum(r.Checksum) {
			return errors.New("invalid SHA-256 checksum")
		}

		if r.Event == EventChunkWriteFailed && r.Error == "" {
			return errors.New("failure reason is required")
		}

	case EventUploadCompleted:
		if r.ChunkCount < 0 {
			return errors.New("chunk count cannot be negative")
		}

		if !isValidChecksum(r.Checksum) {
			return errors.New("invalid file checksum")
		}

	case EventUploadFailed:
		if r.Error == "" {
			return errors.New("failure reason is required")
		}

	default:
		return errors.New("unknown WAL event type")
	}

	return nil
}

func isValidChecksum(checksum string) bool {
	if len(checksum) != 64 {
		return false
	}

	decoded, err := hex.DecodeString(checksum)
	return err == nil && len(decoded) == 32
}
