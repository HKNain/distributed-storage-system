
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
	EventChunkRepairStarted   EventType = "CHUNK_REPAIR_STARTED"
	EventChunkRepairCommitted EventType = "CHUNK_REPAIR_COMMITTED"
	EventChunkRepairFailed    EventType = "CHUNK_REPAIR_FAILED"
)

type Record struct {
	Version   int       `json:"version"`
	Sequence  uint64    `json:"sequence"`
	Timestamp time.Time `json:"timestamp"`
	Event     EventType `json:"event"`

	FileID      string `json:"file_id"`
	StagingPath string `json:"staging_path,omitempty"`
	FileSize    int64  `json:"file_size,omitempty"`

	ChunkID    string `json:"chunk_id,omitempty"`
	ChunkIndex int    `json:"chunk_index"`
	ChunkSize  int64  `json:"chunk_size,omitempty"`
	Checksum   string `json:"checksum,omitempty"`

	// Repair operation details.
	RepairID      string `json:"repair_id,omitempty"`
	SourceNodeID  string `json:"source_node_id,omitempty"`
	RepairReason  string `json:"repair_reason,omitempty"`

	// Primary storage node.
	NodeID string `json:"node_id,omitempty"`

	// Replica destinations, excluding the primary.
	// For replication factor 3, this contains two node IDs.
	ReplicaNodeIDs []string `json:"replica_node_ids,omitempty"`

	ChunkCount int    `json:"chunk_count,omitempty"`
	Error      string `json:"error,omitempty"`
}

func (r Record) Validate() error {
	if r.Version != CurrentVersion {
		return errors.New("unsupported WAL record version")
	}

	if r.Sequence == 0 {
		return errors.New("WAL sequence must be greater than zero")
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

	case EventChunkWriteStarted:
		if err := r.validateChunkFields(); err != nil {
			return err
		}
		if err := r.validateReplicaNodes(); err != nil {
			return err
		}

	case EventChunkWriteCommitted:
		return r.validateChunkFields()

	case EventChunkWriteFailed:
		if err := r.validateChunkFields(); err != nil {
			return err
		}
		if strings.TrimSpace(r.Error) == "" {
			return errors.New("error is required for failed chunk writes")
		}

	case EventUploadCompleted:
		if r.ChunkCount < 0 {
			return errors.New("chunk count cannot be negative")
		}
		if !isValidChecksum(r.Checksum) {
			return errors.New("invalid upload checksum")
		}

	case EventUploadFailed:
		if strings.TrimSpace(r.Error) == "" {
			return errors.New("error is required for failed uploads")
		}

	case EventChunkRepairStarted,
		EventChunkRepairCommitted,
		EventChunkRepairFailed:

		if err := r.validateRepairFields(); err != nil {
			return err
		}

	default:
		return errors.New("unknown WAL event")
	}

	return nil
}

func (r Record) validateChunkFields() error {
	if strings.TrimSpace(r.ChunkID) == "" {
		return errors.New("chunk ID is required")
	}

	if strings.TrimSpace(r.NodeID) == "" {
		return errors.New("storage node ID is required")
	}

	if r.ChunkIndex < 0 {
		return errors.New("chunk index cannot be negative")
	}

	if r.ChunkSize <= 0 {
		return errors.New("chunk size must be greater than zero")
	}

	if !isValidChecksum(r.Checksum) {
		return errors.New("invalid chunk checksum")
	}

	return nil
}

func (r Record) validateReplicaNodes() error {
	seen := make(map[string]struct{}, len(r.ReplicaNodeIDs))

	for _, nodeID := range r.ReplicaNodeIDs {
		nodeID = strings.TrimSpace(nodeID)

		if nodeID == "" {
			return errors.New("replica node ID cannot be empty")
		}

		if nodeID == r.NodeID {
			return errors.New("primary node cannot also be a replica")
		}

		if _, exists := seen[nodeID]; exists {
			return errors.New("duplicate replica node ID")
		}

		seen[nodeID] = struct{}{}
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

func (r Record) validateRepairFields() error {
    if strings.TrimSpace(r.RepairID) == "" {
        return errors.New("repair ID is required")
    }

    if strings.TrimSpace(r.ChunkID) == "" {
        return errors.New("chunk ID is required")
    }

    if strings.TrimSpace(r.NodeID) == "" {
        return errors.New("repair target node ID is required")
    }

    if r.ChunkSize <= 0 {
        return errors.New("repair chunk size must be positive")
    }

    if len(r.Checksum) != 64 {
        return errors.New("invalid repair checksum length")
    }

    decoded, err := hex.DecodeString(r.Checksum)
    if err != nil || len(decoded) != 32 {
        return errors.New("invalid repair checksum")
    }

    switch r.Event {
    case EventChunkRepairStarted:
        if strings.TrimSpace(r.RepairReason) == "" {
            return errors.New("repair reason is required")
        }

    case EventChunkRepairCommitted:
        if strings.TrimSpace(r.SourceNodeID) == "" {
            return errors.New("repair source node ID is required")
        }

    case EventChunkRepairFailed:
        if strings.TrimSpace(r.Error) == "" {
            return errors.New("repair failure reason is required")
        }
    }

    return nil
}