package models

import "time"

type ChunkStatus string

const (
	ChunkStatusPending     ChunkStatus = "PENDING"
	ChunkStatusStored      ChunkStatus = "STORED"
	ChunkStatusFailed      ChunkStatus = "FAILED"
	ChunkStatusReplicating ChunkStatus = "REPLICATING"
)

type Chunk struct {
	ID            string      `json:"id"`
	FileID        string      `json:"file_id"`
	Index         int         `json:"index"`
	Size          int64       `json:"size"`
	Hash          string      `json:"hash"`
	StorageNodeID string      `json:"storage_node_id"`
	Status        ChunkStatus `json:"status"`
	CreatedAt     time.Time   `json:"created_at"`
}
