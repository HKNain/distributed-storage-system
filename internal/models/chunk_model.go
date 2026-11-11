package models

import "time"

type ChunkStatus string

const (
	ChunkStatusPending     ChunkStatus = "PENDING"
	ChunkStatusStored      ChunkStatus = "STORED"
	ChunkStatusFailed      ChunkStatus = "FAILED"
	ChunkStatusReplicating ChunkStatus = "REPLICATING"
)

type ReplicaStatus string

const (
	ReplicaStatusPending ReplicaStatus = "PENDING"
	ReplicaStatusStored  ReplicaStatus = "STORED"
	ReplicaStatusFailed  ReplicaStatus = "FAILED"
)

type ChunkReplica struct {
	NodeID    string        `json:"node_id"`
	Status    ReplicaStatus `json:"status"`
	UpdatedAt time.Time     `json:"updated_at"`
}

type Chunk struct {
	ID                string         `json:"id"`
	FileID            string         `json:"file_id"`
	Index             int            `json:"index"`
	Size              int64          `json:"size"`
	Hash              string         `json:"hash"`
	StorageNodeID     string         `json:"storage_node_id"` // Primary node
	ReplicationFactor int            `json:"replication_factor"`
	Replicas          []ChunkReplica `json:"replicas"`
	Status            ChunkStatus    `json:"status"`
	CreatedAt         time.Time      `json:"created_at"`
}
