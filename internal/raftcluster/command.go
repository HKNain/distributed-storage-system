
package raftcluster

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/HKNain/distributed-storage-system/internal/models"
)

type MutationType string

const (
	MutationPut    MutationType = "PUT"
	MutationDelete MutationType = "DELETE"
)

type Mutation struct {
	Type   MutationType `json:"type"`
	Bucket string       `json:"bucket"`
	Key    []byte       `json:"key"`
	Value  []byte       `json:"value,omitempty"`
}

type CommandOperation string

const (
	OperationMutate             CommandOperation = "MUTATE"
	OperationCreateUser         CommandOperation = "CREATE_USER"
	OperationCreateFile         CommandOperation = "CREATE_FILE"
	OperationUpdateFileStatus   CommandOperation = "UPDATE_FILE_STATUS"
	OperationUpdateUploadDetails CommandOperation = "UPDATE_UPLOAD_DETAILS"
	OperationCreateChunk        CommandOperation = "CREATE_CHUNK"
	OperationUpdateChunkStatus  CommandOperation = "UPDATE_CHUNK_STATUS"
	OperationUpdateChunkReplica CommandOperation = "UPDATE_CHUNK_REPLICA"
)

type Command struct {
	ID        string           `json:"id"`
	Operation CommandOperation `json:"operation"`
	Payload   []byte           `json:"payload,omitempty"`
	Mutations []Mutation       `json:"mutations,omitempty"`
}

// Payload structures for domain-specific metadata operations.

type CreateFilePayload struct {
	File models.File `json:"file"`
}

type UpdateFileStatusPayload struct {
	FileID    string           `json:"file_id"`
	Status    models.FileStatus `json:"status"`
	UpdatedAt time.Time        `json:"updated_at"`
}

type UpdateUploadDetailsPayload struct {
	FileID     string    `json:"file_id"`
	Checksum   string    `json:"checksum"`
	ChunkCount int       `json:"chunk_count"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type CreateChunkPayload struct {
	Chunk models.Chunk `json:"chunk"`
}

type UpdateChunkStatusPayload struct {
	ChunkID string            `json:"chunk_id"`
	Status  models.ChunkStatus `json:"status"`
}

type UpdateChunkReplicaPayload struct {
	ChunkID   string               `json:"chunk_id"`
	NodeID    string               `json:"node_id"`
	Status    models.ReplicaStatus `json:"status"`
	UpdatedAt time.Time            `json:"updated_at"`
}

func (c *Command) Validate() error {
	if c == nil {
		return fmt.Errorf("command is nil")
	}

	if c.ID == "" {
		return fmt.Errorf("command ID is required")
	}

	switch c.Operation {
	case OperationCreateUser:
		return validatePayloadOnly(c, "CREATE_USER")

	case OperationCreateFile:
		var payload CreateFilePayload
		if err := decodePayload(c, &payload); err != nil {
			return err
		}

		if payload.File.ID == "" || payload.File.UserID == "" || payload.File.Name == "" {
			return fmt.Errorf("CREATE_FILE: file ID, user ID and name are required")
		}
		if payload.File.Size < 0 {
			return fmt.Errorf("CREATE_FILE: file size cannot be negative")
		}
		if payload.File.Status != models.FileStatusUploading {
			return fmt.Errorf("CREATE_FILE: initial file status must be UPLOADING")
		}

	case OperationUpdateFileStatus:
		var payload UpdateFileStatusPayload
		if err := decodePayload(c, &payload); err != nil {
			return err
		}

		if payload.FileID == "" {
			return fmt.Errorf("UPDATE_FILE_STATUS: file ID is required")
		}
		if !validFileStatus(payload.Status) {
			return fmt.Errorf("UPDATE_FILE_STATUS: invalid status %q", payload.Status)
		}
		if payload.UpdatedAt.IsZero() {
			return fmt.Errorf("UPDATE_FILE_STATUS: updated_at is required")
		}

	case OperationUpdateUploadDetails:
		var payload UpdateUploadDetailsPayload
		if err := decodePayload(c, &payload); err != nil {
			return err
		}

		if payload.FileID == "" || payload.Checksum == "" {
			return fmt.Errorf("UPDATE_UPLOAD_DETAILS: file ID and checksum are required")
		}
		if payload.ChunkCount < 0 {
			return fmt.Errorf("UPDATE_UPLOAD_DETAILS: chunk count cannot be negative")
		}
		if payload.UpdatedAt.IsZero() {
			return fmt.Errorf("UPDATE_UPLOAD_DETAILS: updated_at is required")
		}

	case OperationCreateChunk:
		var payload CreateChunkPayload
		if err := decodePayload(c, &payload); err != nil {
			return err
		}

		chunk := payload.Chunk

		if chunk.ID == "" || chunk.FileID == "" || chunk.StorageNodeID == "" {
			return fmt.Errorf("CREATE_CHUNK: chunk ID, file ID and primary storage node ID are required")
		}
		if chunk.Index < 0 || chunk.Size < 0 {
			return fmt.Errorf("CREATE_CHUNK: index and size cannot be negative")
		}
		if chunk.Hash == "" {
			return fmt.Errorf("CREATE_CHUNK: chunk hash is required")
		}
		if chunk.ReplicationFactor < 1 {
			return fmt.Errorf("CREATE_CHUNK: replication factor must be at least 1")
		}
		if chunk.Status != models.ChunkStatusPending &&
			chunk.Status != models.ChunkStatusStored &&
			chunk.Status != models.ChunkStatusFailed &&
			chunk.Status != models.ChunkStatusReplicating {
			return fmt.Errorf("CREATE_CHUNK: invalid status %q", chunk.Status)
		}
		if chunk.CreatedAt.IsZero() {
			return fmt.Errorf("CREATE_CHUNK: created_at is required")
		}

	case OperationUpdateChunkStatus:
		var payload UpdateChunkStatusPayload
		if err := decodePayload(c, &payload); err != nil {
			return err
		}

		if payload.ChunkID == "" {
			return fmt.Errorf("UPDATE_CHUNK_STATUS: chunk ID is required")
		}
		if !validChunkStatus(payload.Status) {
			return fmt.Errorf("UPDATE_CHUNK_STATUS: invalid status %q", payload.Status)
		}

	case OperationUpdateChunkReplica:
		var payload UpdateChunkReplicaPayload
		if err := decodePayload(c, &payload); err != nil {
			return err
		}

		if payload.ChunkID == "" || payload.NodeID == "" {
			return fmt.Errorf("UPDATE_CHUNK_REPLICA: chunk ID and node ID are required")
		}
		if !validReplicaStatus(payload.Status) {
			return fmt.Errorf("UPDATE_CHUNK_REPLICA: invalid status %q", payload.Status)
		}
		if payload.UpdatedAt.IsZero() {
			return fmt.Errorf("UPDATE_CHUNK_REPLICA: updated_at is required")
		}

	case OperationMutate:
		if len(c.Mutations) == 0 {
			return fmt.Errorf("MUTATE requires at least one mutation")
		}
		if len(c.Payload) != 0 {
			return fmt.Errorf("MUTATE cannot contain a payload")
		}

		for i, mutation := range c.Mutations {
			if mutation.Bucket == "" {
				return fmt.Errorf("mutation %d: bucket is required", i)
			}
			if len(mutation.Key) == 0 {
				return fmt.Errorf("mutation %d: key is required", i)
			}

			switch mutation.Type {
			case MutationPut:
				if mutation.Value == nil {
					return fmt.Errorf("mutation %d: PUT requires a value", i)
				}
			case MutationDelete:
				if len(mutation.Value) != 0 {
					return fmt.Errorf("mutation %d: DELETE cannot contain a value", i)
				}
			default:
				return fmt.Errorf("mutation %d: unsupported type %q", i, mutation.Type)
			}
		}

	default:
		return fmt.Errorf("unsupported command operation %q", c.Operation)
	}

	return nil
}

func validatePayloadOnly(c *Command, operation string) error {
	if len(c.Payload) == 0 {
		return fmt.Errorf("%s requires a payload", operation)
	}
	if len(c.Mutations) != 0 {
		return fmt.Errorf("%s cannot contain generic mutations", operation)
	}
	return nil
}

func decodePayload(c *Command, destination interface{}) error {
	if len(c.Payload) == 0 {
		return fmt.Errorf("%s requires a payload", c.Operation)
	}
	if len(c.Mutations) != 0 {
		return fmt.Errorf("%s cannot contain generic mutations", c.Operation)
	}
	if err := json.Unmarshal(c.Payload, destination); err != nil {
		return fmt.Errorf("%s: invalid payload: %w", c.Operation, err)
	}
	return nil
}

func validFileStatus(status models.FileStatus) bool {
	switch status {
	case models.FileStatusUploading, models.FileStatusAvailable, models.FileStatusFailed:
		return true
	default:
		return false
	}
}

func validChunkStatus(status models.ChunkStatus) bool {
	switch status {
	case models.ChunkStatusPending,
		models.ChunkStatusStored,
		models.ChunkStatusFailed,
		models.ChunkStatusReplicating:
		return true
	default:
		return false
	}
}

func validReplicaStatus(status models.ReplicaStatus) bool {
	switch status {
	case models.ReplicaStatusPending,
		models.ReplicaStatusStored,
		models.ReplicaStatusFailed:
		return true
	default:
		return false
	}
}

func EncodeCommand(command *Command) ([]byte, error) {
	if err := command.Validate(); err != nil {
		return nil, err
	}

	data, err := json.Marshal(command)
	if err != nil {
		return nil, fmt.Errorf("encode raft command: %w", err)
	}

	return data, nil
}

func DecodeCommand(data []byte) (*Command, error) {
	var command Command

	if err := json.Unmarshal(data, &command); err != nil {
		return nil, fmt.Errorf("decode raft command: %w", err)
	}

	if err := command.Validate(); err != nil {
		return nil, err
	}

	return &command, nil
}
