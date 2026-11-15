
package repository

import (
	"encoding/json"
	"errors"
	"sort"
	"time"
	"strings"
	"fmt"

	"github.com/HKNain/distributed-storage-system/internal/database"
	"github.com/HKNain/distributed-storage-system/internal/models"
	"github.com/HKNain/distributed-storage-system/internal/raftcluster"
	"github.com/google/uuid"

	bolt "go.etcd.io/bbolt"
)

var (
	ErrChunkNotFound  = errors.New("chunk not found")
	ErrChunkIndexUsed = errors.New("chunk index already exists for this file")
)

type ChunkRepository struct {
	DB       *bolt.DB
	metadata *raftcluster.MetadataService
}

func NewChunkRepository(
	db *bolt.DB,
	metadata *raftcluster.MetadataService,
) *ChunkRepository {
	return &ChunkRepository{
		DB:       db,
		metadata: metadata,
	}
}

// CreateChunk stores chunk metadata.
// If an ID is provided, it is preserved. Otherwise, a new UUID is generated.
func (repo *ChunkRepository) CreateChunk(
	chunk models.Chunk,
) (models.Chunk, error) {
	if chunk.FileID == "" {
		return chunk, errors.New("file ID is required")
	}

	if chunk.Index < 0 {
		return chunk, errors.New("chunk index cannot be negative")
	}

	if chunk.Size <= 0 {
		return chunk, errors.New("chunk size must be greater than zero")
	}

	if !isValidChunkStatus(chunk.Status) {
		return chunk, errors.New("invalid chunk status")
	}

	if repo.metadata == nil {
		return chunk, errors.New("metadata service is required")
	}

	// Generate these before submitting the command.
	// Every Raft FSM will receive the same values.
	if chunk.ID == "" {
		chunk.ID = uuid.NewString()
	}

	if chunk.CreatedAt.IsZero() {
		chunk.CreatedAt = time.Now().UTC()
	}

	if err := repo.metadata.CreateChunk(chunk); err != nil {
		// Preserve the existing sentinel error for callers
		// that check for duplicate chunk indexes.
		if strings.Contains(err.Error(), ErrChunkIndexUsed.Error()) {
			return chunk, ErrChunkIndexUsed
		}

		return chunk, fmt.Errorf("create chunk metadata: %w", err)
	}

	return chunk, nil
}

// GetChunkByID retrieves metadata using the chunk ID.
func (repo *ChunkRepository) GetChunkByID(chunkID string) (models.Chunk, error) {
	var chunk models.Chunk

	err := repo.DB.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(database.ChunksBucket)
		if bucket == nil {
			return errors.New("chunks bucket not found")
		}

		data := bucket.Get([]byte(chunkID))
		if data == nil {
			return ErrChunkNotFound
		}

		return json.Unmarshal(data, &chunk)
	})

	return chunk, err
}

// GetChunkByFileIDAndIndex retrieves metadata using a file ID and chunk index.
// This is especially important during recovery when WAL records are missing.
func (repo *ChunkRepository) GetChunkByFileIDAndIndex(
	fileID string,
	index int,
) (models.Chunk, error) {
	var result models.Chunk
	found := false

	err := repo.DB.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(database.ChunksBucket)
		if bucket == nil {
			return errors.New("chunks bucket not found")
		}

		return bucket.ForEach(func(_, value []byte) error {
			var chunk models.Chunk

			if err := json.Unmarshal(value, &chunk); err != nil {
				return err
			}

			if chunk.FileID == fileID && chunk.Index == index {
				if found {
					return errors.New("multiple chunks found for the same file and index")
				}

				result = chunk
				found = true
			}

			return nil
		})
	})

	if err != nil {
		return models.Chunk{}, err
	}

	if !found {
		return models.Chunk{}, ErrChunkNotFound
	}

	return result, nil
}

// GetChunksByFileID retrieves all chunks belonging to a file.
func (repo *ChunkRepository) GetChunksByFileID(fileID string) ([]models.Chunk, error) {
	var chunks []models.Chunk

	err := repo.DB.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(database.ChunksBucket)
		if bucket == nil {
			return errors.New("chunks bucket not found")
		}

		return bucket.ForEach(func(_, value []byte) error {
			var chunk models.Chunk

			if err := json.Unmarshal(value, &chunk); err != nil {
				return err
			}

			if chunk.FileID == fileID {
				chunks = append(chunks, chunk)
			}

			return nil
		})
	})

	if err != nil {
		return nil, err
	}

	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].Index < chunks[j].Index
	})

	return chunks, nil
}

// UpdateChunkStatus updates a chunk's status.
func (repo *ChunkRepository) UpdateChunkStatus(
	chunkID string,
	status models.ChunkStatus,
) error {
	if !isValidChunkStatus(status) {
		return errors.New("invalid chunk status")
	}

	if repo.metadata == nil {
		return errors.New("metadata service is required")
	}

	return repo.metadata.UpdateChunkStatus(chunkID, status)
}

func isValidChunkStatus(status models.ChunkStatus) bool {
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

// UpdateChunkReplica updates the status of a replica identified by its node ID.
func (repo *ChunkRepository) UpdateChunkReplica(
	chunkID string,
	replica models.ChunkReplica,
) error {
	if chunkID == "" || replica.NodeID == "" {
		return errors.New("chunk ID and replica node ID are required")
	}

	if replica.Status != models.ReplicaStatusPending &&
		replica.Status != models.ReplicaStatusStored &&
		replica.Status != models.ReplicaStatusFailed {
		return errors.New("invalid replica status")
	}

	if repo.metadata == nil {
		return errors.New("metadata service is required")
	}

	return repo.metadata.UpdateChunkReplica(
		chunkID,
		replica.NodeID,
		replica.Status,
		time.Now().UTC(),
	)
}

// GetChunksWithPendingReplicas returns stored chunks that still have
// at least one replica awaiting replication or retry.
func (repo *ChunkRepository) GetChunksWithPendingReplicas() ([]models.Chunk, error) {
	var chunks []models.Chunk

	err := repo.DB.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(database.ChunksBucket)
		if bucket == nil {
			return errors.New("chunks bucket not found")
		}

		return bucket.ForEach(func(_, value []byte) error {
			var chunk models.Chunk

			if err := json.Unmarshal(value, &chunk); err != nil {
				return err
			}

			if chunk.Status != models.ChunkStatusStored {
				return nil
			}

			for _, replica := range chunk.Replicas {
				if replica.Status == models.ReplicaStatusPending ||
					replica.Status == models.ReplicaStatusFailed {
					chunks = append(chunks, chunk)
					break
				}
			}

			return nil
		})
	})

	return chunks, err
}
