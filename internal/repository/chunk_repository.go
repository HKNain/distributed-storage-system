
package repository

import (
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/HKNain/distributed-storage-system/internal/database"
	"github.com/HKNain/distributed-storage-system/internal/models"
	"github.com/google/uuid"

	bolt "go.etcd.io/bbolt"
)

var (
	ErrChunkNotFound  = errors.New("chunk not found")
	ErrChunkIndexUsed = errors.New("chunk index already exists for this file")
)

type ChunkRepository struct {
	DB *bolt.DB
}

func NewChunkRepository(db *bolt.DB) *ChunkRepository {
	return &ChunkRepository{DB: db}
}

// CreateChunk stores chunk metadata.
// If an ID is provided, it is preserved. Otherwise, a new UUID is generated.
func (repo *ChunkRepository) CreateChunk(chunk models.Chunk) (models.Chunk, error) {
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

	err := repo.DB.Update(func(tx *bolt.Tx) error {
		filesBucket := tx.Bucket(database.FilesBucket)
		chunksBucket := tx.Bucket(database.ChunksBucket)

		if filesBucket == nil || chunksBucket == nil {
			return errors.New("required database bucket not found")
		}

		// Verify parent file exists.
		fileData := filesBucket.Get([]byte(chunk.FileID))
		if fileData == nil {
			return ErrFileNotFound
		}

		var file models.File
		if err := json.Unmarshal(fileData, &file); err != nil {
			return err
		}

		if file.Status != models.FileStatusUploading {
			return errors.New("file is not accepting chunks")
		}

		// Ensure this file/index combination is unique.
		err := chunksBucket.ForEach(func(_, value []byte) error {
			var existing models.Chunk

			if err := json.Unmarshal(value, &existing); err != nil {
				return err
			}

			if existing.FileID == chunk.FileID &&
				existing.Index == chunk.Index {
				return ErrChunkIndexUsed
			}

			return nil
		})
		if err != nil {
			return err
		}

		// Preserve a caller-provided ID (needed for WAL recovery).
		if chunk.ID != "" {
			if chunksBucket.Get([]byte(chunk.ID)) != nil {
				return errors.New("chunk ID already exists")
			}
		} else {
			for {
				chunk.ID = uuid.NewString()

				if chunksBucket.Get([]byte(chunk.ID)) == nil {
					break
				}
			}
		}

		if chunk.CreatedAt.IsZero() {
			chunk.CreatedAt = time.Now().UTC()
		}

		data, err := json.Marshal(chunk)
		if err != nil {
			return err
		}

		return chunksBucket.Put([]byte(chunk.ID), data)
	})

	return chunk, err
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

	return repo.DB.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(database.ChunksBucket)
		if bucket == nil {
			return errors.New("chunks bucket not found")
		}

		data := bucket.Get([]byte(chunkID))
		if data == nil {
			return ErrChunkNotFound
		}

		var chunk models.Chunk

		if err := json.Unmarshal(data, &chunk); err != nil {
			return err
		}

		chunk.Status = status

		updatedData, err := json.Marshal(chunk)
		if err != nil {
			return err
		}

		return bucket.Put([]byte(chunkID), updatedData)
	})
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
