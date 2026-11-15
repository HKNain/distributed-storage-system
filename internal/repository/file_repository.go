
package repository

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/HKNain/distributed-storage-system/internal/database"
	"github.com/HKNain/distributed-storage-system/internal/models"
	"github.com/HKNain/distributed-storage-system/internal/raftcluster"
	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"
)

var ErrFileNotFound = errors.New("file not found")

type FileRepository struct {
	DB       *bolt.DB
	metadata *raftcluster.MetadataService
}

func NewFileRepository(
	db *bolt.DB,
	metadata *raftcluster.MetadataService,
) *FileRepository {
	return &FileRepository{
		DB:       db,
		metadata: metadata,
	}
}

// CreateFile submits new file metadata through Raft.
func (repo *FileRepository) CreateFile(file models.File) (models.File, error) {
	if repo.metadata == nil {
		return file, errors.New("metadata service is required")
	}

	file.ID = uuid.NewString()

	now := time.Now().UTC()
	file.CreatedAt = now
	file.UpdatedAt = now
	file.Status = models.FileStatusUploading

	if err := repo.metadata.CreateFile(file); err != nil {
		return file, fmt.Errorf("create file metadata: %w", err)
	}

	return file, nil
}

// GetFileByID retrieves file metadata using its ID.
func (repo *FileRepository) GetFileByID(fileID string) (models.File, error) {
	var file models.File

	err := repo.DB.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(database.FilesBucket)
		if bucket == nil {
			return errors.New("files bucket not found")
		}

		data := bucket.Get([]byte(fileID))
		if data == nil {
			return ErrFileNotFound
		}

		return json.Unmarshal(data, &file)
	})

	return file, err
}

// ListFilesByUser retrieves all files belonging to a user.
func (repo *FileRepository) ListFilesByUser(userID string) ([]models.File, error) {
	var files []models.File

	err := repo.DB.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(database.FilesBucket)
		if bucket == nil {
			return errors.New("files bucket not found")
		}

		return bucket.ForEach(func(_, value []byte) error {
			var file models.File

			if err := json.Unmarshal(value, &file); err != nil {
				return err
			}

			if file.UserID == userID {
				files = append(files, file)
			}

			return nil
		})
	})

	return files, err
}

// UpdateFileStatus submits a file status update through Raft.
func (repo *FileRepository) UpdateFileStatus(
	fileID string,
	status models.FileStatus,
) error {
	if !isValidFileStatus(status) {
		return errors.New("invalid file status")
	}

	if repo.metadata == nil {
		return errors.New("metadata service is required")
	}

	return repo.metadata.UpdateFileStatus(
		fileID,
		status,
		time.Now().UTC(),
	)
}

func isValidFileStatus(status models.FileStatus) bool {
	switch status {
	case models.FileStatusUploading,
		models.FileStatusAvailable,
		models.FileStatusFailed:
		return true
	default:
		return false
	}
}

// UpdateUploadDetails submits final upload details through Raft.
func (repo *FileRepository) UpdateUploadDetails(
	fileID string,
	checksum string,
	chunkCount int,
) error {
	if repo.metadata == nil {
		return errors.New("metadata service is required")
	}

	return repo.metadata.UpdateUploadDetails(
		fileID,
		checksum,
		chunkCount,
		time.Now().UTC(),
	)
}
