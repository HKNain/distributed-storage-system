
package repository

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/HKNain/distributed-storage-system/internal/database"
	"github.com/HKNain/distributed-storage-system/internal/models"
	"github.com/google/uuid"

	bolt "go.etcd.io/bbolt"
)

var ErrFileNotFound = errors.New("file not found")

type FileRepository struct {
	DB *bolt.DB
}

func NewFileRepository(db *bolt.DB) *FileRepository {
	return &FileRepository{DB: db}
}

// CreateFile stores new file metadata.
func (repo *FileRepository) CreateFile(file models.File) (models.File, error) {
	err := repo.DB.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(database.FilesBucket)
		if bucket == nil {
			return errors.New("files bucket not found")
		}

		// Generate a unique file ID.
		for {
			file.ID = uuid.NewString()

			if bucket.Get([]byte(file.ID)) == nil {
				break
			}
		}

		now := time.Now().UTC()

		file.CreatedAt = now
		file.UpdatedAt = now
		file.Status = models.FileStatusUploading

		data, err := json.Marshal(file)
		if err != nil {
			return err
		}

		return bucket.Put([]byte(file.ID), data)
	})

	return file, err
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

// UpdateFileStatus updates the status of a file.
func (repo *FileRepository) UpdateFileStatus(
	fileID string,
	status models.FileStatus,
) error {
	if !isValidFileStatus(status) {
		return errors.New("invalid file status")
	}

	return repo.DB.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(database.FilesBucket)
		if bucket == nil {
			return errors.New("files bucket not found")
		}

		data := bucket.Get([]byte(fileID))
		if data == nil {
			return ErrFileNotFound
		}

		var file models.File

		if err := json.Unmarshal(data, &file); err != nil {
			return err
		}

		file.Status = status
		file.UpdatedAt = time.Now().UTC()

		updatedData, err := json.Marshal(file)
		if err != nil {
			return err
		}

		return bucket.Put([]byte(fileID), updatedData)
	})
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

func (r *FileRepository) UpdateUploadDetails(
	fileID string,
	checksum string,
	chunkCount int,
) error {
	return r.DB.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(database.FilesBucket)

		data := bucket.Get([]byte(fileID))
		if data == nil {
			return ErrFileNotFound
		}

		var file models.File
		if err := json.Unmarshal(data, &file); err != nil {
			return err
		}

		file.Checksum = checksum
		file.ChunkCount = chunkCount
		file.UpdatedAt = time.Now().UTC()

		updated, err := json.Marshal(file)
		if err != nil {
			return err
		}

		return bucket.Put([]byte(fileID), updated)
	})
}

