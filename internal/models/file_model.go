package models

import "time"

type FileStatus string

const (
	FileStatusUploading FileStatus = "UPLOADING"
	FileStatusAvailable FileStatus = "AVAILABLE"
	FileStatusFailed    FileStatus = "FAILED"
)

type File struct {
	ID          string     `json:"id"`
	UserID      string     `json:"user_id"`
	Name        string     `json:"name"`
	ContentType string     `json:"content_type"`
	Size        int64      `json:"size"`
	Checksum    string     `json:"checksum"`
	ChunkCount  int        `json:"chunk_count"`
	Status      FileStatus `json:"status"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}
