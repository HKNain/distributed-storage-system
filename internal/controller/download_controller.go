package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"bytes"

	"github.com/HKNain/distributed-storage-system/internal/models"
	"github.com/HKNain/distributed-storage-system/internal/middleware"
)

var (
	ErrDownloadNotFound  = errors.New("file not found")
	ErrFileNotAvailable  = errors.New("file is not available for download")
	ErrFileIntegrity     = errors.New("file integrity verification failed")
)

func (c *FileController) DownloadFile(
	ctx context.Context,
	userID string,
	fileID string,
) (*models.File, *os.File, error) {

	// Ensure storage client is initialized.
	if c.storageClient == nil {
		return nil, nil, errors.New("storage client is not initialized")
	}

	// 1. Fetch file metadata.
	file, err := c.fileRepo.GetFileByID(fileID)
	if err != nil {
		return nil, nil, ErrDownloadNotFound
	}

	// 2. Verify ownership.
	// Return 404 rather than revealing another user's file existence.
	if file.UserID != userID {
		return nil, nil, ErrDownloadNotFound
	}

	// 3. File must be completely uploaded.
	if file.Status != models.FileStatusAvailable {
		return nil, nil, ErrFileNotAvailable
	}

	// 4. Fetch chunks in index order.
	chunks, err := c.chunkRepo.GetChunksByFileID(fileID)
	if err != nil {
		return nil, nil, fmt.Errorf("fetch chunks: %w", err)
	}

	if len(chunks) != file.ChunkCount {
		return nil, nil, fmt.Errorf(
			"%w: expected %d chunks, found %d",
			ErrFileIntegrity,
			file.ChunkCount,
			len(chunks),
		)
	}

	// 5. Create temporary output file.
	tempFile, err := os.CreateTemp("", "distributed-storage-download-*.tmp")
	if err != nil {
		return nil, nil, fmt.Errorf("create temporary file: %w", err)
	}

	cleanup := func(err error) (*models.File, *os.File, error) {
		tempFile.Close()
		os.Remove(tempFile.Name())
		return nil, nil, err
	}

	fileHasher := sha256.New()
	var totalSize int64

	// 6. Retrieve and verify every chunk.
	for i, chunk := range chunks {

		if chunk.Index != i {
			return cleanup(fmt.Errorf(
				"%w: missing or unordered chunk at index %d",
				ErrFileIntegrity,
				i,
			))
		}

		if chunk.Status != models.ChunkStatusStored {
			return cleanup(fmt.Errorf(
				"%w: chunk %d is not stored",
				ErrFileIntegrity,
				i,
			))
		}

		data, err := c.storageClient.GetChunk(ctx, chunk.ID)
		if err != nil {
			return cleanup(fmt.Errorf(
				"retrieve chunk %s: %w",
				chunk.ID,
				err,
			))
		}

		// Verify chunk size.
		if int64(len(data)) != chunk.Size {
			return cleanup(fmt.Errorf(
				"%w: chunk %d size mismatch",
				ErrFileIntegrity,
				i,
			))
		}

		// Verify individual chunk checksum.
		chunkHash := sha256.Sum256(data)
		actualHash := hex.EncodeToString(chunkHash[:])

		if actualHash != chunk.Hash {
			return cleanup(fmt.Errorf(
				"%w: chunk %d checksum mismatch",
				ErrFileIntegrity,
				i,
			))
		}

		// Write verified chunk to temporary file.
		n, err := io.Copy(
			io.MultiWriter(tempFile, fileHasher),
			bytes.NewReader(data),
		)
		if err != nil {
			return cleanup(fmt.Errorf("write chunk: %w", err))
		}

		totalSize += n
	}

	// 7. Verify complete file size.
	if totalSize != file.Size {
		return cleanup(fmt.Errorf(
			"%w: expected size %d, got %d",
			ErrFileIntegrity,
			file.Size,
			totalSize,
		))
	}

	// 8. Verify complete file checksum.
	actualFileHash := hex.EncodeToString(fileHasher.Sum(nil))

	if actualFileHash != file.Checksum {
		return cleanup(fmt.Errorf(
			"%w: full file checksum mismatch",
			ErrFileIntegrity,
		))
	}

	// 9. Rewind temporary file before returning it.
	if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
		return cleanup(err)
	}

	return &file, tempFile, nil
}

// DownloadHandler handles HTTP file downloads.
func (c *FileController) DownloadHandler(w http.ResponseWriter, r *http.Request) {

	userID, ok := r.Context().Value(middleware.UserIDKey).(string)
	if !ok || userID == "" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	fileID := r.PathValue("id")
	if fileID == "" {
		http.Error(w, "File ID is required", http.StatusBadRequest)
		return
	}

	file, tempFile, err := c.DownloadFile(r.Context(), userID, fileID)
	if err != nil {
		switch {
		case errors.Is(err, ErrDownloadNotFound):
			http.Error(w, "File not found", http.StatusNotFound)

		case errors.Is(err, ErrFileNotAvailable):
			http.Error(w, "File is not available", http.StatusConflict)

		case errors.Is(err, ErrFileIntegrity):
			http.Error(w, "File integrity verification failed", http.StatusInternalServerError)

		default:
			http.Error(w, "Could not download file", http.StatusInternalServerError)
		}
		return
	}

	defer tempFile.Close()
	defer os.Remove(tempFile.Name())

	contentType := file.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	w.Header().Set("Content-Type", contentType)

	disposition := mime.FormatMediaType("attachment", map[string]string{
		"filename": filepath.Base(file.Name),
	})

	w.Header().Set("Content-Disposition", disposition)

	http.ServeContent(
		w,
		r,
		file.Name,
		file.CreatedAt,
		tempFile,
	)
}