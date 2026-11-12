package controller

import (
	"bytes"
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

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/HKNain/distributed-storage-system/internal/middleware"
	"github.com/HKNain/distributed-storage-system/internal/models"
)

var (
	ErrDownloadNotFound  = errors.New("file not found")
	ErrFileNotAvailable  = errors.New("file is not available for download")
	ErrFileIntegrity     = errors.New("file integrity verification failed")
	ErrChunkUnavailable = errors.New("no valid chunk copy is available")
)

func (c *FileController) DownloadFile(
	ctx context.Context,
	userID string,
	fileID string,
) (*models.File, *os.File, error) {
	// Ensure the storage manager is initialized.
	if c.storageManager == nil {
		return nil, nil, errors.New("storage client manager is not initialized")
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
		_ = tempFile.Close()
		_ = os.Remove(tempFile.Name())
		return nil, nil, err
	}

	fileHasher := sha256.New()
	var totalSize int64

	// 6. Retrieve and verify every chunk from its assigned node.
	for i, chunk := range chunks {
		if err := ctx.Err(); err != nil {
			return cleanup(err)
		}

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

		// New chunks have StorageNodeID in their metadata.
		// Older chunks may need the legacy node fallback.
		// Retrieve a verified copy from the primary or one of its replicas.
		data, sourceNodeID, err := c.retrieveVerifiedChunk(ctx, chunk)
		if err != nil {
			return cleanup(fmt.Errorf(
				"retrieve chunk %s (index %d): %w",
				chunk.ID,
				i,
				err,
			))
		}

		// Log which node supplied the chunk.
		if sourceNodeID != chunk.StorageNodeID {
			fmt.Printf(
				"Chunk %s recovered from replica node %s (primary: %s)\n",
				chunk.ID,
				sourceNodeID,
				chunk.StorageNodeID,
			)
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

// retrieveVerifiedChunk tries the primary first, followed by replicas.
// It returns only data that passes both size and SHA-256 verification.

func (fc *FileController) retrieveVerifiedChunk(
	ctx context.Context,
	chunk models.Chunk,
) ([]byte, string, error) {
	type candidate struct {
		nodeID string
	}

	candidates := make([]candidate, 0, 1+len(chunk.Replicas))
	seen := make(map[string]bool)

	addCandidate := func(nodeID string) {
		if nodeID == "" || seen[nodeID] {
			return
		}

		seen[nodeID] = true
		candidates = append(candidates, candidate{nodeID: nodeID})
	}

	// Inspect the primary and every assigned replica, including replicas
	// whose metadata currently says PENDING or FAILED.
	addCandidate(chunk.StorageNodeID)

	for _, replica := range chunk.Replicas {
		addCandidate(replica.NodeID)
	}

	var (
		healthyData   []byte
		healthySource string
		unhealthy     = make(map[string]string)
		sawIntegrityFailure bool
	)

	for _, candidate := range candidates {
		nodeID := candidate.nodeID

		client, err := fc.storageManager.GetClientForNode(nodeID)
		if err != nil {
			// The node may simply be unavailable. Do not assume corruption.
			unhealthy[nodeID] = "UNAVAILABLE"
			continue
		}

		data, err := client.GetChunk(ctx, chunk.ID)
		if err != nil {
			if status.Code(err) == codes.NotFound {
				unhealthy[nodeID] = "MISSING"
				sawIntegrityFailure = true
			} else {
				unhealthy[nodeID] = "UNAVAILABLE"
			}
			continue
		}

		hash := sha256.Sum256(data)
		checksum := hex.EncodeToString(hash[:])

		if int64(len(data)) != chunk.Size || checksum != chunk.Hash {
			unhealthy[nodeID] = "CORRUPTED"
			sawIntegrityFailure = true
			continue
		}

		// Retain the first verified copy, but continue inspecting
		// the remaining copies to detect additional unhealthy replicas.
		if healthyData == nil {
			healthyData = data
			healthySource = nodeID
		}
	}

	// Schedule repairs only after inspecting every assigned copy.
	// Scheduling errors must not prevent a successful download.
	for nodeID, reason := range unhealthy {
		if fc.repairCoordinator == nil {
			fmt.Printf(
				"repair unavailable: coordinator not initialized (chunk=%s target=%s reason=%s)\n",
				chunk.ID,
				nodeID,
				reason,
			)
			continue
		}

		if err := fc.repairCoordinator.ScheduleRepair(chunk, nodeID, reason); err != nil {
			fmt.Printf(
				"failed to schedule repair (chunk=%s target=%s): %v\n",
				chunk.ID,
				nodeID,
				err,
			)
		}
	}

	if healthyData != nil {
		return healthyData, healthySource, nil
	}

	if sawIntegrityFailure {
		return nil, "", fmt.Errorf(
			"%w: no valid copy found for chunk %s",
			ErrFileIntegrity,
			chunk.ID,
		)
	}

	return nil, "", fmt.Errorf(
		"%w: no available copy found for chunk %s",
		ErrChunkUnavailable,
		chunk.ID,
	)
}


// DownloadHandler handles HTTP file downloads.
func (c *FileController) DownloadHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
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

	file, tempFile, err := c.DownloadFile(
		r.Context(),
		userID,
		fileID,
	)
	if err != nil {
		switch {
		case errors.Is(err, ErrDownloadNotFound):
			http.Error(w, "File not found", http.StatusNotFound)

		case errors.Is(err, ErrFileNotAvailable):
			http.Error(w, "File is not available", http.StatusConflict)

		case errors.Is(err, ErrFileIntegrity):
			http.Error(
				w,
				"File integrity verification failed",
				http.StatusInternalServerError,
			)

		case errors.Is(err, ErrChunkUnavailable):
			http.Error(
				w,
				"No valid copy of one or more file chunks is currently available",
				http.StatusServiceUnavailable,
			)

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