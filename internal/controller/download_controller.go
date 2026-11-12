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
	"strings"

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
func (c *FileController) retrieveVerifiedChunk(
    ctx context.Context,
    chunk models.Chunk,
) ([]byte, string, error) {

    type candidate struct {
        nodeID string
        role   string
    }

    candidates := make([]candidate, 0, 1+len(chunk.Replicas))
    seen := make(map[string]struct{})

    // Primary node first.
    primaryNodeID := chunk.StorageNodeID
    if primaryNodeID == "" {
        primaryNodeID = legacyStorageNodeID()
    }

    if primaryNodeID != "" {
        candidates = append(candidates, candidate{
            nodeID: primaryNodeID,
            role:   "primary",
        })
        seen[primaryNodeID] = struct{}{}
    }

    // Only use replicas whose metadata says they are stored.
    for _, replica := range chunk.Replicas {
        if replica.Status != models.ReplicaStatusStored {
            continue
        }

        if replica.NodeID == "" {
            continue
        }

        if _, exists := seen[replica.NodeID]; exists {
            continue
        }

        candidates = append(candidates, candidate{
            nodeID: replica.NodeID,
            role:   "replica",
        })
        seen[replica.NodeID] = struct{}{}
    }

    if len(candidates) == 0 {
        return nil, "", fmt.Errorf(
            "%w: chunk %s has no assigned primary or stored replicas",
            ErrChunkUnavailable,
            chunk.ID,
        )
    }

    var failures []string
    integrityFailure := false

    for _, candidate := range candidates {
        if err := ctx.Err(); err != nil {
            return nil, "", err
        }

        storageClient, err := c.storageManager.GetClientForNode(candidate.nodeID)
        if err != nil {
            failures = append(failures, fmt.Sprintf(
                "%s node %s: client unavailable: %v",
                candidate.role,
                candidate.nodeID,
                err,
            ))
            continue
        }

        data, err := storageClient.GetChunk(ctx, chunk.ID)
        if err != nil {
            failures = append(failures, fmt.Sprintf(
                "%s node %s: retrieval failed: %v",
                candidate.role,
                candidate.nodeID,
                err,
            ))
            continue
        }

        // Verify chunk size.
        if int64(len(data)) != chunk.Size {
            integrityFailure = true
            failures = append(failures, fmt.Sprintf(
                "%s node %s: size mismatch (expected %d, got %d)",
                candidate.role,
                candidate.nodeID,
                chunk.Size,
                len(data),
            ))
            continue
        }

        // Verify SHA-256 checksum.
        hash := sha256.Sum256(data)
        actualHash := hex.EncodeToString(hash[:])

        if actualHash != chunk.Hash {
            integrityFailure = true
            failures = append(failures, fmt.Sprintf(
                "%s node %s: checksum mismatch",
                candidate.role,
                candidate.nodeID,
            ))
            continue
        }

        return data, candidate.nodeID, nil
    }

    failureDetails := strings.Join(failures, "; ")

    if integrityFailure {
        return nil, "", fmt.Errorf(
            "%w: no valid copy of chunk %s; attempts: %s",
            ErrFileIntegrity,
            chunk.ID,
            failureDetails,
        )
    }

    return nil, "", fmt.Errorf(
        "%w: chunk %s; attempts: %s",
        ErrChunkUnavailable,
        chunk.ID,
        failureDetails,
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