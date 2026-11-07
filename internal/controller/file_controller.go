package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"encoding/json"
	"net/http"
	"path/filepath"

	"github.com/HKNain/distributed-storage-system/internal/chunker"
	"github.com/HKNain/distributed-storage-system/internal/models"
	"github.com/HKNain/distributed-storage-system/internal/repository"
	"github.com/HKNain/distributed-storage-system/internal/rpc"
	"github.com/HKNain/distributed-storage-system/internal/middleware"
)

type FileController struct {
	fileRepo     *repository.FileRepository
	chunkRepo    *repository.ChunkRepository
	storageClient *rpc.StorageClient
}

const MaxUploadSize int64 = 10 * 1024 * 1024 * 1024 // 10 GiB

func NewFileController(
	fileRepo *repository.FileRepository,
	chunkRepo *repository.ChunkRepository,
	storageClient *rpc.StorageClient,
) *FileController {
	return &FileController{
		fileRepo:      fileRepo,
		chunkRepo:     chunkRepo,
		storageClient: storageClient,
	}
}

func (c *FileController) UploadFile(
	ctx context.Context,
	userID string,
	fileName string,
	contentType string,
	fileSize int64,
	reader io.Reader,
) (models.File, error) {

	if userID == "" || fileName == "" || reader == nil {
		return models.File{}, fmt.Errorf("invalid upload parameters")
	}

	if fileSize < 0 {
		return models.File{}, fmt.Errorf("invalid file size")
	}

	// Create initial file metadata.
	file, err := c.fileRepo.CreateFile(models.File{
		UserID:      userID,
		Name:        fileName,
		ContentType: contentType,
		Size:        fileSize,
	})

	if err != nil {
		return models.File{}, fmt.Errorf("create file metadata: %w", err)
	}

	// Calculate the complete file checksum while streaming.
	hasher := sha256.New()
	reader = io.TeeReader(reader, hasher)

	chunkCount := 0

	// Process one chunk at a time.
	err = chunker.Process(reader, file.ID, func(chunk chunker.ChunkData) error {

		// Step 1: Create pending chunk metadata.
		metadata, err := c.chunkRepo.CreateChunk(chunk.Metadata)
		if err != nil {
			return fmt.Errorf("create chunk metadata: %w", err)
		}

		// Step 2: Send actual bytes to the storage node.
		err = c.storageClient.StoreChunk(
			ctx,
			metadata.ID,
			chunk.Data,
			chunk.Metadata.Hash,
		)

		if err != nil {
			_ = c.chunkRepo.UpdateChunkStatus(
				metadata.ID,
				models.ChunkStatusFailed,
			)

			return fmt.Errorf("store chunk %d: %w", chunk.Metadata.Index, err)
		}

		// Step 3: Mark chunk as stored.
		err = c.chunkRepo.UpdateChunkStatus(
			metadata.ID,
			models.ChunkStatusStored,
		)

		if err != nil {
			return fmt.Errorf("update chunk status: %w", err)
		}

		chunkCount++

		return nil
	})

	if err != nil {
		_ = c.fileRepo.UpdateFileStatus(
			file.ID,
			models.FileStatusFailed,
		)

		return models.File{}, fmt.Errorf("process file chunks: %w", err)
	}

	// Validate the received file size.
	// The HTTP layer should also enforce a maximum upload size.
	// This protects against unexpected reader behavior.
	checksum := hex.EncodeToString(hasher.Sum(nil))

	if err := c.fileRepo.UpdateUploadDetails(
		file.ID,
		checksum,
		chunkCount,
	); err != nil {
		_ = c.fileRepo.UpdateFileStatus(
			file.ID,
			models.FileStatusFailed,
		)

		return models.File{}, fmt.Errorf("update file metadata: %w", err)
	}

	// Mark the file available only after all chunks are stored.
	if err := c.fileRepo.UpdateFileStatus(
		file.ID,
		models.FileStatusAvailable,
	); err != nil {
		return models.File{}, fmt.Errorf("finalize file metadata: %w", err)
	}

	file.Checksum = checksum
	file.ChunkCount = chunkCount
	file.Status = models.FileStatusAvailable

	return file, nil
}

func (c *FileController) UploadHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	// Restrict the maximum request size.
	r.Body = http.MaxBytesReader(w, r.Body, MaxUploadSize)

	// Parse multipart form.
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		http.Error(w, "Invalid or oversized upload", http.StatusBadRequest)
		return
	}

	if r.MultipartForm != nil {
		defer r.MultipartForm.RemoveAll()
	}

	// Retrieve the uploaded file.
	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "File is required", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Retrieve authenticated user ID from middleware.
	userID, ok := r.Context().Value(middleware.UserIDKey).(string)
	if !ok || userID == "" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Use an optional display name.
	fileName := r.FormValue("name")
	if fileName == "" {
		fileName = filepath.Base(header.Filename)
	}

	if fileName == "" || fileName == "." {
		http.Error(w, "Invalid filename", http.StatusBadRequest)
		return
	}

	contentType := header.Header.Get("Content-Type")

	// Execute the upload pipeline.
	uploadedFile, err := c.UploadFile(
		r.Context(),
		userID,
		fileName,
		contentType,
		header.Size,
		file,
	)

	if err != nil {
		http.Error(
			w,
			"File upload failed",
			http.StatusInternalServerError,
		)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	if err := json.NewEncoder(w).Encode(uploadedFile); err != nil {
		return
	}
}
