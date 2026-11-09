package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/HKNain/distributed-storage-system/internal/chunker"
	"github.com/HKNain/distributed-storage-system/internal/middleware"
	"github.com/HKNain/distributed-storage-system/internal/models"
	"github.com/HKNain/distributed-storage-system/internal/repository"
	"github.com/HKNain/distributed-storage-system/internal/rpc"
	"github.com/HKNain/distributed-storage-system/internal/wal"
)

const (
	MaxUploadSize int64 = 10 * 1024 * 1024 * 1024
	StagingDir          = "data/wal/staging"
)

type FileController struct {
	fileRepo       *repository.FileRepository
	chunkRepo      *repository.ChunkRepository
	storageManager *rpc.StorageClientManager
	walWriter      *wal.Writer
}

func NewFileController(
	fileRepo *repository.FileRepository,
	chunkRepo *repository.ChunkRepository,
	storageManager *rpc.StorageClientManager,
	walWriter *wal.Writer,
) *FileController {
	return &FileController{
		fileRepo:       fileRepo,
		chunkRepo:      chunkRepo,
		storageManager: storageManager,
		walWriter:      walWriter,
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

	if fileSize < 0 || fileSize > MaxUploadSize {
		return models.File{}, fmt.Errorf("invalid file size")
	}

	if c.walWriter == nil || c.storageManager  == nil {
		return models.File{}, fmt.Errorf("WAL writer or storage node ID is not configured")
	}

	file, err := c.fileRepo.CreateFile(models.File{
		UserID:      userID,
		Name:        fileName,
		ContentType: contentType,
		Size:        fileSize,
	})
	if err != nil {
		return models.File{}, fmt.Errorf("create file metadata: %w", err)
	}

	// Stage the complete source file before beginning WAL-tracked writes.
	if err := os.MkdirAll(StagingDir, 0755); err != nil {
		_ = c.fileRepo.UpdateFileStatus(file.ID, models.FileStatusFailed)
		return models.File{}, fmt.Errorf("create staging directory: %w", err)
	}

	stagingPath := filepath.Join(StagingDir, file.ID+".upload")

	stagedFile, err := os.OpenFile(
		stagingPath,
		os.O_CREATE|os.O_EXCL|os.O_WRONLY,
		0600,
	)
	if err != nil {
		_ = c.fileRepo.UpdateFileStatus(file.ID, models.FileStatusFailed)
		return models.File{}, fmt.Errorf("create staging file: %w", err)
	}

	hasher := sha256.New()

	written, copyErr := io.Copy(
		io.MultiWriter(stagedFile, hasher),
		io.LimitReader(reader, fileSize+1),
	)

	syncErr := stagedFile.Sync()
	closeErr := stagedFile.Close()

	if copyErr != nil || syncErr != nil || closeErr != nil {
		_ = c.fileRepo.UpdateFileStatus(file.ID, models.FileStatusFailed)
		return models.File{}, fmt.Errorf(
			"stage upload: copy=%v sync=%v close=%v",
			copyErr, syncErr, closeErr,
		)
	}

	if written != fileSize {
		_ = c.fileRepo.UpdateFileStatus(file.ID, models.FileStatusFailed)
		return models.File{}, fmt.Errorf(
			"file size mismatch: expected %d bytes, received %d",
			fileSize, written,
		)
	}

	checksum := hex.EncodeToString(hasher.Sum(nil))

	// Record the upload only after its staging file is durable.
	if err := c.walWriter.Append(models.Record{
		Event:       models.EventUploadStarted,
		FileID:      file.ID,
		StagingPath: stagingPath,
		FileSize:    fileSize,
		Checksum:    checksum,
	}); err != nil {
		_ = c.fileRepo.UpdateFileStatus(file.ID, models.FileStatusFailed)
		return models.File{}, fmt.Errorf("write upload-started WAL record: %w", err)
	}

	failUpload := func(cause error) (models.File, error) {
		walErr := c.walWriter.Append(models.Record{
			Event:  models.EventUploadFailed,
			FileID: file.ID,
			Error:  cause.Error(),
		})

		_ = c.fileRepo.UpdateFileStatus(file.ID, models.FileStatusFailed)

		if walErr != nil {
			return models.File{}, fmt.Errorf(
				"%w (also failed to record upload failure in WAL: %v)",
				cause, walErr,
			)
		}

		return models.File{}, cause
	}

	stagedReader, err := os.Open(stagingPath)
	if err != nil {
		return failUpload(fmt.Errorf("open staged upload: %w", err))
	}
	defer stagedReader.Close()

	chunkCount := 0

	
	err = chunker.Process(stagedReader, file.ID, func(chunk chunker.ChunkData) error {
		// 1. Select an ACTIVE storage node using round-robin.
		node, storageClient, err := c.storageManager.GetNextActiveNode()
		if err != nil {
			return fmt.Errorf("select storage node for chunk %d: %w", chunk.Metadata.Index, err)
		}

		// 2. Persist the selected node ID in chunk metadata.
		chunkMetadata := chunk.Metadata
		chunkMetadata.StorageNodeID = node.NodeID

		metadata, err := c.chunkRepo.CreateChunk(chunkMetadata)
		if err != nil {
			return fmt.Errorf("create chunk metadata: %w", err)
		}

		// 3. Record the write intent before sending bytes.
		startRecord := models.Record{
			Event:      models.EventChunkWriteStarted,
			FileID:     file.ID,
			ChunkID:    metadata.ID,
			ChunkIndex: metadata.Index,
			ChunkSize:  metadata.Size,
			Checksum:   metadata.Hash,
			NodeID:     node.NodeID,
		}

		if err := c.walWriter.Append(startRecord); err != nil {
			return fmt.Errorf("write chunk-started WAL record: %w", err)
		}

		// 4. Store the chunk on its assigned node.
		err = storageClient.StoreChunk(
			ctx,
			metadata.ID,
			chunk.Data,
			metadata.Hash,
		)

		if err != nil {
			_ = c.chunkRepo.UpdateChunkStatus(
				metadata.ID,
				models.ChunkStatusFailed,
			)

			walErr := c.walWriter.Append(models.Record{
				Event:      models.EventChunkWriteFailed,
				FileID:     file.ID,
				ChunkID:    metadata.ID,
				ChunkIndex: metadata.Index,
				ChunkSize:  metadata.Size,
				Checksum:   metadata.Hash,
				NodeID:     node.NodeID,
				Error:      err.Error(),
			})

			if walErr != nil {
				return fmt.Errorf(
					"store chunk %d: %w (also failed to record WAL failure: %v)",
					metadata.Index,
					err,
					walErr,
				)
			}

			return fmt.Errorf("store chunk %d: %w", metadata.Index, err)
		}

		// 5. Record physical storage success in WAL.
		if err := c.walWriter.Append(models.Record{
			Event:      models.EventChunkWriteCommitted,
			FileID:     file.ID,
			ChunkID:    metadata.ID,
			ChunkIndex: metadata.Index,
			ChunkSize:  metadata.Size,
			Checksum:   metadata.Hash,
			NodeID:     node.NodeID,
		}); err != nil {
			return fmt.Errorf("write chunk-committed WAL record: %w", err)
		}

		// 6. Update metadata only after the WAL commit.
		if err := c.chunkRepo.UpdateChunkStatus(
			metadata.ID,
			models.ChunkStatusStored,
		); err != nil {
			return fmt.Errorf("update chunk status: %w", err)
		}

		chunkCount++
		return nil
	})


	if err != nil {
		return failUpload(fmt.Errorf("process file chunks: %w", err))
	}

	if err := c.fileRepo.UpdateUploadDetails(
		file.ID,
		checksum,
		chunkCount,
	); err != nil {
		return failUpload(fmt.Errorf("update file metadata: %w", err))
	}

	// This event means every chunk has been committed and the full
	// file checksum has been calculated.
	if err := c.walWriter.Append(models.Record{
		Event:      models.EventUploadCompleted,
		FileID:     file.ID,
		ChunkCount: chunkCount,	
		Checksum:   checksum,
	}); err != nil {
		return failUpload(fmt.Errorf("write upload-completed WAL record: %w", err))
	}

	if err := c.fileRepo.UpdateFileStatus(
		file.ID,
		models.FileStatusAvailable,
	); err != nil {
		return models.File{}, fmt.Errorf("finalize file metadata: %w", err)
	}

	file.Checksum = checksum
	file.ChunkCount = chunkCount
	file.Status = models.FileStatusAvailable

	// Completed uploads no longer need their source staging file.
	if err := os.Remove(stagingPath); err != nil {
		// Do not fail an otherwise successful upload because cleanup failed.
		fmt.Printf("warning: could not remove staging file %s: %v\n", stagingPath, err)
	}

	return file, nil
}

func (c *FileController) UploadHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	r.Body = http.MaxBytesReader(w, r.Body, MaxUploadSize)

	if err := r.ParseMultipartForm(32 << 20); err != nil {
		http.Error(w, "Invalid or oversized upload", http.StatusBadRequest)
		return
	}

	if r.MultipartForm != nil {
		defer r.MultipartForm.RemoveAll()
	}

	uploaded, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "File is required", http.StatusBadRequest)
		return
	}
	defer uploaded.Close()

	userID, ok := r.Context().Value(middleware.UserIDKey).(string)
	if !ok || userID == "" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	fileName := r.FormValue("name")
	if fileName == "" {
		fileName = filepath.Base(header.Filename)
	}

	if fileName == "" || fileName == "." {
		http.Error(w, "Invalid filename", http.StatusBadRequest)
		return
	}

	contentType := header.Header.Get("Content-Type")

	uploadedFile, err := c.UploadFile(
		r.Context(),
		userID,
		fileName,
		contentType,
		header.Size,
		uploaded,
	)
	if err != nil {
		http.Error(w, "File upload failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	_ = json.NewEncoder(w).Encode(uploadedFile)
}
