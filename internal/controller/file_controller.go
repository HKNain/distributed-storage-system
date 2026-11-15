package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

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
	fileRepo           *repository.FileRepository
	chunkRepo          *repository.ChunkRepository
	storageManager     *rpc.StorageClientManager
	replicationManager *rpc.ReplicationManager
	walWriter          *wal.Writer
	repairCoordinator  *rpc.RepairCoordinator
}

func NewFileController(
	fileRepo *repository.FileRepository,
	chunkRepo *repository.ChunkRepository,
	storageManager *rpc.StorageClientManager,
	replicationManager *rpc.ReplicationManager,
	walWriter *wal.Writer,
	repairCoordinator *rpc.RepairCoordinator,
) *FileController {
	return &FileController{
		fileRepo:           fileRepo,
		chunkRepo:          chunkRepo,
		storageManager:     storageManager,
		replicationManager: replicationManager,
		walWriter:          walWriter,
		repairCoordinator:  repairCoordinator,
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

	if c.walWriter == nil ||
		c.storageManager == nil ||
		c.replicationManager == nil {
		return models.File{}, fmt.Errorf("upload dependencies are not configured")
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
		// 1. Select the primary node.
		primary, primaryClient, err := c.storageManager.GetNextActiveNode()
		if err != nil {
			return fmt.Errorf("select primary node: %w", err)
		}

		// 2. Select two distinct replica nodes, excluding the primary.
		replicaNodes, err := c.replicationManager.SelectReplicaNodes(
			primary.NodeID,
			2,
		)
		if err != nil {
			return fmt.Errorf("select replica nodes: %w", err)
		}

		// 3. Prepare chunk metadata with all three assignments.
		chunkMetadata := chunk.Metadata
		chunkMetadata.StorageNodeID = primary.NodeID
		chunkMetadata.ReplicationFactor = 3
		chunkMetadata.Replicas = []models.ChunkReplica{
			{
				NodeID:    replicaNodes[0].NodeID,
				Status:    models.ReplicaStatusPending,
				UpdatedAt: time.Now().UTC(),
			},
			{
				NodeID:    replicaNodes[1].NodeID,
				Status:    models.ReplicaStatusPending,
				UpdatedAt: time.Now().UTC(),
			},
		}

		metadata, err := c.chunkRepo.CreateChunk(chunkMetadata)
		if err != nil {
			return fmt.Errorf("create chunk metadata: %w", err)
		}

		// 4. Persist the complete placement decision before writing data.
		startRecord := models.Record{
			Event:      models.EventChunkWriteStarted,
			FileID:     file.ID,
			ChunkID:    metadata.ID,
			ChunkIndex: metadata.Index,
			ChunkSize:  metadata.Size,
			Checksum:   metadata.Hash,
			NodeID:     primary.NodeID,
			ReplicaNodeIDs: []string{
				replicaNodes[0].NodeID,
				replicaNodes[1].NodeID,
			},
		}

		if err := c.walWriter.Append(startRecord); err != nil {
			return fmt.Errorf("write chunk-started WAL record: %w", err)
		}

		// Helper to record an unsuccessful quorum write.
		failChunk := func(cause error) error {
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
				NodeID:     primary.NodeID,
				Error:      cause.Error(),
			})

			if walErr != nil {
				return fmt.Errorf(
					"%w (also failed to record WAL failure: %v)",
					cause,
					walErr,
				)
			}

			return cause
		}

		// 5. Store on the primary, falling back to assigned replicas when
		// the primary is unavailable. This mirrors download failover and
		// keeps the chunk available while the primary is repaired.
		fallbackReplicaID := ""
		if err := primaryClient.StoreChunk(
			ctx,
			metadata.ID,
			chunk.Data,
			metadata.Hash,
		); err != nil {
			primaryErr := err

			for _, replica := range replicaNodes {
				replicaClient, clientErr := c.storageManager.GetClientForNode(
					replica.NodeID,
				)
				if clientErr != nil {
					log.Printf(
						"upload fallback unavailable: chunk=%s node=%s: %v",
						metadata.ID,
						replica.NodeID,
						clientErr,
					)
					continue
				}

				if storeErr := replicaClient.StoreChunk(
					ctx,
					metadata.ID,
					chunk.Data,
					metadata.Hash,
				); storeErr != nil {
					log.Printf(
						"upload fallback failed: chunk=%s node=%s: %v",
						metadata.ID,
						replica.NodeID,
						storeErr,
					)
					continue
				}

				fallbackReplicaID = replica.NodeID
				if updateErr := c.chunkRepo.UpdateChunkReplica(
					metadata.ID,
					models.ChunkReplica{
						NodeID: replica.NodeID,
						Status: models.ReplicaStatusStored,
					},
				); updateErr != nil {
					return fmt.Errorf(
						"update fallback replica metadata: %w",
						updateErr,
					)
				}

				break
			}

			if fallbackReplicaID == "" {
				return failChunk(fmt.Errorf(
					"store chunk on primary and replicas: primary=%w",
					primaryErr,
				))
			}

			if c.repairCoordinator != nil {
				if repairErr := c.repairCoordinator.ScheduleRepair(
					metadata,
					primary.NodeID,
					"PRIMARY_UNAVAILABLE",
				); repairErr != nil {
					log.Printf(
						"failed to schedule primary repair: chunk=%s node=%s: %v",
						metadata.ID,
						primary.NodeID,
						repairErr,
					)
				}
			}
		}

		// 6. Store synchronously on the first replica when possible.
		// The primary is already durable at this point. A replica failure
		// remains retryable through the replication worker and must not make
		// an otherwise successful upload fail.
		firstReplicaID := replicaNodes[0].NodeID

		if fallbackReplicaID == firstReplicaID {
			log.Printf(
				"replica write completed during primary fallback: chunk=%s node=%s",
				metadata.ID,
				firstReplicaID,
			)
		} else if err := c.replicationManager.ReplicateToNode(
			ctx,
			firstReplicaID,
			metadata.ID,
			chunk.Data,
			metadata.Hash,
		); err != nil {
			_ = c.chunkRepo.UpdateChunkReplica(
				metadata.ID,
				models.ChunkReplica{
					NodeID: firstReplicaID,
					Status: models.ReplicaStatusFailed,
				},
			)
			log.Printf(
				"replica write deferred: chunk=%s node=%s: %v",
				metadata.ID,
				firstReplicaID,
				err,
			)

		} else {
			// 7. Persist the successful replica write.
			if err := c.chunkRepo.UpdateChunkReplica(
				metadata.ID,
				models.ChunkReplica{
					NodeID: firstReplicaID,
					Status: models.ReplicaStatusStored,
				},
			); err != nil {
				return fmt.Errorf("update synchronous replica metadata: %w", err)
			}
		}

		// 8. Record primary-write success in the WAL. Any failed replica
		// remains pending/failed and is retried asynchronously.
		if err := c.walWriter.Append(models.Record{
			Event:      models.EventChunkWriteCommitted,
			FileID:     file.ID,
			ChunkID:    metadata.ID,
			ChunkIndex: metadata.Index,
			ChunkSize:  metadata.Size,
			Checksum:   metadata.Hash,
			NodeID:     primary.NodeID,
		}); err != nil {
			return fmt.Errorf("write chunk-committed WAL record: %w", err)
		}

		// 9. Mark the chunk stored after both synchronous writes succeed.
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
