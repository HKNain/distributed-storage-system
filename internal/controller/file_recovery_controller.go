
package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/HKNain/distributed-storage-system/internal/chunker"
	"github.com/HKNain/distributed-storage-system/internal/models"
	"github.com/HKNain/distributed-storage-system/internal/repository"
	"github.com/HKNain/distributed-storage-system/internal/wal"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RecoverInterruptedUploads must run after storage nodes are available
// and before accepting new uploads.
func (c *FileController) RecoverInterruptedUploads(ctx context.Context) error {
	if c.walWriter == nil || c.storageClient == nil || c.storageNodeID == "" {
		return fmt.Errorf("recovery dependencies are not configured")
	}

	records, err := c.walWriter.ReadAll()
	if err != nil {
		return fmt.Errorf("read WAL: %w", err)
	}

	plans, err := wal.BuildRecoveryPlans(records)
	if err != nil {
		return fmt.Errorf("build recovery plans: %w", err)
	}

	fileIDs := make([]string, 0, len(plans))
	for fileID := range plans {
		fileIDs = append(fileIDs, fileID)
	}

	sort.Strings(fileIDs)

	for _, fileID := range fileIDs {
		plan := plans[fileID]

		if plan.State != wal.UploadInterrupted {
			continue
		}

		fmt.Printf("Recovering interrupted upload: file=%s\n", fileID)

		if err := c.recoverUpload(ctx, plan); err != nil {
			return fmt.Errorf("recover upload %s: %w", fileID, err)
		}
	}

	return nil
}

func (c *FileController) recoverUpload(
	ctx context.Context,
	plan *wal.UploadRecoveryPlan,
) error {
	if plan.Started == nil {
		return fmt.Errorf("missing UPLOAD_STARTED record")
	}

	stagingPath := plan.Started.StagingPath

	stagedFile, err := os.Open(stagingPath)
	if err != nil {
		return fmt.Errorf("open staging file %q: %w", stagingPath, err)
	}
	defer stagedFile.Close()

	info, err := stagedFile.Stat()
	if err != nil {
		return fmt.Errorf("stat staging file: %w", err)
	}

	if info.Size() != plan.Started.FileSize {
		return fmt.Errorf(
			"staging file size mismatch: expected %d, got %d",
			plan.Started.FileSize,
			info.Size(),
		)
	}

	hasher := sha256.New()
	reader := io.TeeReader(stagedFile, hasher)

	chunkCount := 0

	err = chunker.Process(reader, plan.FileID, func(chunk chunker.ChunkData) error {
		if err := ctx.Err(); err != nil {
			return err
		}

		index := chunk.Metadata.Index
		existing, recorded := plan.Chunks[index]

		var metadata models.Chunk

		if recorded {
			// A WAL record must match the staged file.
			if existing.ChunkID == "" ||
				existing.Size != chunk.Metadata.Size ||
				existing.Checksum != chunk.Metadata.Hash {
				return fmt.Errorf(
					"staged chunk %d does not match its WAL record",
					index,
				)
			}

			if existing.NodeID != c.storageNodeID {
				return fmt.Errorf(
					"chunk %d belongs to node %q, but this recovery client targets %q",
					index,
					existing.NodeID,
					c.storageNodeID,
				)
			}

			metadata, err = c.chunkRepo.GetChunkByID(existing.ChunkID)

			if errors.Is(err, repository.ErrChunkNotFound) {
				// WAL intent exists, but metadata was not persisted.
				// Recreate metadata using the original WAL chunk ID.
				metadata = chunk.Metadata
				metadata.ID = existing.ChunkID

				metadata, err = c.chunkRepo.CreateChunk(metadata)
			}

			if err != nil {
				return fmt.Errorf(
					"load or recreate metadata for chunk %d: %w",
					index,
					err,
				)
			}
		} else {
			// No WAL record: metadata may still exist in bbolt.
			metadata, err = c.chunkRepo.GetChunkByFileIDAndIndex(
				plan.FileID,
				index,
			)

			if err != nil && !errors.Is(err, repository.ErrChunkNotFound) {
				return fmt.Errorf(
					"find metadata for chunk %d: %w",
					index,
					err,
				)
			}

			if errors.Is(err, repository.ErrChunkNotFound) {
				// Neither WAL nor metadata exists.
				// Generate the ID first, then persist WAL intent BEFORE metadata.
				metadata = chunk.Metadata
				metadata.ID = uuid.NewString()

				if err := c.appendRecoveryChunkStart(plan, metadata); err != nil {
					return err
				}

				metadata, err = c.chunkRepo.CreateChunk(metadata)
				if err != nil {
					return fmt.Errorf(
						"create metadata for chunk %d: %w",
						index,
						err,
					)
				}
			} else {
				// Metadata exists even though its WAL records are missing.
				if metadata.FileID != plan.FileID ||
					metadata.Index != index ||
					metadata.Hash != chunk.Metadata.Hash ||
					metadata.Size != chunk.Metadata.Size {
					return fmt.Errorf(
						"existing metadata does not match staged chunk %d",
						index,
					)
				}

				// Reuse its existing ID and restore the missing WAL intent.
				if err := c.appendRecoveryChunkStart(plan, metadata); err != nil {
					return err
				}
			}

			existing = plan.Chunks[index]
		}

		// Validate metadata regardless of how it was recovered.
		if metadata.ID == "" ||
			metadata.FileID != plan.FileID ||
			metadata.Index != index ||
			metadata.Hash != chunk.Metadata.Hash ||
			metadata.Size != chunk.Metadata.Size {
			return fmt.Errorf("chunk metadata mismatch at index %d", index)
		}

		// Check actual physical data on the storage node.
		storedData, getErr := c.storageClient.GetChunk(ctx, metadata.ID)

		if getErr == nil {
			storedHash := sha256.Sum256(storedData)
			storedChecksum := hex.EncodeToString(storedHash[:])

			if int64(len(storedData)) != metadata.Size ||
				storedChecksum != metadata.Hash {
				return fmt.Errorf(
					"stored chunk %d exists but failed integrity verification",
					index,
				)
			}
		} else {
			// Only restore if the node explicitly reports NotFound.
			if status.Code(getErr) != codes.NotFound {
				return fmt.Errorf(
					"check chunk %d on storage node: %w",
					index,
					getErr,
				)
			}

			if err := c.storageClient.StoreChunk(
				ctx,
				metadata.ID,
				chunk.Data,
				metadata.Hash,
			); err != nil {
				return fmt.Errorf("restore chunk %d: %w", index, err)
			}
		}

		// Record commit if it was absent from the WAL.
		if existing.Commit == nil {
			commitRecord := models.Record{
				Event:      models.EventChunkWriteCommitted,
				FileID:     plan.FileID,
				ChunkID:    metadata.ID,
				ChunkIndex: metadata.Index,
				ChunkSize:  metadata.Size,
				Checksum:   metadata.Hash,
				NodeID:     c.storageNodeID,
			}

			if err := c.walWriter.Append(commitRecord); err != nil {
				return fmt.Errorf(
					"record commit for chunk %d: %w",
					index,
					err,
				)
			}

			existing.Commit = &commitRecord
			existing.State = wal.ChunkCommitted
		}

		if err := c.chunkRepo.UpdateChunkStatus(
			metadata.ID,
			models.ChunkStatusStored,
		); err != nil {
			return fmt.Errorf(
				"update status for chunk %d: %w",
				index,
				err,
			)
		}

		chunkCount++
		return nil
	})

	if err != nil {
		return fmt.Errorf("process staged chunks: %w", err)
	}

	fileChecksum := hex.EncodeToString(hasher.Sum(nil))

	// If the original full-file checksum was recorded in UPLOAD_STARTED,
	// verify it before declaring the upload complete.
	if plan.Started.Checksum != "" &&
		plan.Started.Checksum != fileChecksum {
		return fmt.Errorf(
			"full-file checksum mismatch: expected %s, got %s",
			plan.Started.Checksum,
			fileChecksum,
		)
	}

	// Release the staging file handle before attempting deletion.
	if err := stagedFile.Close(); err != nil {
		return fmt.Errorf("close staging file: %w", err)
	}

	if err := c.fileRepo.UpdateUploadDetails(
		plan.FileID,
		fileChecksum,
		chunkCount,
	); err != nil {
		return fmt.Errorf("update upload details: %w", err)
	}

	// The completion record is the durable commit point.
	if err := c.walWriter.Append(models.Record{
		Event:      models.EventUploadCompleted,
		FileID:     plan.FileID,
		ChunkCount: chunkCount,
		Checksum:   fileChecksum,
	}); err != nil {
		return fmt.Errorf("record upload completion: %w", err)
	}

	if err := c.fileRepo.UpdateFileStatus(
		plan.FileID,
		models.FileStatusAvailable,
	); err != nil {
		return fmt.Errorf("mark recovered file available: %w", err)
	}

	if err := os.Remove(stagingPath); err != nil && !os.IsNotExist(err) {
		fmt.Printf(
			"warning: could not remove recovered staging file %s: %v\n",
			stagingPath,
			err,
		)
	}

	fmt.Printf(
		"Recovered upload: file=%s chunks=%d checksum=%s\n",
		plan.FileID,
		chunkCount,
		fileChecksum,
	)

	return nil
}

// appendRecoveryChunkStart writes a durable WAL intent and adds it
// to the in-memory recovery plan.
func (c *FileController) appendRecoveryChunkStart(
	plan *wal.UploadRecoveryPlan,
	metadata models.Chunk,
) error {
	record := models.Record{
		Event:      models.EventChunkWriteStarted,
		FileID:     plan.FileID,
		ChunkID:    metadata.ID,
		ChunkIndex: metadata.Index,
		ChunkSize:  metadata.Size,
		Checksum:   metadata.Hash,
		NodeID:     c.storageNodeID,
	}

	if err := c.walWriter.Append(record); err != nil {
		return fmt.Errorf(
			"record start for chunk %d: %w",
			metadata.Index,
			err,
		)
	}

	plan.Chunks[metadata.Index] = &wal.ChunkRecovery{
		ChunkID:  metadata.ID,
		Index:    metadata.Index,
		NodeID:   c.storageNodeID,
		Checksum: metadata.Hash,
		Size:     metadata.Size,
		State:    wal.ChunkUncertain,
		Started:  &record,
	}

	return nil
}
