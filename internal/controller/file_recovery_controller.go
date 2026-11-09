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
	"github.com/HKNain/distributed-storage-system/internal/rpc"
	"github.com/HKNain/distributed-storage-system/internal/wal"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RecoverInterruptedUploads must run after storage nodes are available
// and before accepting new uploads.
func (c *FileController) RecoverInterruptedUploads(ctx context.Context) error {
	if c.walWriter == nil || c.storageManager == nil {
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
		if err := ctx.Err(); err != nil {
			return err
		}

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
		var nodeID string

		if recorded {
			// The WAL identifies the original chunk and, when available,
			// the storage node to which it was assigned.
			if existing == nil {
				return fmt.Errorf("WAL recovery entry for chunk %d is nil", index)
			}

			if existing.ChunkID == "" ||
				existing.Size != chunk.Metadata.Size ||
				existing.Checksum != chunk.Metadata.Hash {
				return fmt.Errorf(
					"staged chunk %d does not match its WAL record",
					index,
				)
			}

			metadata, err = c.chunkRepo.GetChunkByID(existing.ChunkID)

			if errors.Is(err, repository.ErrChunkNotFound) {
				// WAL intent exists, but metadata was not persisted.
				metadata = chunk.Metadata
				metadata.ID = existing.ChunkID

				nodeID, err = resolveChunkNodeID(
					existing.NodeID,
					"",
				)
				if err != nil {
					return fmt.Errorf("resolve node for chunk %d: %w", index, err)
				}

				metadata.StorageNodeID = nodeID
				metadata, err = c.chunkRepo.CreateChunk(metadata)
			} else if err == nil {
				nodeID, err = resolveChunkNodeID(
					existing.NodeID,
					metadata.StorageNodeID,
				)
				if err != nil {
					return fmt.Errorf("resolve node for chunk %d: %w", index, err)
				}

				if metadata.StorageNodeID != "" &&
					existing.NodeID != "" &&
					metadata.StorageNodeID != existing.NodeID {
					return fmt.Errorf(
						"node mismatch for chunk %d: WAL says %q, metadata says %q",
						index,
						existing.NodeID,
						metadata.StorageNodeID,
					)
				}
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
				return fmt.Errorf("find metadata for chunk %d: %w", index, err)
			}

			if errors.Is(err, repository.ErrChunkNotFound) {
				// Neither WAL nor metadata exists. Select a new active node.
				// Persist the assignment in the WAL before creating metadata.
				node, _, selectErr := c.storageManager.GetNextActiveNode()
				if selectErr != nil {
					return fmt.Errorf(
						"select storage node for recovered chunk %d: %w",
						index,
						selectErr,
					)
				}

				nodeID = node.NodeID
				metadata = chunk.Metadata
				metadata.ID = uuid.NewString()
				metadata.StorageNodeID = nodeID

				if err := c.appendRecoveryChunkStart(plan, metadata, nodeID); err != nil {
					return err
				}

				metadata, err = c.chunkRepo.CreateChunk(metadata)
				if err != nil {
					return fmt.Errorf("create metadata for chunk %d: %w", index, err)
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

				nodeID, err = resolveChunkNodeID("", metadata.StorageNodeID)
				if err != nil {
					return fmt.Errorf("resolve node for chunk %d: %w", index, err)
				}

				// Backfill the in-memory value. The WAL record durably
				// records this assignment for future recovery.
				metadata.StorageNodeID = nodeID

				if err := c.appendRecoveryChunkStart(plan, metadata, nodeID); err != nil {
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

		// A recorded node assignment is mandatory before accessing data.
		if nodeID == "" {
			nodeID, err = resolveChunkNodeID("", metadata.StorageNodeID)
			if err != nil {
				return fmt.Errorf("resolve node for chunk %d: %w", index, err)
			}
		}

		storageClient, err := c.storageManager.GetClientForNode(nodeID)
		if err != nil {
			return fmt.Errorf(
				"get storage client for chunk %d on node %q: %w",
				index,
				nodeID,
				err,
			)
		}

		// Check the physical chunk on its assigned node.
		storedData, getErr := storageClient.GetChunk(ctx, metadata.ID)

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
			// Restore only when the node explicitly reports NotFound.
			// Do not redirect the write to a different node.
			if status.Code(getErr) != codes.NotFound {
				return fmt.Errorf(
					"check chunk %d on storage node %q: %w",
					index,
					nodeID,
					getErr,
				)
			}

			if err := storageClient.StoreChunk(
				ctx,
				metadata.ID,
				chunk.Data,
				metadata.Hash,
			); err != nil {
				return fmt.Errorf(
					"restore chunk %d on node %q: %w",
					index,
					nodeID,
					err,
				)
			}
		}

		// Record commit if it was absent from the WAL.
		if existing == nil || existing.Commit == nil {
			commitRecord := models.Record{
				Event:      models.EventChunkWriteCommitted,
				FileID:     plan.FileID,
				ChunkID:    metadata.ID,
				ChunkIndex: metadata.Index,
				ChunkSize:  metadata.Size,
				Checksum:   metadata.Hash,
				NodeID:     nodeID,
			}

			if err := c.walWriter.Append(commitRecord); err != nil {
				return fmt.Errorf("record commit for chunk %d: %w", index, err)
			}

			if existing != nil {
				existing.Commit = &commitRecord
				existing.State = wal.ChunkCommitted
			}
		}

		if err := c.chunkRepo.UpdateChunkStatus(
			metadata.ID,
			models.ChunkStatusStored,
		); err != nil {
			return fmt.Errorf("update status for chunk %d: %w", index, err)
		}

		chunkCount++
		return nil
	})

	if err != nil {
		return fmt.Errorf("process staged chunks: %w", err)
	}

	fileChecksum := hex.EncodeToString(hasher.Sum(nil))

	// Verify the original full-file checksum when it was recorded.
	if plan.Started.Checksum != "" &&
		plan.Started.Checksum != fileChecksum {
		return fmt.Errorf(
			"full-file checksum mismatch: expected %s, got %s",
			plan.Started.Checksum,
			fileChecksum,
		)
	}

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
	nodeID string,
) error {
	if nodeID == "" {
		return fmt.Errorf("cannot record chunk %d without a storage node ID", metadata.Index)
	}

	record := models.Record{
		Event:      models.EventChunkWriteStarted,
		FileID:     plan.FileID,
		ChunkID:    metadata.ID,
		ChunkIndex: metadata.Index,
		ChunkSize:  metadata.Size,
		Checksum:   metadata.Hash,
		NodeID:     nodeID,
	}

	if err := c.walWriter.Append(record); err != nil {
		return fmt.Errorf("record start for chunk %d: %w", metadata.Index, err)
	}

	plan.Chunks[metadata.Index] = &wal.ChunkRecovery{
		ChunkID:  metadata.ID,
		Index:    metadata.Index,
		NodeID:   nodeID,
		Checksum: metadata.Hash,
		Size:     metadata.Size,
		State:    wal.ChunkUncertain,
		Started:  &record,
	}

	return nil
}

// resolveChunkNodeID preserves the original node assignment.
// It uses the legacy node only for records created before node IDs
// were persisted in chunk metadata and the WAL.
func resolveChunkNodeID(walNodeID, metadataNodeID string) (string, error) {
	if walNodeID != "" && metadataNodeID != "" && walNodeID != metadataNodeID {
		return "", fmt.Errorf(
			"WAL node %q does not match metadata node %q",
			walNodeID,
			metadataNodeID,
		)
	}

	if walNodeID != "" {
		return walNodeID, nil
	}

	if metadataNodeID != "" {
		return metadataNodeID, nil
	}

	legacyNodeID := legacyStorageNodeID()
	if legacyNodeID == "" {
		return "", fmt.Errorf(
			"node ID is missing; configure LEGACY_STORAGE_NODE_ID for legacy chunks",
		)
	}

	return legacyNodeID, nil
}

// legacyStorageNodeID is used only for older chunks that predate
// per-chunk storage-node assignments.
func legacyStorageNodeID() string {
	if nodeID := os.Getenv("LEGACY_STORAGE_NODE_ID"); nodeID != "" {
		return nodeID
	}

	return os.Getenv("STORAGE_NODE_ID")
}

// Keep this compile-time reference useful if the RPC manager's
// concrete client type is changed in the future.
var _ *rpc.StorageClient