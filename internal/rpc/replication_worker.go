package rpc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/HKNain/distributed-storage-system/internal/models"
	"github.com/HKNain/distributed-storage-system/internal/repository"
	"github.com/HKNain/distributed-storage-system/internal/utils"
)

type ReplicationWorker struct {
	chunkRepo      *repository.ChunkRepository
	storageManager *StorageClientManager
	interval       time.Duration
}

func NewReplicationWorker(
	chunkRepo *repository.ChunkRepository,
	storageManager *StorageClientManager,
	interval time.Duration,
) *ReplicationWorker {
	if interval <= 0 {
		interval = 10 * time.Second
	}

	return &ReplicationWorker{
		chunkRepo:      chunkRepo,
		storageManager: storageManager,
		interval:       interval,
	}
}

// Run continuously processes pending asynchronous replicas.
// Pending work is read from bbolt, so it survives master restarts.
func (w *ReplicationWorker) Run(ctx context.Context) {
	logger.LogReplication("event=worker state=started")

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	// Process existing pending work immediately on startup.
	w.processPendingReplicas(ctx)

	for {
		select {
		case <-ctx.Done():
			logger.LogReplication("event=worker state=stopped")
			return

		case <-ticker.C:
			w.processPendingReplicas(ctx)
		}
	}
}

func (w *ReplicationWorker) processPendingReplicas(ctx context.Context) {
	chunks, err := w.chunkRepo.GetChunksWithPendingReplicas()
	if err != nil {
		logger.LogError(fmt.Errorf("fetch pending replica chunks: %w", err))
		return
	}

	for _, chunk := range chunks {
		if ctx.Err() != nil {
			return
		}

		for _, replica := range chunk.Replicas {
			if replica.Status != models.ReplicaStatusPending &&
				replica.Status != models.ReplicaStatusFailed {
				continue
			}

			if ctx.Err() != nil {
				return
			}

			if err := w.replicateChunk(ctx, chunk, replica.NodeID); err != nil {
				logger.LogReplication(fmt.Sprintf("event=replica_sync chunk=%q primary=%q replica=%q result=failed error=%q", chunk.ID, chunk.StorageNodeID, replica.NodeID, err))

				// FAILED remains retryable because the repository query
				// includes both PENDING and FAILED replicas.
				if updateErr := w.chunkRepo.UpdateChunkReplica(
					chunk.ID,
					models.ChunkReplica{
						NodeID: replica.NodeID,
						Status: models.ReplicaStatusFailed,
					},
				); updateErr != nil {
					logger.LogError(fmt.Errorf("update failed replica status for chunk %s on node %s: %w", chunk.ID, replica.NodeID, updateErr))
				}

				continue
			}

			if err := w.chunkRepo.UpdateChunkReplica(
				chunk.ID,
				models.ChunkReplica{
					NodeID: replica.NodeID,
					Status: models.ReplicaStatusStored,
				},
			); err != nil {
				logger.LogError(fmt.Errorf("update stored replica metadata for chunk %s on node %s: %w", chunk.ID, replica.NodeID, err))
				continue
			}

			logger.LogReplication(fmt.Sprintf("event=replica_sync chunk=%q node=%q result=success", chunk.ID, replica.NodeID))
		}
	}
}

func (w *ReplicationWorker) replicateChunk(
	ctx context.Context,
	chunk models.Chunk,
	replicaNodeID string,
) error {
	if chunk.ID == "" || chunk.StorageNodeID == "" || replicaNodeID == "" {
		return fmt.Errorf("chunk or storage node ID is missing")
	}

	if replicaNodeID == chunk.StorageNodeID {
		return fmt.Errorf("replica node is the same as primary")
	}

	// Get the source from the original primary.
	primaryClient, err := w.storageManager.GetClientForNode(chunk.StorageNodeID)
	if err != nil {
		return fmt.Errorf("get primary client: %w", err)
	}

	// Use a worker-owned timeout, not the original HTTP request context.
	replicationCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	data, err := primaryClient.GetChunk(replicationCtx, chunk.ID)
	if err != nil {
		return fmt.Errorf("read primary chunk: %w", err)
	}

	// Verify the source before copying it.
	sum := sha256.Sum256(data)
	actualChecksum := hex.EncodeToString(sum[:])

	if actualChecksum != chunk.Hash {
		logger.LogReplication(fmt.Sprintf("event=replica_sync chunk=%q primary=%q result=checksum_mismatch", chunk.ID, chunk.StorageNodeID))
		return fmt.Errorf(
			"primary checksum mismatch: expected %s, got %s",
			chunk.Hash,
			actualChecksum,
		)
	}

	replicaClient, err := w.storageManager.GetClientForNode(replicaNodeID)
	if err != nil {
		return fmt.Errorf("get replica client: %w", err)
	}

	if err := replicaClient.StoreChunk(
		replicationCtx,
		chunk.ID,
		data,
		chunk.Hash,
	); err != nil {
		return fmt.Errorf("write replica chunk: %w", err)
	}

	logger.LogReplication(fmt.Sprintf("event=replica_sync chunk=%q primary=%q replica=%q bytes=%d result=success", chunk.ID, chunk.StorageNodeID, replicaNodeID, len(data)))
	return nil
}
