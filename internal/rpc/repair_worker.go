package rpc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/HKNain/distributed-storage-system/internal/models"
	"github.com/HKNain/distributed-storage-system/internal/repository"
	"github.com/HKNain/distributed-storage-system/internal/utils"
	"github.com/HKNain/distributed-storage-system/internal/wal"
)

type RepairWorker struct {
	chunkRepo      *repository.ChunkRepository
	storageManager *StorageClientManager
	walWriter      *wal.Writer

	notify chan struct{}
}

func NewRepairWorker(
	chunkRepo *repository.ChunkRepository,
	storageManager *StorageClientManager,
	walWriter *wal.Writer,
) *RepairWorker {
	return &RepairWorker{
		chunkRepo:      chunkRepo,
		storageManager: storageManager,
		walWriter:      walWriter,
		notify:         make(chan struct{}, 1),
	}
}

// Notify wakes the worker after a new repair intent is appended.
// The WAL remains the durable source of truth.
func (w *RepairWorker) Notify() {
	select {
	case w.notify <- struct{}{}:
	default:
	}
}

func (w *RepairWorker) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 10 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Recover pending repairs immediately on startup.
	w.processPending(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.processPending(ctx)
		case <-w.notify:
			w.processPending(ctx)
		}
	}
}

func (w *RepairWorker) processPending(ctx context.Context) {
	records, err := w.walWriter.ReadAll()
	if err != nil {
		logger.LogError(fmt.Errorf("repair worker read WAL: %w", err))
		return
	}

	plans, err := wal.BuildRepairRecoveryPlans(records)
	if err != nil {
		logger.LogError(fmt.Errorf("build repair recovery plans: %w", err))
		return
	}

	for _, plan := range plans {
		if ctx.Err() != nil {
			return
		}

		if plan.State == wal.RepairCommitted {
			continue
		}

		if err := w.repairChunk(ctx, plan); err != nil {
			logger.LogRecovery(fmt.Sprintf("event=repair chunk=%q target=%q result=failed error=%q", plan.ChunkID, plan.TargetNodeID, err))
		}
	}
}

func (w *RepairWorker) repairChunk(
	ctx context.Context,
	plan *wal.RepairRecovery,
) error {
	chunk, err := w.chunkRepo.GetChunkByID(plan.ChunkID)
	if err != nil {
		return w.recordRepairFailure(plan, err)
	}

	// Do not execute stale repair operations against changed metadata.
	if chunk.FileID != plan.FileID ||
		chunk.Size != plan.Size ||
		chunk.Hash != plan.Checksum {
		return w.recordRepairFailure(
			plan,
			errors.New("chunk metadata no longer matches repair operation"),
		)
	}

	if !isRepairTargetAssigned(chunk, plan.TargetNodeID) {
		return w.recordRepairFailure(
			plan,
			errors.New("repair target is no longer assigned to this chunk"),
		)
	}

	// If the target is unavailable, leave the operation pending.
	targetClient, err := w.storageManager.GetClientForNode(plan.TargetNodeID)
	if err != nil {
		return w.recordRepairFailure(plan, err)
	}

	data, sourceNodeID, err := w.findVerifiedSource(
		ctx,
		chunk,
		plan.TargetNodeID,
	)
	if err != nil {
		return w.recordRepairFailure(plan, err)
	}

	if int64(len(data)) != plan.Size {
		return w.recordRepairFailure(
			plan,
			errors.New("verified source has unexpected chunk size"),
		)
	}

	if err := targetClient.StoreChunk(
		ctx,
		plan.ChunkID,
		data,
		plan.Checksum,
	); err != nil {
		return w.recordRepairFailure(plan, err)
	}

	// Read back the target. A successful StoreChunk call alone
	// does not prove that the target contains valid data.
	storedData, err := targetClient.GetChunk(ctx, plan.ChunkID)
	if err != nil {
		return w.recordRepairFailure(plan, err)
	}

	if !verifyChunkData(storedData, plan.Size, plan.Checksum) {
		return w.recordRepairFailure(
			plan,
			errors.New("repaired target failed read-back verification"),
		)
	}

	return w.walWriter.Append(models.Record{
		Event:        models.EventChunkRepairCommitted,
		FileID:       plan.FileID,
		ChunkID:      plan.ChunkID,
		NodeID:       plan.TargetNodeID,
		SourceNodeID: sourceNodeID,
		RepairID:     plan.RepairID,
		ChunkSize:    plan.Size,
		Checksum:     plan.Checksum,
	})
}

func (w *RepairWorker) findVerifiedSource(
	ctx context.Context,
	chunk models.Chunk,
	targetNodeID string,
) ([]byte, string, error) {
	candidates := make([]string, 0, 1+len(chunk.Replicas))

	if chunk.StorageNodeID != "" {
		candidates = append(candidates, chunk.StorageNodeID)
	}

	for _, replica := range chunk.Replicas {
		if replica.Status == models.ReplicaStatusStored {
			candidates = append(candidates, replica.NodeID)
		}
	}

	var lastErr error

	for _, nodeID := range candidates {
		if nodeID == "" || nodeID == targetNodeID {
			continue
		}

		client, err := w.storageManager.GetClientForNode(nodeID)
		if err != nil {
			lastErr = fmt.Errorf("source node %s unavailable: %w", nodeID, err)
			continue
		}

		data, err := client.GetChunk(ctx, chunk.ID)
		if err != nil {
			lastErr = fmt.Errorf("read from source node %s: %w", nodeID, err)
			continue
		}

		if !verifyChunkData(data, chunk.Size, chunk.Hash) {
			lastErr = fmt.Errorf("source node %s returned invalid chunk data", nodeID)
			continue
		}

		return data, nodeID, nil
	}

	if lastErr == nil {
		lastErr = errors.New("no eligible source nodes")
	}

	return nil, "", fmt.Errorf("no healthy source found: %w", lastErr)
}

func verifyChunkData(data []byte, expectedSize int64, expectedChecksum string) bool {
	if int64(len(data)) != expectedSize {
		return false
	}

	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:]) == expectedChecksum
}

func isRepairTargetAssigned(chunk models.Chunk, targetNodeID string) bool {
	if chunk.StorageNodeID == targetNodeID {
		return true
	}

	for _, replica := range chunk.Replicas {
		if replica.NodeID == targetNodeID {
			return true
		}
	}

	return false
}

func (w *RepairWorker) recordRepairFailure(
	plan *wal.RepairRecovery,
	cause error,
) error {
	appendErr := w.walWriter.Append(models.Record{
		Event:     models.EventChunkRepairFailed,
		FileID:    plan.FileID,
		ChunkID:   plan.ChunkID,
		NodeID:    plan.TargetNodeID,
		RepairID:  plan.RepairID,
		ChunkSize: plan.Size,
		Checksum:  plan.Checksum,
		Error:     cause.Error(),
	})

	if appendErr != nil {
		return fmt.Errorf("%v; additionally failed to append repair failure to WAL: %w",
			cause, appendErr)
	}

	return cause
}
