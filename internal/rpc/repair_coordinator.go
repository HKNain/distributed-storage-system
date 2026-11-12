package rpc

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/HKNain/distributed-storage-system/internal/models"
	"github.com/HKNain/distributed-storage-system/internal/wal"
)

type RepairCoordinator struct {
	mu        sync.Mutex
	walWriter *wal.Writer
	worker    *RepairWorker
}

func NewRepairCoordinator(
	walWriter *wal.Writer,
	worker *RepairWorker,
) *RepairCoordinator {
	return &RepairCoordinator{
		walWriter: walWriter,
		worker:    worker,
	}
}

// ScheduleRepair durably records a repair intent before notifying the worker.
// The same chunk-target pair cannot have multiple unfinished operations.
func (c *RepairCoordinator) ScheduleRepair(
	chunk models.Chunk,
	targetNodeID string,
	reason string,
) error {
	if c.walWriter == nil || c.worker == nil {
		return errors.New("repair coordinator is not initialized")
	}

	if strings.TrimSpace(targetNodeID) == "" {
		return errors.New("repair target node ID is required")
	}

	if strings.TrimSpace(reason) == "" {
		return errors.New("repair reason is required")
	}

	if !isRepairTargetAssigned(chunk, targetNodeID) {
		return fmt.Errorf("node %s is not assigned to chunk %s",
			targetNodeID, chunk.ID)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	records, err := c.walWriter.ReadAll()
	if err != nil {
		return fmt.Errorf("read WAL before scheduling repair: %w", err)
	}

	plans, err := wal.BuildRepairRecoveryPlans(records)
	if err != nil {
		return fmt.Errorf("build repair recovery plans: %w", err)
	}

	for _, plan := range plans {
		if plan.ChunkID == chunk.ID &&
			plan.TargetNodeID == targetNodeID &&
			plan.State == wal.RepairPending {
			// An unfinished operation already exists.
			c.worker.Notify()
			return nil
		}
	}

	repairID, err := generateRepairID()
	if err != nil {
		return fmt.Errorf("generate repair ID: %w", err)
	}

	record := models.Record{
		Event:       models.EventChunkRepairStarted,
		FileID:      chunk.FileID,
		ChunkID:     chunk.ID,
		ChunkIndex:  chunk.Index,
		NodeID:      targetNodeID,
		RepairID:    repairID,
		RepairReason: reason,
		ChunkSize:   chunk.Size,
		Checksum:    chunk.Hash,
	}

	if err := c.walWriter.Append(record); err != nil {
		return fmt.Errorf("append repair intent: %w", err)
	}

	// Append has already fsynced the intent.
	c.worker.Notify()

	return nil
}

func generateRepairID() (string, error) {
	var id [16]byte

	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}

	return hex.EncodeToString(id[:]), nil
}
