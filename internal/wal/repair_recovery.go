package wal

import (
	"fmt"

	"github.com/HKNain/distributed-storage-system/internal/models"
)

type RepairRecoveryState string

const (
	RepairPending   RepairRecoveryState = "PENDING"
	RepairCommitted RepairRecoveryState = "COMMITTED"
)

type RepairRecovery struct {
	RepairID     string
	FileID       string
	ChunkID      string
	TargetNodeID string
	SourceNodeID string
	RepairReason string
	Checksum     string
	Size         int64

	State RepairRecoveryState

	Started   *models.Record
	Committed *models.Record
	Failures  []models.Record
}

// BuildRepairRecoveryPlans reconstructs repair operations from WAL records.
//
// A failed attempt does not terminate an operation. Only a committed event
// marks the repair as complete.
func BuildRepairRecoveryPlans(records []models.Record) (map[string]*RepairRecovery, error) {
	plans := make(map[string]*RepairRecovery)

	for i := range records {
		record := records[i]

		switch record.Event {
		case models.EventChunkRepairStarted:
			if _, exists := plans[record.RepairID]; exists {
				return nil, fmt.Errorf(
					"duplicate repair started event: %s",
					record.RepairID,
				)
			}

			plans[record.RepairID] = &RepairRecovery{
				RepairID:     record.RepairID,
				FileID:       record.FileID,
				ChunkID:      record.ChunkID,
				TargetNodeID: record.NodeID,
				RepairReason: record.RepairReason,
				Checksum:     record.Checksum,
				Size:         record.ChunkSize,
				State:        RepairPending,
				Started:      &record,
			}

		case models.EventChunkRepairCommitted:
			plan, exists := plans[record.RepairID]
			if !exists {
				return nil, fmt.Errorf(
					"repair committed without started event: %s",
					record.RepairID,
				)
			}

			if err := validateMatchingRepair(plan, &record); err != nil {
				return nil, err
			}

			if plan.State == RepairCommitted {
				return nil, fmt.Errorf(
					"duplicate repair committed event: %s",
					record.RepairID,
				)
			}

			plan.State = RepairCommitted
			plan.SourceNodeID = record.SourceNodeID
			plan.Committed = &record

		case models.EventChunkRepairFailed:
			plan, exists := plans[record.RepairID]
			if !exists {
				return nil, fmt.Errorf(
					"repair failed without started event: %s",
					record.RepairID,
				)
			}

			if err := validateMatchingRepair(plan, &record); err != nil {
				return nil, err
			}

			if plan.State == RepairCommitted {
				return nil, fmt.Errorf(
					"repair failed after commit: %s",
					record.RepairID,
				)
			}

			plan.Failures = append(plan.Failures, record)
		}
	}

	return plans, nil
}

func validateMatchingRepair(
	plan *RepairRecovery,
	record *models.Record,
) error {
	if plan.FileID != record.FileID ||
		plan.ChunkID != record.ChunkID ||
		plan.TargetNodeID != record.NodeID ||
		plan.Checksum != record.Checksum ||
		plan.Size != record.ChunkSize {

		return fmt.Errorf(
			"repair record does not match started operation: %s",
			record.RepairID,
		)
	}

	return nil
}