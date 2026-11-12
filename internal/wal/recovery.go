
package wal

import (
	"fmt"

	"github.com/HKNain/distributed-storage-system/internal/models"
)

type UploadState string

const (
	UploadCompleted   UploadState = "COMPLETED"
	UploadFailed      UploadState = "FAILED"
	UploadInterrupted UploadState = "INTERRUPTED"
)

type ChunkRecoveryState string

const (
	ChunkUncertain ChunkRecoveryState = "UNCERTAIN"
	ChunkCommitted ChunkRecoveryState = "COMMITTED"
)

type ChunkRecovery struct {
	ChunkID        string
	Index          int
	NodeID         string
	ReplicaNodeIDs []string
	Checksum       string
	Size           int64

	State   ChunkRecoveryState
	Started *models.Record
	Failed  *models.Record
	Commit  *models.Record
}

type UploadRecoveryPlan struct {
	FileID string
	State  UploadState

	Started   *models.Record
	Completed *models.Record
	Failed    *models.Record

	Chunks map[int]*ChunkRecovery
}

// BuildRecoveryPlans reconstructs upload states from WAL records.
// It does not modify the WAL, metadata database, or storage nodes.
func BuildRecoveryPlans(records []models.Record) (map[string]*UploadRecoveryPlan, error) {
	plans := make(map[string]*UploadRecoveryPlan)

	for _, record := range records {
		plan, exists := plans[record.FileID]

		if !exists {
			if record.Event != models.EventUploadStarted {
				return nil, fmt.Errorf(
					"file %s: received event %s before UPLOAD_STARTED",
					record.FileID,
					record.Event,
				)
			}

			plan = &UploadRecoveryPlan{
				FileID: record.FileID,
				State:  UploadInterrupted,
				Chunks: make(map[int]*ChunkRecovery),
			}

			plans[record.FileID] = plan
		}

		if plan.Completed != nil || plan.Failed != nil {
			return nil, fmt.Errorf(
				"file %s: event %s occurred after terminal event",
				record.FileID,
				record.Event,
			)
		}

		current := record

		switch record.Event {

		case models.EventUploadStarted:
			if plan.Started != nil {
				return nil, fmt.Errorf(
					"file %s: duplicate UPLOAD_STARTED event",
					record.FileID,
				)
			}

			plan.Started = &current

		case models.EventChunkWriteStarted:
			if plan.Started == nil {
				return nil, fmt.Errorf(
					"file %s: chunk started before upload",
					record.FileID,
				)
			}

			if _, exists := plan.Chunks[record.ChunkIndex]; exists {
				return nil, fmt.Errorf(
					"file %s: duplicate chunk start at index %d",
					record.FileID,
					record.ChunkIndex,
				)
			}

			plan.Chunks[record.ChunkIndex] = &ChunkRecovery{
				ChunkID:        record.ChunkID,
				Index:          record.ChunkIndex,
				NodeID:         record.NodeID,
				ReplicaNodeIDs: append([]string(nil), record.ReplicaNodeIDs...),
				Checksum:       record.Checksum,
				Size:           record.ChunkSize,
				State:          ChunkUncertain,
				Started:        &current,
			}

		case models.EventChunkWriteCommitted:
			chunk, err := getChunk(plan, record)
			if err != nil {
				return nil, err
			}

			if chunk.Commit != nil {
				return nil, fmt.Errorf(
					"file %s: duplicate commit for chunk %d",
					record.FileID,
					record.ChunkIndex,
				)
			}

			chunk.Commit = &current
			chunk.State = ChunkCommitted

		case models.EventChunkWriteFailed:
			chunk, err := getChunk(plan, record)
			if err != nil {
				return nil, err
			}

			chunk.Failed = &current

			// An RPC error does not prove that the storage node
			// failed to persist the chunk. Recovery must verify it.
			chunk.State = ChunkUncertain

		case models.EventUploadCompleted:
			if plan.Started == nil {
				return nil, fmt.Errorf(
					"file %s: completion without upload start",
					record.FileID,
				)
			}

			if err := validateCompletedUpload(plan, record.ChunkCount); err != nil {
				return nil, err
			}

			plan.Completed = &current
			plan.State = UploadCompleted

		case models.EventUploadFailed:
			if plan.Started == nil {
				return nil, fmt.Errorf(
					"file %s: failure without upload start",
					record.FileID,
				)
			}

			plan.Failed = &current
			plan.State = UploadFailed

		default:
			return nil, fmt.Errorf(
				"file %s: unsupported WAL event %s",
				record.FileID,
				record.Event,
			)
		}
	}

	return plans, nil
}

func getChunk(
	plan *UploadRecoveryPlan,
	record models.Record,
) (*ChunkRecovery, error) {
	chunk, exists := plan.Chunks[record.ChunkIndex]

	if !exists {
		return nil, fmt.Errorf(
			"file %s: chunk event without CHUNK_WRITE_STARTED at index %d",
			record.FileID,
			record.ChunkIndex,
		)
	}

	if chunk.ChunkID != record.ChunkID {
		return nil, fmt.Errorf(
			"file %s: chunk ID mismatch at index %d",
			record.FileID,
			record.ChunkIndex,
		)
	}

	return chunk, nil
}

func validateCompletedUpload(
	plan *UploadRecoveryPlan,
	expectedChunkCount int,
) error {
	if len(plan.Chunks) != expectedChunkCount {
		return fmt.Errorf(
			"file %s: completed upload expects %d chunks, WAL contains %d",
			plan.FileID,
			expectedChunkCount,
			len(plan.Chunks),
		)
	}

	for index := 0; index < expectedChunkCount; index++ {
		chunk, exists := plan.Chunks[index]

		if !exists || chunk.State != ChunkCommitted {
			return fmt.Errorf(
				"file %s: completion recorded but chunk %d is not committed",
				plan.FileID,
				index,
			)
		}
	}

	return nil
}

func BuildUploadRecoveryPlans(
	records []models.Record,
) (map[string]*UploadRecoveryPlan, error) {
	uploadRecords := make([]models.Record, 0, len(records))

	for _, record := range records {
		switch record.Event {
		case models.EventUploadStarted,
			models.EventChunkWriteStarted,
			models.EventChunkWriteCommitted,
			models.EventChunkWriteFailed,
			models.EventUploadCompleted,
			models.EventUploadFailed:

			uploadRecords = append(uploadRecords, record)
		}
	}

	return BuildRecoveryPlans(uploadRecords)
}