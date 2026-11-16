package raftcluster

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/HKNain/distributed-storage-system/internal/models"
	"github.com/HKNain/distributed-storage-system/internal/utils"
	"github.com/hashicorp/raft"
)

type MetadataService struct {
	raftNode *raft.Raft
	timeout  time.Duration
}

func NewMetadataService(raftNode *raft.Raft) (*MetadataService, error) {
	if raftNode == nil {
		return nil, fmt.Errorf("raft node is required")
	}

	return &MetadataService{
		raftNode: raftNode,
		timeout:  10 * time.Second,
	}, nil
}

// Submit submits a generic bbolt mutation through Raft.
func (s *MetadataService) Submit(mutations []Mutation) error {
	return s.applyCommand(&Command{
		Operation: OperationMutate,
		Mutations: mutations,
	})
}

// CreateUser submits a user creation command through Raft.
func (s *MetadataService) CreateUser(payload []byte) error {
	return s.applyCommand(&Command{
		Operation: OperationCreateUser,
		Payload:   payload,
	})
}

// CreateFile submits new file metadata through Raft.
func (s *MetadataService) CreateFile(file models.File) error {
	payload, err := json.Marshal(CreateFilePayload{
		File: file,
	})
	if err != nil {
		return fmt.Errorf("marshal create-file payload: %w", err)
	}

	return s.applyCommand(&Command{
		Operation: OperationCreateFile,
		Payload:   payload,
	})
}

// UpdateFileStatus submits a file status update through Raft.
func (s *MetadataService) UpdateFileStatus(
	fileID string,
	status models.FileStatus,
	updatedAt time.Time,
) error {
	payload, err := json.Marshal(UpdateFileStatusPayload{
		FileID:    fileID,
		Status:    status,
		UpdatedAt: updatedAt,
	})
	if err != nil {
		return fmt.Errorf("marshal file-status payload: %w", err)
	}

	return s.applyCommand(&Command{
		Operation: OperationUpdateFileStatus,
		Payload:   payload,
	})
}

// UpdateUploadDetails submits the final upload details through Raft.
func (s *MetadataService) UpdateUploadDetails(
	fileID string,
	checksum string,
	chunkCount int,
	updatedAt time.Time,
) error {
	payload, err := json.Marshal(UpdateUploadDetailsPayload{
		FileID:     fileID,
		Checksum:   checksum,
		ChunkCount: chunkCount,
		UpdatedAt:  updatedAt,
	})
	if err != nil {
		return fmt.Errorf("marshal upload-details payload: %w", err)
	}

	return s.applyCommand(&Command{
		Operation: OperationUpdateUploadDetails,
		Payload:   payload,
	})
}

// CreateChunk submits new chunk metadata through Raft.
func (s *MetadataService) CreateChunk(chunk models.Chunk) error {
	payload, err := json.Marshal(CreateChunkPayload{
		Chunk: chunk,
	})
	if err != nil {
		return fmt.Errorf("marshal create-chunk payload: %w", err)
	}

	return s.applyCommand(&Command{
		Operation: OperationCreateChunk,
		Payload:   payload,
	})
}

// UpdateChunkStatus submits a chunk status update through Raft.
func (s *MetadataService) UpdateChunkStatus(
	chunkID string,
	status models.ChunkStatus,
) error {
	payload, err := json.Marshal(UpdateChunkStatusPayload{
		ChunkID: chunkID,
		Status:  status,
	})
	if err != nil {
		return fmt.Errorf("marshal chunk-status payload: %w", err)
	}

	return s.applyCommand(&Command{
		Operation: OperationUpdateChunkStatus,
		Payload:   payload,
	})
}

// UpdateChunkReplica submits a replica status update through Raft.
func (s *MetadataService) UpdateChunkReplica(
	chunkID string,
	nodeID string,
	status models.ReplicaStatus,
	updatedAt time.Time,
) error {
	payload, err := json.Marshal(UpdateChunkReplicaPayload{
		ChunkID:   chunkID,
		NodeID:    nodeID,
		Status:    status,
		UpdatedAt: updatedAt,
	})
	if err != nil {
		return fmt.Errorf("marshal replica-update payload: %w", err)
	}

	return s.applyCommand(&Command{
		Operation: OperationUpdateChunkReplica,
		Payload:   payload,
	})
}

// applyCommand is the single path for submitting commands to the Raft FSM.
func (s *MetadataService) applyCommand(command *Command) error {
	if s.raftNode.State() != raft.Leader {
		logger.LogRaft(fmt.Sprintf("event=command operation=%q result=rejected reason=not_leader leader=%q", command.Operation, s.raftNode.Leader()))
		return fmt.Errorf(
			"this master is not the leader; current leader: %s",
			s.raftNode.Leader(),
		)
	}

	commandID, err := generateCommandID()
	if err != nil {
		return err
	}

	command.ID = commandID
	logger.LogRaft(fmt.Sprintf("event=command operation=%q id=%q result=submitted", command.Operation, command.ID))

	data, err := EncodeCommand(command)
	if err != nil {
		return fmt.Errorf("encode metadata command: %w", err)
	}

	future := s.raftNode.Apply(data, s.timeout)

	if err := future.Error(); err != nil {
		logger.LogError(fmt.Errorf("raft command %s failed: %w", command.ID, err))
		return fmt.Errorf("raft command failed: %w", err)
	}

	response, ok := future.Response().(ApplyResult)
	if !ok {
		return fmt.Errorf("unexpected Raft FSM response")
	}

	if response.Error != "" {
		logger.LogRaft(fmt.Sprintf("event=command operation=%q id=%q result=failed error=%q", command.Operation, command.ID, response.Error))
		return fmt.Errorf("metadata command failed: %s", response.Error)
	}

	logger.LogRaft(fmt.Sprintf("event=command operation=%q id=%q result=committed", command.Operation, command.ID))
	return nil
}

func generateCommandID() (string, error) {
	var id [16]byte

	if _, err := rand.Read(id[:]); err != nil {
		return "", fmt.Errorf("generate command ID: %w", err)
	}

	return hex.EncodeToString(id[:]), nil
}
