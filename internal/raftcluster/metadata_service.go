package raftcluster

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

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

func (s *MetadataService) Submit(mutations []Mutation) error {
	if s.raftNode.State() != raft.Leader {
		return fmt.Errorf(
			"this master is not the leader; current leader: %s",
			s.raftNode.Leader(),
		)
	}

	commandID, err := generateCommandID()
	if err != nil {
		return err
	}

	command := &Command{
		ID:        commandID,
		Mutations: mutations,
	}

	data, err := EncodeCommand(command)
	if err != nil {
		return err
	}

	future := s.raftNode.Apply(data, s.timeout)

	if err := future.Error(); err != nil {
		return fmt.Errorf("raft command failed: %w", err)
	}

	response, ok := future.Response().(ApplyResult)
	if !ok {
		return fmt.Errorf("unexpected raft FSM response")
	}

	if response.Error != "" {
		return fmt.Errorf("metadata command failed: %s", response.Error)
	}

	return nil
}

func generateCommandID() (string, error) {
	var id [16]byte

	if _, err := rand.Read(id[:]); err != nil {
		return "", fmt.Errorf("generate command ID: %w", err)
	}

	return hex.EncodeToString(id[:]), nil
}

func (s *MetadataService) CreateUser(payload []byte) error {
    if s.raftNode.State() != raft.Leader {
        return fmt.Errorf(
            "this master is not the leader; current leader: %s",
            s.raftNode.Leader(),
        )
    }

    commandID, err := generateCommandID()
    if err != nil {
        return err
    }

    command := &Command{
        ID:        commandID,
        Operation: OperationCreateUser,
        Payload:   payload,
    }

    data, err := EncodeCommand(command)
    if err != nil {
        return err
    }

    future := s.raftNode.Apply(data, s.timeout)

    if err := future.Error(); err != nil {
        return fmt.Errorf("raft user creation failed: %w", err)
    }

    response, ok := future.Response().(ApplyResult)
    if !ok {
        return fmt.Errorf("unexpected FSM response")
    }

    if response.Error != "" {
        return fmt.Errorf("%s", response.Error)
    }

    return nil
}