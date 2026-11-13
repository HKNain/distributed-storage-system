package raftcluster

import (
	"encoding/json"
	"fmt"
)

type MutationType string

const (
	MutationPut    MutationType = "PUT"
	MutationDelete MutationType = "DELETE"
)

type Mutation struct {
	Type   MutationType `json:"type"`
	Bucket string       `json:"bucket"`
	Key    []byte       `json:"key"`
	Value  []byte       `json:"value,omitempty"`
}

type CommandOperation string

const (
	OperationMutate   CommandOperation = "MUTATE"
	OperationCreateUser CommandOperation = "CREATE_USER"
)

type Command struct {
	ID        string           `json:"id"`
	Operation CommandOperation `json:"operation"`
	Payload   []byte           `json:"payload,omitempty"`
	Mutations []Mutation       `json:"mutations,omitempty"`
}

func (c *Command) Validate() error {
	if c == nil {
		return fmt.Errorf("command is nil")
	}

	if c.ID == "" {
		return fmt.Errorf("command ID is required")
	}

	switch c.Operation {
	case OperationCreateUser:
		if len(c.Payload) == 0 {
			return fmt.Errorf("CREATE_USER requires a payload")
		}

		if len(c.Mutations) != 0 {
			return fmt.Errorf("CREATE_USER cannot contain generic mutations")
		}

	case OperationMutate:
		if len(c.Mutations) == 0 {
			return fmt.Errorf("MUTATE requires at least one mutation")
		}

		for i, mutation := range c.Mutations {
			if mutation.Bucket == "" {
				return fmt.Errorf("mutation %d: bucket is required", i)
			}

			if len(mutation.Key) == 0 {
				return fmt.Errorf("mutation %d: key is required", i)
			}

			switch mutation.Type {
			case MutationPut:
				if mutation.Value == nil {
					return fmt.Errorf("mutation %d: PUT requires a value", i)
				}
			case MutationDelete:
				if len(mutation.Value) != 0 {
					return fmt.Errorf("mutation %d: DELETE cannot contain a value", i)
				}
			default:
				return fmt.Errorf("mutation %d: unsupported type %q", i, mutation.Type)
			}
		}

	default:
		return fmt.Errorf("unsupported command operation %q", c.Operation)
	}

	return nil
}

func EncodeCommand(command *Command) ([]byte, error) {
	if err := command.Validate(); err != nil {
		return nil, err
	}

	data, err := json.Marshal(command)
	if err != nil {
		return nil, fmt.Errorf("encode raft command: %w", err)
	}

	return data, nil
}

func DecodeCommand(data []byte) (*Command, error) {
	var command Command

	if err := json.Unmarshal(data, &command); err != nil {
		return nil, fmt.Errorf("decode raft command: %w", err)
	}

	if err := command.Validate(); err != nil {
		return nil, err
	}

	return &command, nil
}
