package raftcluster

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
	"errors"
	"github.com/HKNain/distributed-storage-system/internal/database"

	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"
)

var appliedCommandsBucket = []byte("raft_applied_commands")

type MetadataFSM struct {
	db *bolt.DB
}

func NewMetadataFSM(db *bolt.DB) (*MetadataFSM, error) {
	if db == nil {
		return nil, fmt.Errorf("metadata database is nil")
	}

	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(appliedCommandsBucket)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("initialize FSM: %w", err)
	}

	return &MetadataFSM{db: db}, nil
}

type ApplyResult struct {
	Error     string
	Duplicate bool
}

type UserCommandPayload struct {
    ID        string    `json:"id"`
    Username  string    `json:"username"`
    Email     string    `json:"email"`
    Password  string    `json:"password"`
    CreatedAt time.Time `json:"created_at"`
    UpdatedAt time.Time `json:"updated_at"`
}


func (f *MetadataFSM) Apply(log *raft.Log) interface{} {
	command, err := DecodeCommand(log.Data)
	if err != nil {
		return ApplyResult{Error: err.Error()}
	}

	result := ApplyResult{}

	err = f.db.Update(func(tx *bolt.Tx) error {
		applied := tx.Bucket(appliedCommandsBucket)

		if applied.Get([]byte(command.ID)) != nil {
			result.Duplicate = true
			return nil
		}

		switch command.Operation {
		case OperationCreateUser:
    		if err := applyCreateUser(tx, command.Payload); err != nil {
				return err
			}

		case OperationMutate:
			for _, mutation := range command.Mutations {
				bucket, err := tx.CreateBucketIfNotExists(
					[]byte(mutation.Bucket),
				)
				if err != nil {
					return err
				}

				switch mutation.Type {
				case MutationPut:
					if err := bucket.Put(mutation.Key, mutation.Value); err != nil {
						return err
					}

				case MutationDelete:
					if err := bucket.Delete(mutation.Key); err != nil {
						return err
					}
				}
			}

		default:
			return fmt.Errorf("unsupported command operation: %s", command.Operation)
		}

		return applied.Put([]byte(command.ID), []byte{1})
	})

	if err != nil {
		return ApplyResult{Error: err.Error()}
	}

	return result
}

func applyCreateUser(tx *bolt.Tx, payload []byte) error {
    var user UserCommandPayload

    if err := json.Unmarshal(payload, &user); err != nil {
        return fmt.Errorf("invalid user payload: %w", err)
    }

    user.ID = strings.TrimSpace(user.ID)
    user.Username = strings.ToLower(strings.TrimSpace(user.Username))
    user.Email = strings.ToLower(strings.TrimSpace(user.Email))

    if user.ID == "" || user.Username == "" || user.Email == "" || user.Password == "" {
        return errors.New("invalid user: required fields are missing")
    }

    usersBucket := tx.Bucket(database.UsersBucket)
    if usersBucket == nil {
        return errors.New("users bucket does not exist")
    }

    // Check username uniqueness.
    if usersBucket.Get([]byte(user.Username)) != nil {
        return errors.New("USERNAME_EXISTS")
    }

    // Check email uniqueness.
    err := usersBucket.ForEach(func(_, value []byte) error {
        var existing UserCommandPayload

        if err := json.Unmarshal(value, &existing); err != nil {
            return err
        }

        if strings.EqualFold(existing.Email, user.Email) {
            return errors.New("EMAIL_EXISTS")
        }

        return nil
    })
    if err != nil {
        return err
    }

    normalizedPayload, err := json.Marshal(user)
    if err != nil {
        return err
    }

    return usersBucket.Put([]byte(user.Username), normalizedPayload)
}

type BucketSnapshot struct {
	Name    string            `json:"name"`
	Entries []SnapshotEntry   `json:"entries"`
}

type SnapshotEntry struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

type MetadataSnapshot struct {
	Buckets []BucketSnapshot `json:"buckets"`
}

func (f *MetadataFSM) Snapshot() (raft.FSMSnapshot, error) {
	snapshot := MetadataSnapshot{}

	err := f.db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, bucket *bolt.Bucket) error {
			bucketSnapshot := BucketSnapshot{
				Name: string(name),
			}

			err := bucket.ForEach(func(key, value []byte) error {
				if value == nil {
					return fmt.Errorf(
						"nested buckets are not supported: %s",
						name,
					)
				}

				bucketSnapshot.Entries = append(
					bucketSnapshot.Entries,
					SnapshotEntry{
						Key:   append([]byte(nil), key...),
						Value: append([]byte(nil), value...),
					},
				)

				return nil
			})
			if err != nil {
				return err
			}

			snapshot.Buckets = append(
				snapshot.Buckets,
				bucketSnapshot,
			)

			return nil
		})
	})

	if err != nil {
		return nil, fmt.Errorf("create metadata snapshot: %w", err)
	}

	data, err := json.Marshal(snapshot)
	if err != nil {
		return nil, fmt.Errorf("encode metadata snapshot: %w", err)
	}

	return &fsmSnapshot{data: data}, nil
}

func (f *MetadataFSM) Restore(reader io.ReadCloser) error {
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("read metadata snapshot: %w", err)
	}

	var snapshot MetadataSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("decode metadata snapshot: %w", err)
	}

	err = f.db.Update(func(tx *bolt.Tx) error {
		var bucketNames [][]byte

		if err := tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			bucketNames = append(bucketNames, append([]byte(nil), name...))
			return nil
		}); err != nil {
			return err
		}

		for _, name := range bucketNames {
			if err := tx.DeleteBucket(name); err != nil {
				return err
			}
		}

		for _, bucketSnapshot := range snapshot.Buckets {
			bucket, err := tx.CreateBucketIfNotExists(
				[]byte(bucketSnapshot.Name),
			)
			if err != nil {
				return err
			}

			for _, entry := range bucketSnapshot.Entries {
				if err := bucket.Put(entry.Key, entry.Value); err != nil {
					return err
				}
			}
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("restore metadata snapshot: %w", err)
	}

	return nil
}

type fsmSnapshot struct {
	data []byte
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}

	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
