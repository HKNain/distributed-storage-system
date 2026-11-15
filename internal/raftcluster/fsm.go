
package raftcluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/HKNain/distributed-storage-system/internal/database"
	"github.com/HKNain/distributed-storage-system/internal/models"
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
		if applied == nil {
			return errors.New("applied commands bucket does not exist")
		}

		if applied.Get([]byte(command.ID)) != nil {
			result.Duplicate = true
			return nil
		}

		switch command.Operation {
		case OperationCreateUser:
			err = applyCreateUser(tx, command.Payload)

		case OperationCreateFile:
			err = applyCreateFile(tx, command.Payload)

		case OperationUpdateFileStatus:
			err = applyUpdateFileStatus(tx, command.Payload)

		case OperationUpdateUploadDetails:
			err = applyUpdateUploadDetails(tx, command.Payload)

		case OperationCreateChunk:
			err = applyCreateChunk(tx, command.Payload)

		case OperationUpdateChunkStatus:
			err = applyUpdateChunkStatus(tx, command.Payload)

		case OperationUpdateChunkReplica:
			err = applyUpdateChunkReplica(tx, command.Payload)

		case OperationMutate:
			err = applyMutations(tx, command.Mutations)

		default:
			err = fmt.Errorf("unsupported command operation: %s", command.Operation)
		}

		if err != nil {
			return err
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

	if user.ID == "" || user.Username == "" ||
		user.Email == "" || user.Password == "" {
		return errors.New("invalid user: required fields are missing")
	}

	usersBucket := tx.Bucket(database.UsersBucket)
	if usersBucket == nil {
		return errors.New("users bucket does not exist")
	}

	if usersBucket.Get([]byte(user.Username)) != nil {
		return errors.New("USERNAME_EXISTS")
	}

	err := usersBucket.ForEach(func(_, value []byte) error {
		var existing UserCommandPayload

		if err := json.Unmarshal(value, &existing); err != nil {
			return err
		}

		if strings.EqualFold(existing.Email, user.Email) {
			return errors.New("EMAIL_EXISTS")
		}

		if existing.ID == user.ID {
			return errors.New("USER_ID_EXISTS")
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

func applyMutations(tx *bolt.Tx, mutations []Mutation) error {
	for _, mutation := range mutations {
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

		default:
			return fmt.Errorf("unsupported mutation type: %s", mutation.Type)
		}
	}

	return nil
}

func applyCreateFile(tx *bolt.Tx, payload []byte) error {
	var command CreateFilePayload

	if err := json.Unmarshal(payload, &command); err != nil {
		return fmt.Errorf("decode create file payload: %w", err)
	}

	file := command.File

	files := tx.Bucket(database.FilesBucket)
	if files == nil {
		return errors.New("files bucket does not exist")
	}

	if file.ID == "" || file.UserID == "" || file.Name == "" {
		return errors.New("invalid file: required fields are missing")
	}

	if file.Size < 0 || file.Status != models.FileStatusUploading {
		return errors.New("invalid initial file metadata")
	}

	if file.CreatedAt.IsZero() || file.UpdatedAt.IsZero() {
		return errors.New("file timestamps are required")
	}

	if files.Get([]byte(file.ID)) != nil {
		return errors.New("FILE_ALREADY_EXISTS")
	}

	users := tx.Bucket(database.UsersBucket)
	if users == nil {
		return errors.New("users bucket does not exist")
	}

	userExists := false

	err := users.ForEach(func(_, value []byte) error {
		var user UserCommandPayload

		if err := json.Unmarshal(value, &user); err != nil {
			return err
		}

		if user.ID == file.UserID {
			userExists = true
		}

		return nil
	})
	if err != nil {
		return err
	}

	if !userExists {
		return errors.New("USER_NOT_FOUND")
	}

	data, err := json.Marshal(file)
	if err != nil {
		return err
	}

	return files.Put([]byte(file.ID), data)
}

func applyUpdateFileStatus(tx *bolt.Tx, payload []byte) error {
	var command UpdateFileStatusPayload

	if err := json.Unmarshal(payload, &command); err != nil {
		return fmt.Errorf("decode file status payload: %w", err)
	}

	files := tx.Bucket(database.FilesBucket)
	if files == nil {
		return errors.New("files bucket does not exist")
	}

	data := files.Get([]byte(command.FileID))
	if data == nil {
		return errors.New("FILE_NOT_FOUND")
	}

	var file models.File

	if err := json.Unmarshal(data, &file); err != nil {
		return err
	}

	file.Status = command.Status
	file.UpdatedAt = command.UpdatedAt

	updated, err := json.Marshal(file)
	if err != nil {
		return err
	}

	return files.Put([]byte(file.ID), updated)
}

func applyUpdateUploadDetails(tx *bolt.Tx, payload []byte) error {
	var command UpdateUploadDetailsPayload

	if err := json.Unmarshal(payload, &command); err != nil {
		return fmt.Errorf("decode upload details payload: %w", err)
	}

	files := tx.Bucket(database.FilesBucket)
	if files == nil {
		return errors.New("files bucket does not exist")
	}

	data := files.Get([]byte(command.FileID))
	if data == nil {
		return errors.New("FILE_NOT_FOUND")
	}

	var file models.File

	if err := json.Unmarshal(data, &file); err != nil {
		return err
	}

	if file.Status != models.FileStatusUploading {
		return errors.New("file is not in UPLOADING state")
	}

	file.Checksum = command.Checksum
	file.ChunkCount = command.ChunkCount
	file.UpdatedAt = command.UpdatedAt

	updated, err := json.Marshal(file)
	if err != nil {
		return err
	}

	return files.Put([]byte(file.ID), updated)
}

func applyCreateChunk(tx *bolt.Tx, payload []byte) error {
	var command CreateChunkPayload

	if err := json.Unmarshal(payload, &command); err != nil {
		return fmt.Errorf("decode create chunk payload: %w", err)
	}

	chunk := command.Chunk

	chunks := tx.Bucket(database.ChunksBucket)
	files := tx.Bucket(database.FilesBucket)

	if chunks == nil || files == nil {
		return errors.New("metadata buckets do not exist")
	}

	if chunk.ID == "" || chunk.FileID == "" ||
		chunk.StorageNodeID == "" {
		return errors.New("invalid chunk: required fields are missing")
	}

	if chunk.Index < 0 || chunk.Size < 0 {
		return errors.New("chunk index and size cannot be negative")
	}

	if chunk.Hash == "" || chunk.ReplicationFactor < 1 {
		return errors.New("invalid chunk hash or replication factor")
	}

	if chunk.CreatedAt.IsZero() {
		return errors.New("chunk created_at is required")
	}

	if chunks.Get([]byte(chunk.ID)) != nil {
		return errors.New("CHUNK_ALREADY_EXISTS")
	}

	fileData := files.Get([]byte(chunk.FileID))
	if fileData == nil {
		return errors.New("FILE_NOT_FOUND")
	}

	var file models.File

	if err := json.Unmarshal(fileData, &file); err != nil {
		return err
	}

	if file.Status != models.FileStatusUploading {
		return errors.New("parent file is not in UPLOADING state")
	}

	err := chunks.ForEach(func(_, value []byte) error {
		var existing models.Chunk

		if err := json.Unmarshal(value, &existing); err != nil {
			return err
		}

		if existing.FileID == chunk.FileID &&
			existing.Index == chunk.Index {
			return errors.New("CHUNK_INDEX_ALREADY_EXISTS")
		}

		return nil
	})
	if err != nil {
		return err
	}

	seenNodes := map[string]bool{
		chunk.StorageNodeID: true,
	}

	for _, replica := range chunk.Replicas {
		if replica.NodeID == "" {
			return errors.New("replica node ID is required")
		}

		if seenNodes[replica.NodeID] {
			return fmt.Errorf(
				"duplicate storage node in chunk placement: %s",
				replica.NodeID,
			)
		}

		seenNodes[replica.NodeID] = true
	}

	data, err := json.Marshal(chunk)
	if err != nil {
		return err
	}

	return chunks.Put([]byte(chunk.ID), data)
}

func applyUpdateChunkStatus(tx *bolt.Tx, payload []byte) error {
	var command UpdateChunkStatusPayload

	if err := json.Unmarshal(payload, &command); err != nil {
		return fmt.Errorf("decode chunk status payload: %w", err)
	}

	chunks := tx.Bucket(database.ChunksBucket)
	if chunks == nil {
		return errors.New("chunks bucket does not exist")
	}

	data := chunks.Get([]byte(command.ChunkID))
	if data == nil {
		return errors.New("CHUNK_NOT_FOUND")
	}

	var chunk models.Chunk

	if err := json.Unmarshal(data, &chunk); err != nil {
		return err
	}

	chunk.Status = command.Status

	updated, err := json.Marshal(chunk)
	if err != nil {
		return err
	}

	return chunks.Put([]byte(chunk.ID), updated)
}

func applyUpdateChunkReplica(tx *bolt.Tx, payload []byte) error {
	var command UpdateChunkReplicaPayload

	if err := json.Unmarshal(payload, &command); err != nil {
		return fmt.Errorf("decode chunk replica payload: %w", err)
	}

	chunks := tx.Bucket(database.ChunksBucket)
	if chunks == nil {
		return errors.New("chunks bucket does not exist")
	}

	data := chunks.Get([]byte(command.ChunkID))
	if data == nil {
		return errors.New("CHUNK_NOT_FOUND")
	}

	var chunk models.Chunk

	if err := json.Unmarshal(data, &chunk); err != nil {
		return err
	}

	found := false

	for i := range chunk.Replicas {
		if chunk.Replicas[i].NodeID == command.NodeID {
			chunk.Replicas[i].Status = command.Status
			chunk.Replicas[i].UpdatedAt = command.UpdatedAt
			found = true
			break
		}
	}

	if !found {
		return errors.New("REPLICA_NOT_FOUND")
	}

	updated, err := json.Marshal(chunk)
	if err != nil {
		return err
	}

	return chunks.Put([]byte(chunk.ID), updated)
}

type BucketSnapshot struct {
	Name    string          `json:"name"`
	Entries []SnapshotEntry `json:"entries"`
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
			bucketNames = append(
				bucketNames,
				append([]byte(nil), name...),
			)
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
