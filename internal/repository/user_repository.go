package repository

import (
	"encoding/json"
	"errors"
	"strings"
	"time"
	"crypto/rand"
	"fmt"
	
	"github.com/HKNain/distributed-storage-system/internal/database"
	"github.com/HKNain/distributed-storage-system/internal/models"
	"github.com/google/uuid"
	"github.com/HKNain/distributed-storage-system/internal/raftcluster"

	bolt "go.etcd.io/bbolt"
)

var (
	ErrUserExists  = errors.New("username already exists")
	ErrEmailExists = errors.New("email already registered")
	ErrIDExists    = errors.New("user ID already exists")
	ErrUserNotFound = errors.New("user not found")
)

type UserRepository struct {
	DB *bolt.DB
	metadata *raftcluster.MetadataService
}

func NewUserRepository(db *bolt.DB) *UserRepository {
	return &UserRepository{
		DB: db,
	}
}

func NewUserRepositoryWithMetadata(
    db *bolt.DB,
    metadata *raftcluster.MetadataService,
) *UserRepository {
    return &UserRepository{
        DB:       db,
        metadata: metadata,
    }
}

// CreateUser stores a new user.
func (r *UserRepository) CreateUser(user models.User) error {
	user.Username = strings.ToLower(strings.TrimSpace(user.Username))
	user.Email = strings.ToLower(strings.TrimSpace(user.Email))

	if user.Username == "" || user.Email == "" || user.Password == "" {
		return errors.New("username, email and password are required")
	}

	user.ID = newUserID()

	now := time.Now().UTC()
	user.CreatedAt = now
	user.UpdatedAt = now

	// Independent local bbolt mode.
	if r.metadata == nil {
		return r.DB.Update(func(tx *bolt.Tx) error {
			bucket, err := tx.CreateBucketIfNotExists(database.UsersBucket)
			if err != nil {
				return err
			}

			if bucket.Get([]byte(user.Username)) != nil {
				return ErrUserExists
			}

			err = bucket.ForEach(func(_, value []byte) error {
				var existing models.User

				if err := json.Unmarshal(value, &existing); err != nil {
					return err
				}

				if strings.EqualFold(existing.Email, user.Email) {
					return ErrEmailExists
				}

				if existing.ID == user.ID {
					return ErrIDExists
				}

				return nil
			})
			if err != nil {
				return err
			}

			data, err := json.Marshal(user)
			if err != nil {
				return err
			}

			return bucket.Put([]byte(user.Username), data)
		})
	}

	// Raft-backed mode.
	payload, err := json.Marshal(user)
	if err != nil {
		return fmt.Errorf("marshal user: %w", err)
	}

	err = r.metadata.CreateUser(payload)
	if err != nil {
		switch err.Error() {
		case "USERNAME_EXISTS":
			return ErrUserExists
		case "EMAIL_EXISTS":
			return ErrEmailExists
		default:
			return fmt.Errorf("raft user creation failed: %w", err)
		}
	}

	return nil
}

// GetUserByUsername retrieves a user using username.
func (repo *UserRepository) GetUserByUsername(username string) (models.User, error) {

	var user models.User

	err := repo.DB.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(database.UsersBucket)
		if bucket == nil {
			return errors.New("users bucket not found")
		}

		data := bucket.Get([]byte(strings.ToLower(username)))

		if data == nil {
			return ErrUserNotFound
		}

		return json.Unmarshal(data, &user)
	})

	return user, err
}

// GetUserByEmail retrieves a user using email.
func (repo *UserRepository) GetUserByEmail(email string) (models.User, error) {

	var user models.User

	err := repo.DB.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(database.UsersBucket)
		if bucket == nil {
			return errors.New("users bucket not found")
		}

		return bucket.ForEach(func(k, v []byte) error {

			var existingUser models.User

			if err := json.Unmarshal(v, &existingUser); err != nil {
				return err
			}

			if strings.EqualFold(existingUser.Email, email) {
				user = existingUser
				return nil
			}

			return nil
		})
	})

	if err != nil {
		return user, err
	}

	if user.Username == "" {
		return user, ErrUserNotFound
	}

	return user, nil
}

func generateUniqueUserID(bucket *bolt.Bucket) (string, error) {
    for {
        id := uuid.NewString()

        var exists bool

        err := bucket.ForEach(func(_, v []byte) error {
            var existingUser models.User

            if err := json.Unmarshal(v, &existingUser); err != nil {
                return err
            }

            if existingUser.ID == id {
                exists = true
            }

            return nil
        })

        if err != nil {
            return "", err
        }

        if !exists {
            return id, nil
        }
    }
}

func newUserID() string {
    id := make([]byte, 16)

    if _, err := rand.Read(id); err != nil {
        panic(fmt.Sprintf("failed to generate user ID: %v", err))
    }

    // UUID v4 version and variant bits.
    id[6] = (id[6] & 0x0f) | 0x40
    id[8] = (id[8] & 0x3f) | 0x80

    return fmt.Sprintf(
        "%x-%x-%x-%x-%x",
        id[0:4], id[4:6], id[6:8], id[8:10], id[10:16],
    )
}