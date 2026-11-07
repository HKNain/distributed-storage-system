
package repository

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/HKNain/distributed-storage-system/internal/database"
	"github.com/HKNain/distributed-storage-system/internal/models"

	bolt "go.etcd.io/bbolt"
)

var (
	ErrUserExists  = errors.New("username already exists")
	ErrEmailExists = errors.New("email already registered")
	ErrUserNotFound = errors.New("user not found")
)

type UserRepository struct {
	DB *bolt.DB
}

func NewUserRepository(db *bolt.DB) *UserRepository {
	return &UserRepository{
		DB: db,
	}
}

// CreateUser stores a new user.
func (repo *UserRepository) CreateUser(user models.User) error {

	return repo.DB.Update(func(tx *bolt.Tx) error {

		bucket := tx.Bucket(database.UsersBucket)
		if bucket == nil {
			return errors.New("users bucket not found")
		}

		username := strings.ToLower(user.Username)

		// Check username uniqueness.
		if bucket.Get([]byte(username)) != nil {
			return ErrUserExists
		}

		// Check email uniqueness.
		err := bucket.ForEach(func(k, v []byte) error {
			var existingUser models.User

			if err := json.Unmarshal(v, &existingUser); err != nil {
				return err
			}

			if strings.EqualFold(existingUser.Email, user.Email) {
				return ErrEmailExists
			}

			return nil
		})

		if err != nil {
			return err
		}

		now := time.Now().UTC()

		user.CreatedAt = now
		user.UpdatedAt = now

		data, err := json.Marshal(user)
		if err != nil {
			return err
		}

		return bucket.Put([]byte(username), data)
	})
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
