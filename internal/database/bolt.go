package database

import (
	"os"
	"path/filepath"

	bolt "go.etcd.io/bbolt"
)

var UsersBucket = []byte("users")

func OpenDB(dbPath string) (*bolt.DB, error) {

	dir := filepath.Dir(dbPath)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(UsersBucket)
		return err
	})

	if err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}
