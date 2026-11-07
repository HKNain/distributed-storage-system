package database

import (
	"os"
	"path/filepath"

	bolt "go.etcd.io/bbolt"
)

var UsersBucket = []byte("users")
var FilesBucket = []byte("files")
var ChunksBucket = []byte("chunks")

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
		// Create the users bucket if it doesn't exist.
		_, err := tx.CreateBucketIfNotExists(UsersBucket)

		// Create the files bucket if it doesn't exist.
		_, err = tx.CreateBucketIfNotExists(FilesBucket)

		// Create the chunks bucket if it doesn't exist.
		_, err = tx.CreateBucketIfNotExists(ChunksBucket)
		
		return err
	})

	if err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}
