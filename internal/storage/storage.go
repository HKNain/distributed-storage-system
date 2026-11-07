package storage

import "context"

type Storage interface {
    Store(ctx context.Context, chunkID string, data []byte) error
    Retrieve(ctx context.Context, chunkID string) ([]byte, error)
    Delete(ctx context.Context, chunkID string) error
}
