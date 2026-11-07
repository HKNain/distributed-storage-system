package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var (
	ErrInvalidChunkID = errors.New("invalid chunk ID")
	ErrChunkNotFound  = errors.New("chunk not found")
)

type DiskStorage struct {
	BaseDir string
}

func NewDiskStorage(baseDir string) (*DiskStorage, error) {
	if baseDir == "" {
		return nil, errors.New("storage directory cannot be empty")
	}

	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("create storage directory: %w", err)
	}

	return &DiskStorage{
		BaseDir: baseDir,
	}, nil
}

func (s *DiskStorage) Store(ctx context.Context, chunkID string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if !isValidChunkID(chunkID) {
		return ErrInvalidChunkID
	}

	chunkPath := filepath.Join(s.BaseDir, chunkID+".chunk")
	tempPath := chunkPath + ".tmp"

	file, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return fmt.Errorf("create temporary chunk file: %w", err)
	}

	defer os.Remove(tempPath)

	n, err := file.Write(data)
	if err != nil {
		file.Close()
		return fmt.Errorf("write chunk: %w", err)
	}

	if n != len(data) {
		file.Close()
		return errors.New("incomplete chunk write")
	}

	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("sync chunk: %w", err)
	}

	if err := file.Close(); err != nil {
		return fmt.Errorf("close chunk: %w", err)
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := os.Rename(tempPath, chunkPath); err != nil {
		return fmt.Errorf("commit chunk file: %w", err)
	}

	return nil
}

func (s *DiskStorage) Retrieve(ctx context.Context, chunkID string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if !isValidChunkID(chunkID) {
		return nil, ErrInvalidChunkID
	}

	chunkPath := filepath.Join(s.BaseDir, chunkID+".chunk")

	data, err := os.ReadFile(chunkPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrChunkNotFound
		}

		return nil, fmt.Errorf("read chunk: %w", err)
	}

	return data, nil
}

func (s *DiskStorage) Delete(ctx context.Context, chunkID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if !isValidChunkID(chunkID) {
		return ErrInvalidChunkID
	}

	chunkPath := filepath.Join(s.BaseDir, chunkID+".chunk")

	if err := os.Remove(chunkPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ErrChunkNotFound
		}

		return fmt.Errorf("delete chunk: %w", err)
	}

	return nil
}

func isValidChunkID(id string) bool {
	if id == "" || strings.ContainsAny(id, `/\`) {
		return false
	}

	return filepath.Base(id) == id && id != "." && id != ".."
}
