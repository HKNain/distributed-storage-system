package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/HKNain/distributed-storage-system/internal/utils"
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
		logger.LogError(errors.New("storage directory cannot be empty"))
		return nil, errors.New("storage directory cannot be empty")
	}

	if err := os.MkdirAll(baseDir, 0755); err != nil {
		logger.LogError(fmt.Errorf("create storage directory %q: %w", baseDir, err))
		return nil, fmt.Errorf("create storage directory: %w", err)
	}

	logger.LogStorage(fmt.Sprintf("operation=initialize directory=%q result=success", baseDir))
	return &DiskStorage{
		BaseDir: baseDir,
	}, nil
}

func (s *DiskStorage) Store(ctx context.Context, chunkID string, data []byte) error {
	if err := ctx.Err(); err != nil {
		logger.LogStorage(fmt.Sprintf("operation=write chunk=%q result=cancelled error=%q", chunkID, err))
		return err
	}

	if !isValidChunkID(chunkID) {
		logger.LogStorage(fmt.Sprintf("operation=write chunk=%q result=invalid", chunkID))
		return ErrInvalidChunkID
	}

	chunkPath := filepath.Join(s.BaseDir, chunkID+".chunk")
	tempPath := chunkPath + ".tmp"

	file, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		logger.LogError(fmt.Errorf("create temporary chunk file for %q: %w", chunkID, err))
		return fmt.Errorf("create temporary chunk file: %w", err)
	}

	defer os.Remove(tempPath)

	n, err := file.Write(data)
	if err != nil {
		file.Close()
		logger.LogError(fmt.Errorf("write chunk %q: %w", chunkID, err))
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
		logger.LogError(fmt.Errorf("commit chunk %q: %w", chunkID, err))
		return fmt.Errorf("commit chunk file: %w", err)
	}

	logger.LogStorage(fmt.Sprintf("operation=write chunk=%q bytes=%d result=success", chunkID, len(data)))
	return nil
}

func (s *DiskStorage) Retrieve(ctx context.Context, chunkID string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if !isValidChunkID(chunkID) {
		logger.LogStorage(fmt.Sprintf("operation=read chunk=%q result=invalid", chunkID))
		return nil, ErrInvalidChunkID
	}

	chunkPath := filepath.Join(s.BaseDir, chunkID+".chunk")

	data, err := os.ReadFile(chunkPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			logger.LogStorage(fmt.Sprintf("operation=read chunk=%q result=not_found", chunkID))
			return nil, ErrChunkNotFound
		}

		logger.LogError(fmt.Errorf("read chunk %q: %w", chunkID, err))
		return nil, fmt.Errorf("read chunk: %w", err)
	}

	logger.LogStorage(fmt.Sprintf("operation=read chunk=%q bytes=%d result=success", chunkID, len(data)))
	return data, nil
}

func (s *DiskStorage) Delete(ctx context.Context, chunkID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if !isValidChunkID(chunkID) {
		logger.LogStorage(fmt.Sprintf("operation=delete chunk=%q result=invalid", chunkID))
		return ErrInvalidChunkID
	}

	chunkPath := filepath.Join(s.BaseDir, chunkID+".chunk")

	if err := os.Remove(chunkPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			logger.LogStorage(fmt.Sprintf("operation=delete chunk=%q result=not_found", chunkID))
			return ErrChunkNotFound
		}

		logger.LogError(fmt.Errorf("delete chunk %q: %w", chunkID, err))
		return fmt.Errorf("delete chunk: %w", err)
	}

	logger.LogStorage(fmt.Sprintf("operation=delete chunk=%q result=success", chunkID))
	return nil
}

func isValidChunkID(id string) bool {
	if id == "" || strings.ContainsAny(id, `/\`) {
		return false
	}

	return filepath.Base(id) == id && id != "." && id != ".."
}
