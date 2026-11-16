package rpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	storagepb "github.com/HKNain/distributed-storage-system/api/proto"
	"github.com/HKNain/distributed-storage-system/internal/storage"
	"github.com/HKNain/distributed-storage-system/internal/utils"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type StorageServer struct {
	storagepb.UnimplementedStorageServiceServer
	disk storage.Storage
}

func NewStorageServer(disk storage.Storage) *StorageServer {
	return &StorageServer{
		disk: disk,
	}
}
func (s *StorageServer) StoreChunk(
	ctx context.Context,
	req *storagepb.StoreChunkRequest,
) (*storagepb.StoreChunkResponse, error) {

	if req == nil || req.ChunkId == "" {
		logger.LogStorage("operation=store result=invalid_request")
		return nil, status.Error(codes.InvalidArgument, "invalid chunk ID")
	}

	if len(req.Data) == 0 {
		logger.LogStorage("operation=store result=empty_chunk")
		return nil, status.Error(codes.InvalidArgument, "chunk data cannot be empty")
	}

	if len(req.Sha256) != 64 {
		logger.LogStorage(fmt.Sprintf("operation=store chunk=%q result=invalid_checksum", req.ChunkId))
		return nil, status.Error(codes.InvalidArgument, "invalid SHA-256 checksum")
	}

	expectedHash, err := hex.DecodeString(req.Sha256)
	if err != nil {
		logger.LogStorage(fmt.Sprintf("operation=store chunk=%q result=invalid_checksum", req.ChunkId))
		return nil, status.Error(codes.InvalidArgument, "invalid SHA-256 checksum")
	}

	actualHash := sha256.Sum256(req.Data)

	if !bytes.Equal(actualHash[:], expectedHash) {
		logger.LogStorage(fmt.Sprintf("operation=store chunk=%q result=checksum_mismatch", req.ChunkId))
		return nil, status.Error(codes.DataLoss, "chunk checksum mismatch")
	}

	err = s.disk.Store(ctx, req.ChunkId, req.Data)
	if err != nil {
		logger.LogError(fmt.Errorf("store chunk %q: %w", req.ChunkId, err))
		if errors.Is(err, storage.ErrInvalidChunkID) {
			return nil, status.Error(codes.InvalidArgument, "invalid chunk ID")
		}

		logger.LogStorage(fmt.Sprintf("operation=store chunk=%q bytes=%d result=success", req.ChunkId, len(req.Data)))
		return nil, status.Error(codes.Internal, "failed to store chunk")
	}

	return &storagepb.StoreChunkResponse{
		Success: true,
		Message: "chunk stored successfully",
	}, nil
}

func (s *StorageServer) GetChunk(
	ctx context.Context,
	req *storagepb.GetChunkRequest,
) (*storagepb.GetChunkResponse, error) {

	if req == nil || req.ChunkId == "" {
		logger.LogStorage("operation=read result=invalid_request")
		return nil, status.Error(codes.InvalidArgument, "invalid chunk ID")
	}

	data, err := s.disk.Retrieve(ctx, req.ChunkId)
	if err != nil {
		if errors.Is(err, storage.ErrChunkNotFound) {
			logger.LogStorage(fmt.Sprintf("operation=read chunk=%q result=not_found", req.ChunkId))
			return nil, status.Error(codes.NotFound, "chunk not found")
		}

		if errors.Is(err, storage.ErrInvalidChunkID) {
			logger.LogStorage(fmt.Sprintf("operation=read chunk=%q result=invalid_chunk", req.ChunkId))
			return nil, status.Error(codes.InvalidArgument, "invalid chunk ID")
		}

		logger.LogError(fmt.Errorf("retrieve chunk %q: %w", req.ChunkId, err))
		return nil, status.Error(codes.Internal, "failed to retrieve chunk")
	}

	logger.LogStorage(fmt.Sprintf("operation=read chunk=%q bytes=%d result=success", req.ChunkId, len(data)))
	return &storagepb.GetChunkResponse{
		Data: data,
	}, nil
}

func (s *StorageServer) DeleteChunk(
	ctx context.Context,
	req *storagepb.DeleteChunkRequest,
) (*storagepb.DeleteChunkResponse, error) {

	if req == nil || req.ChunkId == "" {
		logger.LogStorage("operation=delete result=invalid_request")
		return nil, status.Error(codes.InvalidArgument, "invalid chunk ID")
	}

	err := s.disk.Delete(ctx, req.ChunkId)
	if err != nil {
		if errors.Is(err, storage.ErrChunkNotFound) {
			logger.LogStorage(fmt.Sprintf("operation=delete chunk=%q result=not_found", req.ChunkId))
			return nil, status.Error(codes.NotFound, "chunk not found")
		}

		if errors.Is(err, storage.ErrInvalidChunkID) {
			logger.LogStorage(fmt.Sprintf("operation=delete chunk=%q result=invalid_chunk", req.ChunkId))
			return nil, status.Error(codes.InvalidArgument, "invalid chunk ID")
		}

		logger.LogError(fmt.Errorf("delete chunk %q: %w", req.ChunkId, err))
		return nil, status.Error(codes.Internal, "failed to delete chunk")
	}

	logger.LogStorage(fmt.Sprintf("operation=delete chunk=%q result=success", req.ChunkId))
	return &storagepb.DeleteChunkResponse{
		Success: true,
		Message: "chunk deleted successfully",
	}, nil
}

func (s *StorageServer) HealthCheck(
	ctx context.Context,
	req *storagepb.HealthCheckRequest,
) (*storagepb.HealthCheckResponse, error) {

	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	return &storagepb.HealthCheckResponse{
		Healthy: true,
	}, nil
}
