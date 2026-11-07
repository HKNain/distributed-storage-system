
package rpc

import (
	"context"
	"errors"
	"crypto/sha256"
	"encoding/hex"
	"bytes"

	storagepb "github.com/HKNain/distributed-storage-system/api/proto"
	"github.com/HKNain/distributed-storage-system/internal/storage"

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
		return nil, status.Error(codes.InvalidArgument, "invalid chunk ID")
	}

	if len(req.Data) == 0 {
		return nil, status.Error(codes.InvalidArgument, "chunk data cannot be empty")
	}

	if len(req.Sha256) != 64 {
		return nil, status.Error(codes.InvalidArgument, "invalid SHA-256 checksum")
	}

	expectedHash, err := hex.DecodeString(req.Sha256)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid SHA-256 checksum")
	}

	actualHash := sha256.Sum256(req.Data)

	if !bytes.Equal(actualHash[:], expectedHash) {
		return nil, status.Error(codes.DataLoss, "chunk checksum mismatch")
	}

	err = s.disk.Store(ctx, req.ChunkId, req.Data)
	if err != nil {
		if errors.Is(err, storage.ErrInvalidChunkID) {
			return nil, status.Error(codes.InvalidArgument, "invalid chunk ID")
		}

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
		return nil, status.Error(codes.InvalidArgument, "invalid chunk ID")
	}

	data, err := s.disk.Retrieve(ctx, req.ChunkId)
	if err != nil {
		if errors.Is(err, storage.ErrChunkNotFound) {
			return nil, status.Error(codes.NotFound, "chunk not found")
		}

		if errors.Is(err, storage.ErrInvalidChunkID) {
			return nil, status.Error(codes.InvalidArgument, "invalid chunk ID")
		}

		return nil, status.Error(codes.Internal, "failed to retrieve chunk")
	}

	return &storagepb.GetChunkResponse{
		Data: data,
	}, nil
}

func (s *StorageServer) DeleteChunk(
	ctx context.Context,
	req *storagepb.DeleteChunkRequest,
) (*storagepb.DeleteChunkResponse, error) {

	if req == nil || req.ChunkId == "" {
		return nil, status.Error(codes.InvalidArgument, "invalid chunk ID")
	}

	err := s.disk.Delete(ctx, req.ChunkId)
	if err != nil {
		if errors.Is(err, storage.ErrChunkNotFound) {
			return nil, status.Error(codes.NotFound, "chunk not found")
		}

		if errors.Is(err, storage.ErrInvalidChunkID) {
			return nil, status.Error(codes.InvalidArgument, "invalid chunk ID")
		}

		return nil, status.Error(codes.Internal, "failed to delete chunk")
	}

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
