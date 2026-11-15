package rpc

import (
	"context"
	"errors"
	"fmt"
	"time"

	storagepb "github.com/HKNain/distributed-storage-system/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	storageRPCDeadline  = 60 * time.Second
	maxChunkMessageSize = 8 * 1024 * 1024
)

type StorageClient struct {
	conn   *grpc.ClientConn
	client storagepb.StorageServiceClient
}

func NewStorageClient(address string) (*StorageClient, error) {
	if address == "" {
		return nil, errors.New("storage node address cannot be empty")
	}

	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxChunkMessageSize),
			grpc.MaxCallSendMsgSize(maxChunkMessageSize),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create storage node connection: %w", err)
	}

	return &StorageClient{
		conn:   conn,
		client: storagepb.NewStorageServiceClient(conn),
	}, nil
}

func (s *StorageClient) StoreChunk(
	ctx context.Context,
	chunkID string,
	data []byte,
	checksum string,
) error {
	ctx, cancel := context.WithTimeout(ctx, storageRPCDeadline)
	defer cancel()

	resp, err := s.client.StoreChunk(ctx, &storagepb.StoreChunkRequest{
		ChunkId: chunkID,
		Data:    data,
		Sha256:  checksum,
	})
	if err != nil {
		return fmt.Errorf("store chunk RPC: %w", err)
	}

	if !resp.Success {
		return errors.New("storage node rejected chunk")
	}

	return nil
}

func (s *StorageClient) GetChunk(
	ctx context.Context,
	chunkID string,
) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, storageRPCDeadline)
	defer cancel()

	resp, err := s.client.GetChunk(ctx, &storagepb.GetChunkRequest{
		ChunkId: chunkID,
	})
	if err != nil {
		return nil, fmt.Errorf("get chunk RPC: %w", err)
	}

	return resp.Data, nil
}

func (s *StorageClient) DeleteChunk(
	ctx context.Context,
	chunkID string,
) error {
	ctx, cancel := context.WithTimeout(ctx, storageRPCDeadline)
	defer cancel()

	resp, err := s.client.DeleteChunk(ctx, &storagepb.DeleteChunkRequest{
		ChunkId: chunkID,
	})
	if err != nil {
		return fmt.Errorf("delete chunk RPC: %w", err)
	}

	if !resp.Success {
		return errors.New("storage node rejected deletion")
	}

	return nil
}

func (s *StorageClient) HealthCheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := s.client.HealthCheck(ctx, &storagepb.HealthCheckRequest{})
	if err != nil {
		return fmt.Errorf("storage node health check: %w", err)
	}

	if !resp.Healthy {
		return errors.New("storage node is unhealthy")
	}

	return nil
}

func (s *StorageClient) Close() error {
	return s.conn.Close()
}
