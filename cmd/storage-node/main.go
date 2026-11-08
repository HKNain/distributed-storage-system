
package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	storagepb "github.com/HKNain/distributed-storage-system/api/proto"
	"github.com/HKNain/distributed-storage-system/internal/rpc"
	"github.com/HKNain/distributed-storage-system/internal/storage"

	"google.golang.org/grpc"
)

func main() {
	port := os.Getenv("STORAGE_NODE_PORT")
	if port == "" {
		port = "3001"
	}

	storageDir := os.Getenv("STORAGE_DIR")
	if storageDir == "" {
		storageDir = "data/chunks"
	}

	nodeID := os.Getenv("STORAGE_NODE_ID")
	if nodeID == "" {
		log.Fatal("STORAGE_NODE_ID environment variable is required")
	}

	nodeAddress := os.Getenv("STORAGE_NODE_ADDRESS")
	if nodeAddress == "" {
		nodeAddress = "localhost:" + port
	}

	// Initialize disk storage.
	diskStorage, err := storage.NewDiskStorage(storageDir)
	if err != nil {
		log.Fatalf("failed to initialize disk storage: %v", err)
	}

	// Create TCP listener.
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen on port %s: %v", port, err)
	}
	defer listener.Close()

	// Create storage gRPC server.
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(5*1024*1024),
		grpc.MaxSendMsgSize(5*1024*1024),
	)

	storageServer := rpc.NewStorageServer(diskStorage)

	storagepb.RegisterStorageServiceServer(
		grpcServer,
		storageServer,
	)

	// Context for graceful shutdown.
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// Start heartbeat client.
	go func() {
		if err := rpc.RunHeartbeatClient(
			ctx,
			"localhost:4000",
			nodeID,
			nodeAddress,
		); err != nil {
			log.Printf("Heartbeat client error: %v", err)
		}
	}()

	log.Printf("Storage Node %s listening on port %s", nodeID, port)
	log.Printf("Storage directory: %s", storageDir)
	log.Printf("Master address: localhost:4000")

	// Serve blocks, so run it in a goroutine.
	serverErr := make(chan error, 1)

	go func() {
		serverErr <- grpcServer.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		log.Println("Shutting down storage node...")
		grpcServer.GracefulStop()

	case err := <-serverErr:
		if err != nil {
			log.Fatalf("storage node failed: %v", err)
		}
	}
}
