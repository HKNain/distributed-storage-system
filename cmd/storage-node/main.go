package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"

	storagepb "github.com/HKNain/distributed-storage-system/api/proto"
	"github.com/HKNain/distributed-storage-system/internal/rpc"
	"github.com/HKNain/distributed-storage-system/internal/storage"

	"google.golang.org/grpc"
)

func main() {
	// Load shared configuration.
	// Existing process environment variables take precedence over .env.
	if err := godotenv.Load(); err != nil {
		log.Printf("Could not load .env: %v", err)
	}

	port := os.Getenv("STORAGE_NODE_PORT")
	if port == "" {
		port = "3001"
	}

	nodeID := os.Getenv("STORAGE_NODE_ID")
	if nodeID == "" {
		log.Fatal("STORAGE_NODE_ID environment variable is required")
	}

	host := os.Getenv("STORAGE_NODE_HOST")
	if host == "" {
		host = "localhost"
	}

	nodeAddress := os.Getenv("STORAGE_NODE_ADDRESS")
	if nodeAddress == "" {
		nodeAddress = host + ":" + port
	}

	storageDir := os.Getenv("STORAGE_DIR")
	if storageDir == "" {
		storageDir = "data/chunks"
	}

	masterAddress := os.Getenv("MASTER_GRPC_ADDRESS")
	if masterAddress == "" {
		masterAddress = "localhost:4000"
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

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// Start the gRPC server before sending heartbeats.
	serverErr := make(chan error, 1)

	go func() {
		serverErr <- grpcServer.Serve(listener)
	}()

	log.Printf("Storage Node %s started", nodeID)
	log.Printf("Listening address: %s", nodeAddress)
	log.Printf("Storage directory: %s", storageDir)
	log.Printf("Master address: %s", masterAddress)
	log.Printf("Heartbeat interval: %s", rpc.HeartbeatIntervalFromEnv())

	// Start heartbeat client.
	go func() {
		if err := rpc.RunHeartbeatClient(
			ctx,
			masterAddress,
			nodeID,
			nodeAddress,
		); err != nil {
			log.Printf("Heartbeat client error for %s: %v", nodeID, err)
		}
	}()

	select {
	case <-ctx.Done():
		log.Printf("Shutting down storage node %s...", nodeID)
		grpcServer.GracefulStop()

	case err := <-serverErr:
		if err != nil {
			log.Fatalf("storage node failed: %v", err)
		}
	}
}