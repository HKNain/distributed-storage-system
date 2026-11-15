package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"path/filepath"
	"strings"

	"github.com/HKNain/distributed-storage-system/internal/raftcluster"
	"strconv"

	"github.com/HKNain/distributed-storage-system/internal/controller"
	"github.com/HKNain/distributed-storage-system/internal/database"
	"github.com/HKNain/distributed-storage-system/internal/middleware"
	"github.com/HKNain/distributed-storage-system/internal/repository"
	"github.com/HKNain/distributed-storage-system/internal/routes"
	"github.com/HKNain/distributed-storage-system/internal/rpc"
	"github.com/HKNain/distributed-storage-system/internal/utils"
	"github.com/HKNain/distributed-storage-system/internal/wal"
)

func main() {
	// Load environment variables.
	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file:", err)
	}

	// Initialize logger.
	if err := logger.Init(); err != nil {
		log.Fatal(err)
	}

	// Database.
	// Master-specific data directory.
	dataDir := os.Getenv("MASTER_DATA_DIR")
	if dataDir == "" {
		log.Fatal("MASTER_DATA_DIR is not configured")
	}

	dbPath := filepath.Join(dataDir, "storage.db")

	// Open this master's local metadata database.
	db, err := database.OpenDB(dbPath)
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	defer db.Close()

	// Initialize the Raft FSM using this master's local database.
	fsm, err := raftcluster.NewMetadataFSM(db)
	if err != nil {
		log.Fatal("Failed to initialize Raft FSM:", err)
	}

	// Parse initial Raft peers.
	var peers []raftcluster.Peer

	peerConfig := os.Getenv("MASTER_RAFT_PEERS")
	httpPeerConfig := os.Getenv("MASTER_HTTP_PEERS")
	httpPeers := make(map[string]string)

	for _, entry := range strings.Split(httpPeerConfig, ",") {
		parts := strings.SplitN(strings.TrimSpace(entry), "=", 2)
		if len(parts) == 2 &&
			parts[0] != "" &&
			parts[1] != "" {
			httpPeers[parts[0]] = parts[1]
		}
	}

	for _, entry := range strings.Split(peerConfig, ",") {
		parts := strings.SplitN(strings.TrimSpace(entry), "=", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			log.Fatalf("Invalid MASTER_RAFT_PEERS entry: %q", entry)
		}

		peers = append(peers, raftcluster.Peer{
			ID:      parts[0],
			Address: parts[1],
		})
	}

	bootstrap, err := strconv.ParseBool(os.Getenv("MASTER_RAFT_BOOTSTRAP"))
	if err != nil {
		log.Fatal("Invalid MASTER_RAFT_BOOTSTRAP:", err)
	}

	raftConfig := raftcluster.Config{
		NodeID:           os.Getenv("MASTER_NODE_ID"),
		BindAddress:      os.Getenv("MASTER_RAFT_BIND_ADDRESS"),
		AdvertiseAddress: os.Getenv("MASTER_RAFT_ADVERTISE_ADDRESS"),
		DataDir:          filepath.Join(dataDir, "raft"),
		Bootstrap:        bootstrap,
		Peers:            peers,
	}

	if raftConfig.NodeID == "" {
		log.Fatal("MASTER_NODE_ID is not configured")
	}

	// Start this master's Raft node.
	raftNode, err := raftcluster.Start(raftConfig, fsm)
	if err != nil {
		log.Fatal("Failed to start Raft node:", err)
	}
	defer raftNode.Shutdown()

	// Wait for a Raft leader to be elected.
	leaderCtx, cancelLeader := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancelLeader()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		if raftNode.Raft.Leader() != "" {
			log.Printf(
				"Raft leader elected: %s (local node: %s)",
				raftNode.Raft.Leader(),
				raftConfig.NodeID,
			)
			break
		}

		select {
		case <-leaderCtx.Done():
			log.Fatal("Raft leader election timed out")
		case <-ticker.C:
		}
	}

	// Metadata service submits mutations through Raft.
	metadataService, err := raftcluster.NewMetadataService(raftNode.Raft)
	if err != nil {
		log.Fatal("Failed to initialize metadata service:", err)
	}
	// Repositories.
	userRepo := repository.NewUserRepositoryWithMetadata(db, metadataService)
	fileRepo := repository.NewFileRepository(db, metadataService)
	chunkRepo := repository.NewChunkRepository(db, metadataService)

	// Storage node registry.
	nodeRegistry := rpc.NewNodeRegistry()

	// Start master gRPC server before waiting for storage nodes.
	masterGRPC, masterListener, err := rpc.StartMasterGRPCServer(
		os.Getenv("MASTER_GRPC_ADDRESS"),
		nodeRegistry,
	)
	if err != nil {
		log.Fatal("Failed to start master gRPC server:", err)
	}

	go func() {
		if err := masterGRPC.Serve(masterListener); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Printf("Master gRPC server stopped: %v", err)
			}
		}
	}()

	// Start node monitor.
	monitorCtx, cancelMonitor := context.WithCancel(
		context.Background(),
	)
	defer cancelMonitor()

	go nodeRegistry.RunMonitor(monitorCtx)

	// Configure expected storage node count.
	nodeCount := 4

	if value := os.Getenv("STORAGE_NODE_COUNT"); value != "" {
		if _, err := fmt.Sscanf(value, "%d", &nodeCount); err != nil || nodeCount < 1 {
			log.Fatal("Invalid STORAGE_NODE_COUNT")
		}
	}

	// Wait for storage nodes to register and become active.
	// Storage nodes register asynchronously through heartbeats.
	// Do not block master startup waiting for storage nodes.
	log.Printf(
		"Waiting asynchronously for %d storage nodes",
		nodeCount,
	)

	// Initialize shared storage client manager.
	storageManager := rpc.NewStorageClientManager(nodeRegistry)
	replicationManager := rpc.NewReplicationManager(storageManager)
	defer storageManager.Close()

	// Initialize shared WAL writer.
	walWriter, err := wal.OpenWriter(filepath.Join(dataDir, "wal", "uploads.wal"))
	if err != nil {
		log.Fatal("Failed to initialize WAL:", err)
	}

	defer func() {
		if err := walWriter.Close(); err != nil {
			log.Printf("Failed to close WAL: %v", err)
		}
	}()

	repairWorker := rpc.NewRepairWorker(
		chunkRepo,
		storageManager,
		walWriter,
	)

	repairCoordinator := rpc.NewRepairCoordinator(
		walWriter,
		repairWorker,
	)

	// Controllers.
	authController := controller.NewAuthController(userRepo)

	fileController := controller.NewFileController(
		fileRepo,
		chunkRepo,
		storageManager,
		replicationManager,
		walWriter,
		repairCoordinator,
	)

	// JWT authentication middleware.
	jwtSecret := os.Getenv("JWT_SECRET")
	if jwtSecret == "" {
		log.Fatal("JWT_SECRET is not configured")
	}

	authMiddleware := middleware.AuthMiddleware(jwtSecret)
	uploadHandler := middleware.ForwardToLeader(
		raftNode.Raft,
		httpPeersByRaftAddress(peerConfig, httpPeers),
		http.HandlerFunc(fileController.UploadHandler),
	)

	// Routes.
	mux := http.NewServeMux()

	routes.AuthRoutes(mux, authController)
	routes.FileRoutes(mux, fileController, authMiddleware, uploadHandler)

	// Middleware.
	handler := middleware.LoggingMiddleware(mux)

	// Recover interrupted uploads before accepting new uploads.
	log.Println("Starting interrupted upload recovery...")

	recoveryCtx, cancelRecovery := context.WithTimeout(
		context.Background(),
		30*time.Minute,
	)

	if err := fileController.RecoverInterruptedUploads(recoveryCtx); err != nil {
		cancelRecovery()
		log.Fatalf("Upload recovery failed: %v", err)
	}

	cancelRecovery()

	log.Println("Upload recovery completed successfully")

	// Start asynchronous replication after upload recovery.
	replicationWorker := rpc.NewReplicationWorker(
		chunkRepo,
		storageManager,
		10*time.Second,
	)

	replicationWorkerCtx, stopReplicationWorker := context.WithCancel(
		context.Background(),
	)

	replicationWorkerDone := make(chan struct{})

	go func() {
		defer close(replicationWorkerDone)
		replicationWorker.Run(replicationWorkerCtx)
	}()

	repairCtx, stopRepairWorker := context.WithCancel(context.Background())
	repairDone := make(chan struct{})

	go func() {
		defer close(repairDone)
		repairWorker.Run(repairCtx, 10*time.Second)
	}()

	// HTTP server.
	httpServer := &http.Server{
		Addr:              os.Getenv("MASTER_HTTP_ADDRESS"),
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Println("Master HTTP server listening on :3000")

		if err := httpServer.ListenAndServe(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	log.Println("Master gRPC control server listening on :4000")
	log.Println("Master server is ready")

	// Graceful shutdown.
	signalCtx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	<-signalCtx.Done()

	log.Println("Shutdown signal received")

	// Stop accepting new requests.
	shutdownCtx, cancelShutdown := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancelShutdown()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP graceful shutdown failed: %v", err)
	}

	// Stop repair worker before closing WAL and storage clients.
	stopRepairWorker()

	select {
	case <-repairDone:
		log.Println("Repair worker shut down")
	case <-time.After(5 * time.Second):
		log.Println("Repair worker shutdown timed out")
	}
	// Stop asynchronous replication before closing storage clients.
	stopReplicationWorker()

	select {
	case <-replicationWorkerDone:
		log.Println("Replication worker shut down")
	case <-time.After(5 * time.Second):
		log.Println("Replication worker shutdown timed out")
	}

	// Stop node monitoring.
	cancelMonitor()

	// Stop gRPC server.
	masterGRPC.GracefulStop()

	log.Println("Master server stopped")
}

func httpPeersByRaftAddress(
	raftPeerConfig string,
	httpPeersByID map[string]string,
) map[string]string {
	result := make(map[string]string)

	for _, entry := range strings.Split(raftPeerConfig, ",") {
		parts := strings.SplitN(strings.TrimSpace(entry), "=", 2)
		if len(parts) != 2 {
			continue
		}

		nodeID := strings.TrimSpace(parts[0])
		raftAddress := strings.TrimSpace(parts[1])
		httpAddress := strings.TrimSpace(httpPeersByID[nodeID])

		if nodeID != "" && raftAddress != "" && httpAddress != "" {
			result[raftAddress] = httpAddress
		}
	}

	return result
}
