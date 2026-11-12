
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
	db, err := database.OpenDB("data/storage.db")
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	defer db.Close()

	// Repositories.
	userRepo := repository.NewUserRepository(db)
	fileRepo := repository.NewFileRepository(db)
	chunkRepo := repository.NewChunkRepository(db)

	// Storage node registry.
	nodeRegistry := rpc.NewNodeRegistry()

	// Start master gRPC server before waiting for storage nodes.
	masterGRPC, masterListener, err := rpc.StartMasterGRPCServer(
		":4000",
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
	waitCtx, cancelWait := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)

	if err := nodeRegistry.WaitForActiveNodes(waitCtx, nodeCount); err != nil {
		cancelWait()
		log.Fatalf("Storage nodes did not become ready: %v", err)
	}

	cancelWait()

	log.Printf("%d storage nodes are active", nodeCount)

	// Initialize shared storage client manager.
	storageManager := rpc.NewStorageClientManager(nodeRegistry)
	replicationManager := rpc.NewReplicationManager(storageManager)
	defer storageManager.Close()

	// Initialize shared WAL writer.
	walWriter, err := wal.OpenWriter("data/wal/uploads.wal")
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

	// Routes.
	mux := http.NewServeMux()

	routes.AuthRoutes(mux, authController)
	routes.FileRoutes(mux, fileController, authMiddleware)

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
		Addr:              ":3000",
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
