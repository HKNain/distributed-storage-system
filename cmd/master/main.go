
package main

import (
	"context"
	"errors"
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
	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file:", err)
	}

	if err := logger.Init(); err != nil {
		log.Fatal(err)
	}

	// Database
	db, err := database.OpenDB("data/storage.db")
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	defer db.Close()

	// Repositories
	userRepo := repository.NewUserRepository(db)
	fileRepo := repository.NewFileRepository(db)
	chunkRepo := repository.NewChunkRepository(db)

	// Storage node client
	storageClient, err := rpc.NewStorageClient("localhost:3001")
	if err != nil {
		log.Fatal("Failed to initialize storage client:", err)
	}
	defer storageClient.Close()

	// WAL writer: one shared instance for the master process.
	walWriter, err := wal.OpenWriter("data/wal/uploads.wal")
	if err != nil {
		log.Fatal("Failed to initialize WAL:", err)
	}
	defer func() {
		if err := walWriter.Close(); err != nil {
			log.Printf("Failed to close WAL: %v", err)
		}
	}()

	// Controllers
	authController := controller.NewAuthController(userRepo)

	// WAL writer is injected into FileController.
	// temporary node assign ofr :3001 till further implementation of node registry and selection is done
	storageNodeID := os.Getenv("STORAGE_NODE_ID")
	if storageNodeID == "" {
		log.Fatal("STORAGE_NODE_ID is not configured in master")
	}

	fileController := controller.NewFileController(
		fileRepo,
		chunkRepo,
		storageClient,
		walWriter,
		storageNodeID,
	)

	// Middleware
	jwtSecret := os.Getenv("JWT_SECRET")
	if jwtSecret == "" {
		log.Fatal("JWT_SECRET is not configured")
	}

	authMiddleware := middleware.AuthMiddleware(jwtSecret)

	// Routes
	mux := http.NewServeMux()
	routes.AuthRoutes(mux, authController)
	routes.FileRoutes(mux, fileController, authMiddleware)

	handler := middleware.LoggingMiddleware(mux)

	// Storage node registry and monitor
	nodeRegistry := rpc.NewNodeRegistry()

	monitorCtx, cancelMonitor := context.WithCancel(context.Background())
	defer cancelMonitor()

	go nodeRegistry.RunMonitor(monitorCtx)

	// Master gRPC control server
	masterGRPC, masterListener, err := rpc.StartMasterGRPCServer(
		":4000",
		nodeRegistry,
	)
	if err != nil {
		log.Fatal("Failed to start master gRPC server:", err)
	}
	defer masterGRPC.Stop()

	go func() {
		if err := masterGRPC.Serve(masterListener); err != nil {
			log.Printf("Master gRPC server stopped: %v", err)
		}
	}()

	// HTTP server
	httpServer := &http.Server{
		Addr:    ":3000",
		Handler: handler,
	}

	go func() {
		log.Println("Master HTTP server listening on :3000")
		if err := httpServer.ListenAndServe(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	log.Println("Master gRPC control server listening on :4000")

	// Graceful shutdown
	signalCtx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	<-signalCtx.Done()
	log.Println("Shutdown signal received")

	shutdownCtx, cancelShutdown := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancelShutdown()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP graceful shutdown failed: %v", err)
	}

	masterGRPC.GracefulStop()
	log.Println("Master server stopped")
}
