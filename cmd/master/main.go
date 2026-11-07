package main

import (
	"log"
	"net/http"
	"os"

	"github.com/joho/godotenv"

	"github.com/HKNain/distributed-storage-system/internal/controller"
	"github.com/HKNain/distributed-storage-system/internal/database"
	"github.com/HKNain/distributed-storage-system/internal/middleware"
	"github.com/HKNain/distributed-storage-system/internal/repository"
	"github.com/HKNain/distributed-storage-system/internal/routes"
	logger "github.com/HKNain/distributed-storage-system/internal/utils"
    "github.com/HKNain/distributed-storage-system/internal/rpc"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file")
	}

	// Initialize logger
	if err := logger.Init(); err != nil {
		log.Fatal(err)
	}

	// Connect to database
	db, err := database.OpenDB("data/storage.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Initialize repositories
	userRepo := repository.NewUserRepository(db)
	fileRepo := repository.NewFileRepository(db)
	chunkRepo := repository.NewChunkRepository(db)

    storageClient, err := rpc.NewStorageClient("localhost:3001")
    if err != nil {
        log.Fatal("Failed to connect to storage node:", err)
    }
    defer storageClient.Close()
	// Initialize controllers
	authController := controller.NewAuthController(userRepo)
	fileController := controller.NewFileController(fileRepo, chunkRepo, storageClient)

	// Initialize middleware
	jwtSecret := os.Getenv("JWT_SECRET")
	if jwtSecret == "" {
		log.Fatal("JWT_SECRET is not configured")
	}

	authMiddleware := middleware.AuthMiddleware(jwtSecret)

	// Initialize routes
	mux := http.NewServeMux()

	routes.AuthRoutes(mux, authController)
	routes.FileRoutes(mux, fileController, authMiddleware)

	// Global logging middleware
	handler := middleware.LoggingMiddleware(mux)

	log.Println("Master server starting on port 3000")

	if err := http.ListenAndServe(":3000", handler); err != nil {
		logger.Errorf("Server stopped: %v", err)
		log.Fatal(err)
	}
}