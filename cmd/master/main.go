package main

import (
    "log"
    "net/http"
    "github.com/joho/godotenv"

    "github.com/HKNain/distributed-storage-system/internal/routes"
    "github.com/HKNain/distributed-storage-system/internal/utils"
    "github.com/HKNain/distributed-storage-system/internal/middleware"
    "github.com/HKNain/distributed-storage-system/internal/database"
    "github.com/HKNain/distributed-storage-system/internal/repository"
    "github.com/HKNain/distributed-storage-system/internal/controller"
)

func main() {
    err := godotenv.Load()
    if err != nil {
        log.Fatal("Error loading .env file")
    }
    // Connect to the database.
    db, err := database.OpenDB("data/storage.db")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Initialize repositories
    userRepo := repository.NewUserRepository(db)

    // Initialize controllers
    authController := controller.NewAuthController(userRepo)
	
    err = logger.Init()
    if err != nil {
        log.Fatal(err)
    }

    mux := http.NewServeMux()

    // Initialize routes
    routes.AuthRoutes(mux, authController)

	handler := middleware.LoggingMiddleware(mux)

    log.Println("Master server starting on port 3000")

	err = http.ListenAndServe(":3000", handler)
    if err != nil {
        logger.Errorf("Server stopped: %v", err)
        log.Fatal(err)
    }
}