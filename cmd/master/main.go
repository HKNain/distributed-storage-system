package main

import (
    "log"
    "net/http"

    "github.com/HKNain/distributed-storage-system/internal/routes"
    "github.com/HKNain/distributed-storage-system/internal/utils"
    "github.com/HKNain/distributed-storage-system/internal/middleware"
)

func main() {
	err := logger.Init()
    if err != nil {
        log.Fatal(err)
    }

    mux := routes.MainRoutes()

	handler := middleware.LoggingMiddleware(mux)

    log.Println("Master server starting on port 3000")

	err = http.ListenAndServe(":3000", handler)
    if err != nil {
        logger.Errorf("Server stopped: %v", err)
        log.Fatal(err)
    }
}