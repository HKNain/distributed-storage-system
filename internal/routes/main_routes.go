package routes

import (
	"net/http"

	"github.com/HKNain/distributed-storage-system/internal/controller"
)

func MainRoutes() *http.ServeMux {
    mux := http.NewServeMux()

    mux.HandleFunc("/health", controller.HealthHandler)

    return mux
}
