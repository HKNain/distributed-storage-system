package routes

import (
	"net/http"

	"github.com/HKNain/distributed-storage-system/internal/controller"
)

func MainRoutes(mux *http.ServeMux)  {
    mux.HandleFunc("/health", controller.HealthHandler)

}
