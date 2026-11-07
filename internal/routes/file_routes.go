package routes

import (
	"net/http"

	"github.com/HKNain/distributed-storage-system/internal/controller"
)

func FileRoutes(
	mux *http.ServeMux,
	fileController *controller.FileController,
	authMiddleware func(http.Handler) http.Handler,
) {
	mux.Handle(
		"POST /files/upload",
		authMiddleware(http.HandlerFunc(fileController.UploadHandler)),
	)
	mux.Handle(
		"GET /files/{id}/download",
		authMiddleware(http.HandlerFunc(fileController.DownloadHandler)),
	)
}