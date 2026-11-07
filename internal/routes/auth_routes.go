package routes

import (
    "net/http"
    "github.com/HKNain/distributed-storage-system/internal/controller"
)

func AuthRoutes(mux *http.ServeMux, authController *controller.AuthController) {
    mux.HandleFunc("/auth/signup", authController.SignupHandler)
    mux.HandleFunc("/auth/login", authController.LoginHandler)
}