package controller

import (
    "encoding/json"
    "net/http"
)

func HealthHandler(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusOK)

    response := map[string]string{
        "status":  "success",
        "message": "Master server is running",
    }

    json.NewEncoder(w).Encode(response)
}