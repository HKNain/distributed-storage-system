package middleware

import (
	"net/http"
	"time"

	"github.com/HKNain/distributed-storage-system/internal/utils"
)

type responseRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (r *responseRecorder) WriteHeader(statusCode int) {
	r.statusCode = statusCode
	r.ResponseWriter.WriteHeader(statusCode)
}

func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		recorder := &responseRecorder{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
		}

		next.ServeHTTP(recorder, r)

		duration := time.Since(start)

		logger.LogAPI(logger.APIEvent{
			Method:     r.Method,
			Path:       r.URL.RequestURI(),
			StatusCode: recorder.statusCode,
			Duration:   duration,
			RequestID:  r.Header.Get("X-Request-ID"),
		})
	})
}
