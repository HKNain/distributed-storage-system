package middleware

import (
	"net/http"
	"time"

	"github.com/HKNain/distributed-storage-system/internal/utils"

)

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(status int) {
	rw.status = status
	rw.ResponseWriter.WriteHeader(status)
}

func (rw *responseWriter) Write(data []byte) (int, error) {
	if rw.status == 0 {
		rw.status = http.StatusOK
	}

	return rw.ResponseWriter.Write(data)
}

func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		start := time.Now()

		rw := &responseWriter{
			ResponseWriter: w,
			status:         0,
		}

		next.ServeHTTP(rw, r)

		if rw.status == 0 {
			rw.status = http.StatusOK
		}

		duration := time.Since(start)

		logger.LogAPI(
			r.Method,
			r.URL.Path,
			rw.status,
			duration.String(),
		)
	})
}
