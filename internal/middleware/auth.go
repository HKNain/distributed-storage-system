package middleware

import (
	"context"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"

	"github.com/HKNain/distributed-storage-system/internal/utils"
)

type ContextKey string

const UserIDKey ContextKey = "user_id"

func AuthMiddleware(jwtSecret string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")

			if authHeader == "" {
				logger.LogAuth(logger.AuthEvent{Action: "authenticate", Success: false, StatusCode: http.StatusUnauthorized, Reason: "missing authorization token"})
				http.Error(w, "Missing authorization token", http.StatusUnauthorized)
				return
			}

			parts := strings.SplitN(authHeader, " ", 2)
			if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
				logger.LogAuth(logger.AuthEvent{Action: "authenticate", Success: false, StatusCode: http.StatusUnauthorized, Reason: "invalid authorization header"})
				http.Error(w, "Invalid authorization header", http.StatusUnauthorized)
				return
			}

			token, err := jwt.Parse(parts[1], func(token *jwt.Token) (interface{}, error) {
				if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
					return nil, jwt.ErrTokenSignatureInvalid
				}
				return []byte(jwtSecret), nil
			})

			if err != nil || !token.Valid {
				logger.LogAuth(logger.AuthEvent{Action: "authenticate", Success: false, StatusCode: http.StatusUnauthorized, Reason: "invalid or expired token"})
				http.Error(w, "Invalid or expired token", http.StatusUnauthorized)
				return
			}

			claims, ok := token.Claims.(jwt.MapClaims)
			if !ok {
				logger.LogAuth(logger.AuthEvent{Action: "authenticate", Success: false, StatusCode: http.StatusUnauthorized, Reason: "invalid token claims"})
				http.Error(w, "Invalid token claims", http.StatusUnauthorized)
				return
			}

			userID, ok := claims["user_id"].(string)
			if !ok || userID == "" {
				logger.LogAuth(logger.AuthEvent{Action: "authenticate", Success: false, StatusCode: http.StatusUnauthorized, Reason: "invalid user identity"})
				http.Error(w, "Invalid user identity", http.StatusUnauthorized)
				return
			}

			logger.LogAuth(logger.AuthEvent{Action: "authenticate", Identifier: userID, Success: true, StatusCode: http.StatusOK})
			ctx := context.WithValue(r.Context(), UserIDKey, userID)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
