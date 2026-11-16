package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/HKNain/distributed-storage-system/internal/models"
	"github.com/HKNain/distributed-storage-system/internal/repository"
	"github.com/HKNain/distributed-storage-system/internal/utils"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

type AuthController struct {
	UserRepo  *repository.UserRepository
	JWTSecret []byte
}

func NewAuthController(repo *repository.UserRepository) *AuthController {
	return &AuthController{
		UserRepo:  repo,
		JWTSecret: []byte(os.Getenv("JWT_SECRET")),
	}
}

// SignupHandler registers a new user.
func (ac *AuthController) SignupHandler(w http.ResponseWriter, r *http.Request) {

	var input models.SignupRequest

	err := json.NewDecoder(r.Body).Decode(&input)
	if err != nil {
		logger.LogAuth(logger.AuthEvent{Action: "signup", Success: false, StatusCode: http.StatusBadRequest, Reason: "invalid JSON body"})
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	input.Username = strings.ToLower(strings.TrimSpace(input.Username))
	input.Email = strings.ToLower(strings.TrimSpace(input.Email))

	if input.Username == "" || input.Email == "" || input.Password == "" {
		logger.LogAuth(logger.AuthEvent{Action: "signup", Identifier: input.Username, Success: false, StatusCode: http.StatusBadRequest, Reason: "required fields missing"})
		http.Error(w, "All fields are required", http.StatusBadRequest)
		return
	}

	if len(input.Password) < 8 {
		logger.LogAuth(logger.AuthEvent{Action: "signup", Identifier: input.Username, Success: false, StatusCode: http.StatusBadRequest, Reason: "password too short"})
		http.Error(w, "Password must contain at least 8 characters", http.StatusBadRequest)
		return
	}

	if len(ac.JWTSecret) == 0 {
		logger.LogError(errors.New("JWT secret is not configured"))
		http.Error(w, "Server configuration error", http.StatusInternalServerError)
		return
	}

	// Hash password.
	hashedPassword, err := bcrypt.GenerateFromPassword(
		[]byte(input.Password),
		bcrypt.DefaultCost,
	)
	if err != nil {
		logger.LogError(fmt.Errorf("hash password for user %q: %w", input.Username, err))
		http.Error(w, "Could not process password", http.StatusInternalServerError)
		return
	}

	user := models.User{
		Username: input.Username,
		Email:    input.Email,
		Password: string(hashedPassword),
	}

	err = ac.UserRepo.CreateUser(user)
	if err != nil {

		if errors.Is(err, repository.ErrUserExists) {
			logger.LogAuth(logger.AuthEvent{Action: "signup", Identifier: input.Username, Success: false, StatusCode: http.StatusConflict, Reason: "username already exists"})
			http.Error(w, "Username already exists", http.StatusConflict)
			return
		}

		if errors.Is(err, repository.ErrEmailExists) {
			logger.LogAuth(logger.AuthEvent{Action: "signup", Identifier: input.Username, Success: false, StatusCode: http.StatusConflict, Reason: "email already registered"})
			http.Error(w, "Email already registered", http.StatusConflict)
			return
		}

		http.Error(w, "Could not create user", http.StatusInternalServerError)
		logger.LogError(fmt.Errorf("create user %q: %w", input.Username, err))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	json.NewEncoder(w).Encode(map[string]string{
		"message": "User registered successfully",
	})
	logger.LogAuth(logger.AuthEvent{Action: "signup", Identifier: input.Username, Success: true, StatusCode: http.StatusCreated})
}

// LoginHandler authenticates an existing user.
func (ac *AuthController) LoginHandler(w http.ResponseWriter, r *http.Request) {

	var input models.LoginRequest
	err := json.NewDecoder(r.Body).Decode(&input)
	if err != nil {
		logger.LogAuth(logger.AuthEvent{Action: "login", Success: false, StatusCode: http.StatusBadRequest, Reason: "invalid JSON body"})
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	input.Username = strings.ToLower(strings.TrimSpace(input.Username))

	if input.Username == "" || input.Password == "" {
		logger.LogAuth(logger.AuthEvent{Action: "login", Identifier: input.Username, Success: false, StatusCode: http.StatusBadRequest, Reason: "required fields missing"})
		http.Error(w, "Username and password are required", http.StatusBadRequest)
		return
	}

	if len(ac.JWTSecret) == 0 {
		logger.LogError(errors.New("JWT secret is not configured"))
		http.Error(w, "Server configuration error", http.StatusInternalServerError)
		return
	}
	// Find user.
	user, err := ac.UserRepo.GetUserByUsername(input.Username)
	if err != nil {
		logger.LogAuth(logger.AuthEvent{Action: "login", Identifier: input.Username, Success: false, StatusCode: http.StatusUnauthorized, Reason: "invalid username or password"})
		http.Error(w, "Invalid username or password", http.StatusUnauthorized)
		return
	}
	// Compare password with stored bcrypt hash.
	err = bcrypt.CompareHashAndPassword(
		[]byte(user.Password),
		[]byte(input.Password),
	)
	if err != nil {
		logger.LogAuth(logger.AuthEvent{Action: "login", Identifier: input.Username, Success: false, StatusCode: http.StatusUnauthorized, Reason: "password mismatch"})
		http.Error(w, "Invalid username or password", http.StatusUnauthorized)
		return
	}
	// Generate JWT.
	claims := jwt.MapClaims{
		"user_id":  user.ID,
		"username": user.Username,
		"exp":      time.Now().Add(24 * time.Hour).Unix(),
		"iat":      time.Now().Unix(),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(ac.JWTSecret)
	if err != nil {
		logger.LogError(fmt.Errorf("generate token for user %q: %w", input.Username, err))
		http.Error(w, "Could not generate token", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"message": "Login successful",
		"token":   tokenString,
	})
	logger.LogAuth(logger.AuthEvent{Action: "login", Identifier: input.Username, Success: true, StatusCode: http.StatusOK})
}
