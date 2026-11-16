package logger

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	errorLogger       *log.Logger
	apiLogger         *log.Logger
	authLogger        *log.Logger
	storageLogger     *log.Logger
	replicationLogger *log.Logger
	raftLogger        *log.Logger
	clusterLogger     *log.Logger
	transferLogger    *log.Logger
	recoveryLogger    *log.Logger

	files []*os.File
	mu    sync.Mutex
)

// Init initializes all log files.
func Init() error {
	mu.Lock()
	defer mu.Unlock()

	if err := os.MkdirAll("logs", 0755); err != nil {
		return err
	}

	logTypes := map[string]**log.Logger{
		"error.log":       &errorLogger,
		"api.log":         &apiLogger,
		"auth.log":        &authLogger,
		"storage.log":     &storageLogger,
		"replication.log": &replicationLogger,
		"raft.log":        &raftLogger,
		"cluster.log":     &clusterLogger,
		"transfer.log":    &transferLogger,
		"recovery.log":    &recoveryLogger,
	}

	var opened []*os.File

	for name, target := range logTypes {
		file, err := os.OpenFile(
			filepath.Join("logs", name),
			os.O_CREATE|os.O_APPEND|os.O_WRONLY,
			0600,
		)

		if err != nil {
			for _, f := range opened {
				f.Close()
			}
			return err
		}

		opened = append(opened, file)

		*target = log.New(
			file,
			"",
			log.Ldate|log.Ltime|log.Lmicroseconds,
		)
	}

	files = opened
	return nil
}

// Close closes all log files.
func Close() error {
	mu.Lock()
	defer mu.Unlock()

	var firstErr error

	for _, file := range files {
		if err := file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	files = nil

	errorLogger = nil
	apiLogger = nil
	authLogger = nil
	storageLogger = nil
	replicationLogger = nil
	raftLogger = nil
	clusterLogger = nil
	transferLogger = nil
	recoveryLogger = nil

	return firstErr
}

// LogError records application errors.
func LogError(err error) {
	if err == nil {
		return
	}

	mu.Lock()
	defer mu.Unlock()

	if errorLogger != nil {
		errorLogger.Printf("ERROR: %v", err)
	}
}

// Errorf records formatted errors.
func Errorf(format string, args ...any) {
	LogError(fmt.Errorf(format, args...))
}

// APIEvent represents an HTTP request and response.
type APIEvent struct {
	Method     string
	Path       string
	StatusCode int
	Duration   time.Duration
	RequestID  string
}

func LogAPI(event APIEvent) {
	mu.Lock()
	defer mu.Unlock()

	if apiLogger == nil {
		return
	}

	apiLogger.Printf(
		"%s %s %d %s",
		event.Method,
		event.Path,
		event.StatusCode,
		event.Duration,
	)
}

// AuthEvent represents an authentication attempt.
type AuthEvent struct {
	Action     string
	Identifier string
	Success    bool
	StatusCode int
	Reason     string
}

func LogAuth(event AuthEvent) {
	mu.Lock()
	defer mu.Unlock()

	if authLogger == nil {
		return
	}

	result := "FAILED"
	if event.Success {
		result = "SUCCESS"
	}

	authLogger.Printf(
		"ACTION=%q IDENTIFIER=%q RESULT=%s STATUS=%d REASON=%q",
		event.Action,
		event.Identifier,
		result,
		event.StatusCode,
		event.Reason,
	)
}

// LogStorage records chunk and storage operations.
func LogStorage(message string) {
	logMessage(storageLogger, "STORAGE", message)
}

// LogReplication records replica operations.
func LogReplication(message string) {
	logMessage(replicationLogger, "REPLICATION", message)
}

// LogRaft records Raft consensus and leader events.
func LogRaft(message string) {
	logMessage(raftLogger, "RAFT", message)
}

// LogCluster records node and cluster events.
func LogCluster(message string) {
	logMessage(clusterLogger, "CLUSTER", message)
}

// LogRecovery records WAL and crash recovery events.
func LogRecovery(message string) {
	logMessage(recoveryLogger, "RECOVERY", message)
}

func logMessage(target *log.Logger, category string, message string) {
	mu.Lock()
	defer mu.Unlock()

	if target != nil {
		target.Printf("[%s] %s", category, message)
	}
}

// TransferEvent represents an upload or download operation.
type TransferEvent struct {
	Operation  string
	ClientIP   string
	ServerAddr string
	Host       string
	Method     string
	Path       string
	FileID     string
	RequestID  string
	StatusCode int
	Bytes      int64
	Duration   time.Duration
	Message    string
}

// LogTransfer records who initiated a transfer and where it was handled.
func LogTransfer(event TransferEvent) {
	mu.Lock()
	defer mu.Unlock()

	if transferLogger == nil {
		return
	}

	transferLogger.Printf(
		"OPERATION=%q CLIENT_IP=%q SERVER_ADDR=%q HOST=%q METHOD=%q PATH=%q FILE_ID=%q REQUEST_ID=%q STATUS=%d BYTES=%d DURATION=%s MESSAGE=%q",
		event.Operation,
		event.ClientIP,
		event.ServerAddr,
		event.Host,
		event.Method,
		event.Path,
		event.FileID,
		event.RequestID,
		event.StatusCode,
		event.Bytes,
		event.Duration,
		event.Message,
	)
}

// ClientIP extracts the immediate client's IP address.
func ClientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}

	return r.RemoteAddr
}
