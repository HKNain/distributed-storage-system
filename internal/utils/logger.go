package logger

import (
	"log"
	"os"
	"path/filepath"
)

var (
	errorLogger *log.Logger
	apiLogger   *log.Logger
)

func Init() error {
	err := os.MkdirAll("logs", 0755)
	if err != nil {
		return err
	}

	errorFile, err := os.OpenFile(
		filepath.Join("logs", "error.log"),
		os.O_CREATE|os.O_APPEND|os.O_WRONLY,
		0644,
	)
	if err != nil {
		return err
	}

	apiFile, err := os.OpenFile(
		filepath.Join("logs", "api.log"),
		os.O_CREATE|os.O_APPEND|os.O_WRONLY,
		0644,
	)
	if err != nil {
		errorFile.Close()
		return err
	}

	errorLogger = log.New(errorFile, "[ERROR] ", log.Ldate|log.Ltime)
	apiLogger = log.New(apiFile, "[API] ", log.Ldate|log.Ltime)

	return nil
}

func Errorf(format string, args ...any) {
	if errorLogger != nil {
		errorLogger.Printf(format, args...)
	}
}

func LogAPI(method, path string, status int, duration string) {
	if apiLogger != nil {
		apiLogger.Printf(
			"%s %s %d %s",
			method,
			path,
			status,
			duration,
		)
	}
}
