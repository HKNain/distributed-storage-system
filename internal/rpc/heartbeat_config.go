package rpc

import (
	"log"
	"os"
	"strconv"
	"time"
)

const (
	defaultHeartbeatInterval = 2 * time.Second
	defaultFailureThreshold  = 3
	defaultMonitorInterval   = 1 * time.Second
)

func HeartbeatIntervalFromEnv() time.Duration {
	value := os.Getenv("HEARTBEAT_INTERVAL")
	if value == "" {
		return defaultHeartbeatInterval
	}

	interval, err := time.ParseDuration(value)
	if err != nil || interval <= 0 {
		log.Printf(
			"Invalid HEARTBEAT_INTERVAL %q; using default %s",
			value,
			defaultHeartbeatInterval,
		)
		return defaultHeartbeatInterval
	}

	return interval
}

func FailureThresholdFromEnv() int {
	value := os.Getenv("HEARTBEAT_FAILURE_THRESHOLD")
	if value == "" {
		return defaultFailureThreshold
	}

	threshold, err := strconv.Atoi(value)
	if err != nil || threshold <= 0 {
		log.Printf(
			"Invalid HEARTBEAT_FAILURE_THRESHOLD %q; using default %d",
			value,
			defaultFailureThreshold,
		)
		return defaultFailureThreshold
	}

	return threshold
}

func MonitorIntervalFromEnv() time.Duration {
	value := os.Getenv("MONITOR_INTERVAL")
	if value == "" {
		return defaultMonitorInterval
	}

	interval, err := time.ParseDuration(value)
	if err != nil || interval <= 0 {
		log.Printf(
			"Invalid MONITOR_INTERVAL %q; using default %s",
			value,
			defaultMonitorInterval,
		)
		return defaultMonitorInterval
	}

	return interval
}