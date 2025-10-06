package helpers

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// parseTimeout parses a timeout from viper with fallback
func parseTimeout(envKey string, defaultValue time.Duration) time.Duration {
	timeoutStr := viper.GetString(envKey)
	if timeoutStr == "" {
		return defaultValue
	}

	if seconds, err := strconv.Atoi(timeoutStr); err == nil {
		return time.Duration(seconds) * time.Second
	}

	if duration, err := time.ParseDuration(timeoutStr); err == nil {
		return duration
	}

	return defaultValue
}

// GetActivityTimeout reads activity timeout from viper configuration
func GetActivityTimeout(operation string) time.Duration {
	switch operation {
	case "discover":
		return parseTimeout("timeouts.activity.discover", 2*time.Hour)
	case "test":
		return parseTimeout("timeouts.activity.test", 2*time.Hour)
	case "sync":
		return parseTimeout("timeouts.activity.sync", 700*time.Hour)
	case "spec":
		return parseTimeout("timeouts.activity.spec", 5*time.Minute)
	default:
		return 30 * time.Minute
	}
}

// WaitForTelemetryID blocks until the telemetry identifier file can be read.
func WaitForTelemetryID(path string) string {
	for {
		if data, err := os.ReadFile(path); err == nil {
			if id := strings.TrimSpace(string(data)); id != "" {
				return id
			}
		}
		time.Sleep(10 * time.Second)
	}
}
