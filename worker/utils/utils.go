package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/logger"
	"github.com/robfig/cron"
)

// GetEnv gets the value of an environment variable or returns a default value if the variable is not set
func GetEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// Ternary returns trueValue if condition is true, otherwise returns falseValue
func Ternary(condition bool, trueValue, falseValue interface{}) interface{} {
	if condition {
		return trueValue
	}
	return falseValue
}

// GetValueOrDefault gets the value of a key in a map or returns a default value if the key is not found
func GetValueOrDefault(m map[string]interface{}, key string, defaultValue string) string {
	if value, ok := m[key]; ok {
		return value.(string)
	}
	return defaultValue
}

// GetExecutorEnvironment gets the value of the executor environment variable or returns a default value if the variable is not set
func GetExecutorEnvironment() string {
	return GetEnv(constants.EnvExecutorEnvironment, constants.DefaultExecutorEnvironment)
}

// GetHealthPort gets the value of the health port variable or returns a default value if the variable is not set
func GetHealthPort() int {
	portStr := GetEnv(constants.EnvHealthPort, strconv.Itoa(constants.DefaultHealthPort))
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return constants.DefaultHealthPort
	}
	return port
}

func GetDockerImageName(sourceType, version string) string {
	if version == "" {
		version = "latest"
	}
	prefix := GetEnv(constants.EnvDockerImagePrefix, constants.DockerImagePrefix)
	return fmt.Sprintf("%s-%s:%s", prefix, sourceType, version)
}

func CleanOldLogs(logDir string, retentionPeriod int) {
	logger.Info("Running log cleaner...")
	cutoff := time.Now().AddDate(0, 0, -retentionPeriod)

	// check if old logs are present
	shouldDelete := func(path string, cutoff time.Time) bool {
		entries, _ := os.ReadDir(path)
		if len(entries) == 0 {
			return true
		}

		var foundOldLog bool
		_ = filepath.Walk(path, func(filePath string, info os.FileInfo, _ error) error {
			if info == nil || info.IsDir() {
				return nil
			}
			if (strings.HasSuffix(filePath, ".log") || strings.HasSuffix(filePath, ".log.gz")) &&
				info.ModTime().Before(cutoff) {
				foundOldLog = true
				return filepath.SkipDir
			}
			return nil
		})
		return foundOldLog
	}

	entries, err := os.ReadDir(logDir)
	if err != nil {
		logger.Error("failed to read log dir: %v", err)
		return
	}
	// delete dir if old logs are found or is empty
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == "telemetry" {
			continue
		}
		dirPath := filepath.Join(logDir, entry.Name())
		if toDelete := shouldDelete(dirPath, cutoff); toDelete {
			logger.Info("Deleting folder: %s", dirPath)
			_ = os.RemoveAll(dirPath)
		}
	}
}

// starts a log cleaner that removes old logs from the specified directory based on the retention period
func InitLogCleaner(logDir string, retentionPeriod int) {
	logger.Info("Log cleaner started...")
	CleanOldLogs(logDir, retentionPeriod) // catchup missed cycles if any
	c := cron.New()
	err := c.AddFunc("@midnight", func() {
		CleanOldLogs(logDir, retentionPeriod)
	})
	if err != nil {
		logger.Error("Failed to start log cleaner: %v", err)
		return
	}
	c.Start()
}

// GetRetentionPeriod returns the retention period for logs
func GetLogRetentionPeriod() int {
	if val := GetEnv(constants.EnvLogRetentionPeriod, strconv.Itoa(constants.DefaultLogRetentionPeriod)); val != "" {
		if retentionPeriod, err := strconv.Atoi(val); err == nil && retentionPeriod > 0 {
			return retentionPeriod
		}
	}
	return constants.DefaultLogRetentionPeriod
}

func GetConfigDir() string {
	execEnv := GetExecutorEnvironment()
	if execEnv == "docker" {
		return constants.DefaultConfigDir
	}
	return constants.DefaultK8sConfigDir

}
