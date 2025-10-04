package utils

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/executor"
	"github.com/datazip-inc/olake-helm/worker/logger"
	"github.com/robfig/cron"
	"github.com/spf13/viper"
)

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
	return viper.GetString(constants.EnvExecutorEnvironment)
}

func GetDockerImageName(sourceType, version string) string {
	if version == "" {
		version = "latest"
	}
	prefix := viper.GetString(constants.EnvDockerImagePrefix)
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
	return viper.GetInt(constants.EnvLogRetentionPeriod)
}

func GetConfigDir() string {
	return viper.GetString(constants.EnvContainerPersistentDir)
}

func UpdateConfigWithJobDetails(details map[string]interface{}, req *executor.ExecutionRequest) error {
	for idx, config := range req.Configs {
		configName := strings.Split(config.Name, ".")[0]
		req.Configs[idx].Data = GetValueOrDefault(details, configName, config.Data)
	}
	return nil
}
