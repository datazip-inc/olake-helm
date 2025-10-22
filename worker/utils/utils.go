package utils

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/executor"
	"github.com/datazip-inc/olake-helm/worker/logger"
	"github.com/datazip-inc/olake-helm/worker/types"
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

func GetDockerImageName(sourceType, version string) string {
	prefix := constants.DefaultDockerImagePrefix
	return fmt.Sprintf("%s-%s:%s", prefix, sourceType, version)
}

func CleanOldLogs(logDir string, retentionPeriod int) {
	logger.Info("running log cleaner...")
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
		logger.Error("failed to read log dir: %s", err)
		return
	}
	// delete dir if old logs are found or is empty
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == "telemetry" {
			continue
		}
		dirPath := filepath.Join(logDir, entry.Name())
		if toDelete := shouldDelete(dirPath, cutoff); toDelete {
			logger.Info("deleting folder: %s", dirPath)
			_ = os.RemoveAll(dirPath)
		}
	}
}

// starts a log cleaner that removes old logs from the specified directory based on the retention period
func InitLogCleaner(logDir string, retentionPeriod int) {
	logger.Info("log cleaner started...")
	CleanOldLogs(logDir, retentionPeriod) // catchup missed cycles if any
	c := cron.New()
	err := c.AddFunc("@midnight", func() {
		CleanOldLogs(logDir, retentionPeriod)
	})
	if err != nil {
		logger.Error("failed to start log cleaner: %s", err)
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

// getHostOutputDir returns the host output directory
func GetHostOutputDir(outputDir string) string {
	hostPersistencePath := viper.GetString(constants.EnvHostPersistentDir)
	persistencePath := GetConfigDir()
	if hostPersistencePath != "" {
		hostOutputDir := strings.Replace(outputDir, persistencePath, hostPersistencePath, 1)
		return hostOutputDir
	}
	return outputDir
}

func UpdateConfigWithJobDetails(details map[string]interface{}, req *executor.ExecutionRequest) {
	for idx, config := range req.Configs {
		configName := strings.Split(config.Name, ".")[0]
		req.Configs[idx].Data = GetValueOrDefault(details, configName, config.Data)
	}
}

// GetWorkflowDirectory determines the directory name based on operation and workflow ID
func GetWorkflowDirectory(operation types.Command, originalWorkflowID string) string {
	if operation == types.Sync {
		return fmt.Sprintf("%x", sha256.Sum256([]byte(originalWorkflowID)))
	} else {
		return originalWorkflowID
	}
}

// WorkflowHash returns a deterministic hash string for a given workflowID
func WorkflowHash(workflowID string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(workflowID)))
}

// GetWorkerEnvVars returns the environment variables from the worker container.
func GetWorkerEnvVars() map[string]string {
	// ignoredWorkerEnv is a map of environment variables that are ignored from the worker container.
	var ignoredWorkerEnv = map[string]any{
		"HOSTNAME":                nil,
		"PATH":                    nil,
		"PWD":                     nil,
		"HOME":                    nil,
		"SHLVL":                   nil,
		"TERM":                    nil,
		"PERSISTENT_DIR":          nil,
		"CONTAINER_REGISTRY_BASE": nil,
		"TEMPORAL_ADDRESS":        nil,
		"OLAKE_SECRET_KEY":        nil,
		"_":                       nil,
	}

	vars := make(map[string]string)
	for _, entry := range os.Environ() {
		parts := strings.SplitN(entry, "=", 2)
		key := parts[0]
		if _, ignore := ignoredWorkerEnv[key]; ignore {
			continue
		}
		vars[key] = parts[1]
	}
	return vars
}
