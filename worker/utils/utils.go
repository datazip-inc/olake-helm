package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"github.com/spf13/viper"
)

// Ternary returns trueValue if condition is true, otherwise returns falseValue
func Ternary(condition bool, trueValue, falseValue interface{}) interface{} {
	if condition {
		return trueValue
	}
	return falseValue
}

// Unmarshal serializes and deserializes any from into the object
func Unmarshal(from, object any) error {
	b, err := json.Marshal(from)
	if err != nil {
		return fmt.Errorf("error marshaling object: %s", err)
	}
	err = json.Unmarshal(b, object)
	if err != nil {
		return fmt.Errorf("error unmarshalling from object: %s", err)
	}

	return nil
}

// RetryWithBackoff retries a function with exponential backoff
func RetryWithBackoff(fn func() error, maxRetries int, initialDelay time.Duration) error {
	delay := initialDelay
	var errMsg error

	for retry := 0; retry < maxRetries; retry++ {
		if err := fn(); err != nil {
			errMsg = err
			if retry < maxRetries-1 {
				logger.Warnf("Retry attempt %d/%d failed: %s. Retrying in %v...", retry+1, maxRetries, err, delay)
				time.Sleep(delay)
				delay *= 2
				continue
			}
		} else {
			return nil
		}
	}
	return fmt.Errorf("failed after %d retries: %s", maxRetries, errMsg)
}

func GetDockerImageName(sourceType, version string) string {
	return fmt.Sprintf("%s-%s:%s", constants.DefaultDockerImagePrefix, sourceType, version)
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

func UpdateConfigWithJobDetails(jobData types.JobData, req *types.ExecutionRequest) {
	updates := map[string]string{
		"source.json":      jobData.Source,
		"destination.json": jobData.Destination,
		"streams.json":     jobData.Streams,
		"state.json":       jobData.State,
	}

	existing := make(map[string]int)
	for i, config := range req.Configs {
		existing[config.Name] = i
	}

	// update the configs with the latest data
	for name, data := range updates {
		if idx, found := existing[name]; found {
			req.Configs[idx].Data = data
		} else {
			req.Configs = append(req.Configs, types.JobConfig{Name: name, Data: data})
		}
	}

	// if user_id not present in the configs, get it from telemetry directory
	if _, exists := existing["user_id.txt"]; !exists {
		req.Configs = append(req.Configs, types.JobConfig{Name: "user_id.txt", Data: GetTelemetryUserID()})
	}
}

// GetWorkflowDirectory determines the directory name based on operation and workflow ID
func GetWorkflowDirectory(operation types.Command, originalWorkflowID string) string {
	if slices.Contains(constants.AsyncCommands, operation) {
		return fmt.Sprintf("%x", sha256.Sum256([]byte(originalWorkflowID)))
	} else {
		return originalWorkflowID
	}
}

func GetStateFileFromWorkdir(workflowID string, command types.Command) (string, error) {
	stateFilePath := filepath.Join(GetConfigDir(), GetWorkflowDirectory(command, workflowID), "state.json")
	stateFile, err := ReadFile(stateFilePath)
	if err != nil {
		return "", fmt.Errorf("failed to read state file: %s", err)
	}
	return stateFile, nil
}

func GetConfigDir() string {
	switch types.ExecutorEnvironment(GetExecutorEnvironment()) {
	case types.Kubernetes:
		return constants.K8sPersistentDir
	case types.Docker:
		return constants.DockerPersistentDir
	default:
		return ""
	}
}

func GetTelemetryUserID() string {
	root := GetConfigDir()
	telemetryPath := filepath.Join(root, "telemetry", "user_id")

	userID, err := os.ReadFile(telemetryPath)
	if err != nil {
		logger.Errorf("failed to read telemetry user ID from file %s: %s", telemetryPath, err)
		newUserID := generateUniqueID()
		logger.Infof("generated new telemetry user ID: %s", newUserID)
		return newUserID
	}
	return string(userID)
}

func generateUniqueID() string {
	hash := sha256.New()
	hash.Write([]byte(time.Now().String()))
	return hex.EncodeToString(hash.Sum(nil))[:32]
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

// WorkflowAlreadyLaunched checked for the folder named log
// inside the provided working directory
//
// workdir/logs - present -> workflow has started already
// workdir/logs - not present -> workflow is running for the first time
func WorkflowAlreadyLaunched(workdir string) bool {
	launchedMarker := filepath.Join(workdir, "logs")
	if _, err := os.Stat(launchedMarker); os.IsNotExist(err) {
		return false
	}
	return true
}

// WorkflowHash returns a deterministic hash string for a given workflowID
func WorkflowHash(workflowID string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(workflowID)))
}

func GetExecutorEnvironment() string {
	if viper.GetString(constants.EnvKubernetesServiceHost) != "" {
		return string(types.Kubernetes)
	}
	return string(types.Docker)
}

// RevertUpdatesInSchedule reverts the updates made to the schedule for clear-destination request
func RevertUpdatesInSchedule(req *types.ExecutionRequest) {
	args := []string{
		"sync",
		"--config", "/mnt/config/source.json",
		"--destination", "/mnt/config/destination.json",
		"--catalog", "/mnt/config/streams.json",
		"--state", "/mnt/config/state.json",
	}

	req.Command = types.Sync
	req.Args = args
}
