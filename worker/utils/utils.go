package utils

import (
	"context"
	"crypto/sha256"
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
				logger.Warnf("retry attempt %d/%d failed: %s. retrying in %v...", retry+1, maxRetries, err, delay)
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

func applyConfigUpdates(req *types.ExecutionRequest, updates map[string]string, addIfMissing map[string]string) {
	existing := make(map[string]int)
	for i, config := range req.Configs {
		existing[config.Name] = i
	}

	for name, data := range updates {
		if idx, found := existing[name]; found {
			req.Configs[idx].Data = data
		} else {
			req.Configs = append(req.Configs, types.JobConfig{Name: name, Data: data})
		}
	}

	for name, data := range addIfMissing {
		if _, found := existing[name]; !found {
			req.Configs = append(req.Configs, types.JobConfig{Name: name, Data: data})
		}
	}
}

func UpdateConfigWithJobDetails(jobData types.JobData, req *types.ExecutionRequest) {
	req.Version = jobData.Version

	updates := map[string]string{
		"source.json":      jobData.Source,
		"destination.json": jobData.Destination,
		"streams.json":     jobData.Streams,
		"state.json":       jobData.State,
	}

	addIfMissing := map[string]string{
		"user_id.txt": GetTelemetryUserID(),
	}

	applyConfigUpdates(req, updates, addIfMissing)
}

func UpdateConfigForClearDestination(jobDetails types.JobData, req *types.ExecutionRequest) error {
	req.Version = jobDetails.Version

	if req.TempPath != "" {
		data, err := os.ReadFile(filepath.Join(GetConfigDir(), req.TempPath))
		if err != nil {
			return fmt.Errorf("failed to read streams file: %s", err)
		}

		updates := map[string]string{
			"destination.json": jobDetails.Destination,
			"state.json":       jobDetails.State,
			"streams.json":     string(data),
		}

		applyConfigUpdates(req, updates, nil)
	}

	return nil
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
		return ""
	}
	return string(userID)
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

func GetWorkflowDirAndSubDir(workflowID string, command types.Command) (string, string) {
	subdir := GetWorkflowDirectory(command, workflowID)
	workdir := filepath.Join(GetConfigDir(), subdir)
	return subdir, workdir
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

// ExtractJSONAndMarshal extracts and returns the last valid JSON block from output
func ExtractJSONAndMarshal(output string) ([]byte, error) {
	outputStr := strings.TrimSpace(output)
	if outputStr == "" {
		return nil, fmt.Errorf("empty output")
	}

	lines := strings.Split(outputStr, "\n")

	// Find the last non-empty line with valid JSON
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			continue
		}

		start := strings.Index(line, "{")
		end := strings.LastIndex(line, "}")
		if start != -1 && end != -1 && end > start {
			jsonPart := line[start : end+1]
			var result map[string]interface{}
			if err := json.Unmarshal([]byte(jsonPart), &result); err != nil {
				continue // Skip invalid JSON
			}
			return json.Marshal(result)
		}
	}

	return nil, fmt.Errorf("no valid JSON block found in output")
}

// PrepareWorkflowLogger ensures the workflow directory exists and initializes the workflow logger.
// It returns the new context with the workflow logger attached, and the log file handle that must be closed when the workflow finishes.
func PrepareWorkflowLogger(ctx context.Context, workflowID string, command types.Command) (context.Context, *logger.WorkflowLogFile, error) {
	_, workdirPath := GetWorkflowDirAndSubDir(workflowID, command)
	workflowLogPath := filepath.Join(workdirPath, "logs")
	if err := SetupWorkDirectory(workflowLogPath); err != nil {
		return ctx, nil, err
	}

	ctxWithLogger, logFile, err := logger.InitWorkflowLogger(ctx, workflowLogPath)
	return ctxWithLogger, logFile, err
}
