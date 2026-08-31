package utils

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"github.com/datazip-inc/olake-helm/worker/utils/storagemode"
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
	registryBase := strings.TrimRight(viper.GetString(constants.ContainerRegistryBase), "/")
	imageName := fmt.Sprintf("%s-%s:%s", constants.DefaultDockerImagePrefix, sourceType, version)

	if registryBase == "" || registryBase == "registry-1.docker.io" {
		return imageName
	}

	return fmt.Sprintf("%s/%s", registryBase, imageName)
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
		"TEMPORAL_API_KEY":        nil,
		"TEMPORAL_EXTERNAL":       nil,
		"TEMPORAL_ENABLE_TLS":     nil,
		"TEMPORAL_NAMESPACE":      nil,
		"TEMPORAL_TASK_QUEUE":     nil,
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

// ApplyConfigUpdates overwrites req.Configs entries named in updates, and adds addIfMissing entries only if not already present.
func ApplyConfigUpdates(req *types.ExecutionRequest, updates map[string]string, addIfMissing map[string]string) {
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

func UpdateConfigWithJobDetails(ctx context.Context, jobData types.JobData, req *types.ExecutionRequest) {
	req.Version = jobData.Version

	updates := map[string]string{
		"source.json":      jobData.Source,
		"destination.json": jobData.Destination,
		"streams.json":     jobData.Streams,
		"state.json":       jobData.State,
	}

	ApplyConfigUpdates(req, updates, nil)
}

func UpdateConfigForClearDestination(ctx context.Context, jobDetails types.JobData, req *types.ExecutionRequest) error {
	req.Version = jobDetails.Version

	if req.TempPath != "" {
		var data string
		var err error
		switch storagemode.Get() {
		case constants.StorageModeS3:
			data, err = ReadFileFromS3(ctx, "", req.TempPath, true)
		case constants.StorageModeNFS:
			data, err = ReadFileFromNFS(GetConfigDir(), req.TempPath)
		default:
			return fmt.Errorf("unsupported storage mode: %s", storagemode.Get())
		}
		if err != nil {
			return fmt.Errorf("failed to read streams file: %s", err)
		}

		updates := map[string]string{
			"destination.json": jobDetails.Destination,
			"state.json":       jobDetails.State,
			"streams.json":     data,
		}

		ApplyConfigUpdates(req, updates, nil)
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

func GetStateFileFromWorkdir(ctx context.Context, workflowID string, command types.Command) (string, error) {
	_, workDir := GetWorkflowDirAndSubDir(workflowID, command)

	var stateFile string
	var err error
	switch storagemode.Get() {
	case constants.StorageModeS3:
		stateFile, err = ReadFileFromS3(ctx, workDir, "state.json", true)
	case constants.StorageModeNFS:
		stateFile, err = ReadFileFromNFS(workDir, "state.json")
	default:
		return "", fmt.Errorf("unsupported storage mode: %s", storagemode.Get())
	}
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

// GetTelemetryUserID reads the telemetry user ID from the appropriate storage mode.
func GetTelemetryUserID(ctx context.Context) string {
	switch storagemode.Get() {
	case constants.StorageModeS3:
		data, err := ReadFileFromS3(ctx, "", constants.TelemetryUserIDPath, false)
		if err != nil {
			logger.Errorf("failed to read telemetry user ID: %s", err)
			return ""
		}
		return data
	case constants.StorageModeNFS:
		telemetryPath := filepath.Join(GetConfigDir(), constants.TelemetryUserIDPath)

		userID, err := os.ReadFile(telemetryPath)
		if err != nil {
			logger.Errorf("failed to read telemetry user ID from file %s: %s", telemetryPath, err)
			return ""
		}
		return string(userID)
	default:
		logger.Errorf("unsupported storage mode for telemetry user ID: %s", storagemode.Get())
		return ""
	}
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

// WorkflowAlreadyLaunched reports whether this workflow has already started a connector run.
// Config files alone do not count — they are written before the container/pod is launched.
func WorkflowAlreadyLaunched(ctx context.Context, workdir string) bool {
	switch storagemode.Get() {
	case constants.StorageModeS3:
		return workflowConnectorLogsExistInS3(ctx, workdir)
	case constants.StorageModeNFS:
		logDir := filepath.Join(workdir, "logs")
		entries, err := os.ReadDir(logDir)
		if err != nil {
			return false
		}

		for _, entry := range entries {
			if entry.IsDir() {
				olakeLogPath := filepath.Join(logDir, entry.Name(), "olake.log")
				if _, err := os.Stat(olakeLogPath); err == nil {
					return true
				}
			}
		}
		return false
	default:
		return false
	}
}

// WorkflowHash returns a deterministic hash string for a given workflowID
func WorkflowHash(workflowID string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(workflowID)))
}

// SyncWorkflowAndScheduleID returns a job's base sync workflow ID and its
// schedule ID
func SyncWorkflowAndScheduleID(projectID string, jobID int) (string, string) {
	workflowID := fmt.Sprintf("sync-%s-%d", projectID, jobID)
	return workflowID, fmt.Sprintf("schedule-%s", workflowID)
}

// GetTemporalNamespace returns the configured namespace when TEMPORAL_EXTERNAL is true,
// otherwise returns the default namespace.
func GetTemporalNamespace() string {
	if viper.GetBool(constants.EnvTemporalExternal) {
		if ns := viper.GetString(constants.EnvTemporalNamespace); ns != "" {
			return ns
		}
	}
	return constants.DefaultTemporalNamespace
}

// GetTemporalTaskQueue returns the configured task queue when TEMPORAL_EXTERNAL is true,
// otherwise returns the default task queue.
func GetTemporalTaskQueue() string {
	if viper.GetBool(constants.EnvTemporalExternal) {
		if queue := viper.GetString(constants.EnvTemporalTaskQueue); queue != "" {
			return queue
		}
	}
	return constants.TaskQueue
}

func IsTemporalCloud() bool {
	return viper.GetBool(constants.EnvTemporalExternal) && viper.GetString(constants.EnvTemporalAPIKey) != ""
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

// connectorConfigPath returns the path the connector binary should read for a config file.
// NFS mounts the workflow dir at /mnt/config; S3 uses s3://bucket/[prefix/]{workflow-dir}/file.
func connectorConfigPath(command types.Command, workflowID, filename string) string {
	switch storagemode.Get() {
	case constants.StorageModeS3:
		bucket := strings.TrimSpace(viper.GetString(constants.EnvS3Bucket))
		jobDir := GetWorkflowDirectory(command, workflowID)
		key := path.Join(jobDir, filename)
		if prefix := strings.Trim(viper.GetString(constants.EnvS3Prefix), "/"); prefix != "" {
			key = path.Join(prefix, key)
		}
		return fmt.Sprintf("s3://%s/%s", bucket, key)
	case constants.StorageModeNFS:
		// Workflow dir is mounted at /mnt/config (K8s subPath or Docker bind mount).
		return path.Join(constants.ContainerMountDir, filename)
	default:
		return path.Join(constants.ContainerMountDir, filename)
	}
}

// RefreshConnectorArgs rebuilds CLI args from the execution WorkflowID.
// Schedule metadata is baked with the stable schedule ID (e.g. sync-123-1), but Temporal
// runs each fire under a unique ID (sync-123-1-<timestamp>). Configs are written under the
// execution ID hash, so Args must match that path — not the schedule-time hash.
// When revertToSync is true, Command is reset to Sync first (used after clear-destination).
func RefreshConnectorArgs(req *types.ExecutionRequest, revertToSync bool) {
	if req == nil || req.WorkflowID == "" {
		return
	}
	if revertToSync {
		req.Command = types.Sync
	}

	switch req.Command {
	case types.Sync:
		req.Args = []string{
			"sync",
			"--config", connectorConfigPath(types.Sync, req.WorkflowID, "source.json"),
			"--destination", connectorConfigPath(types.Sync, req.WorkflowID, "destination.json"),
			"--catalog", connectorConfigPath(types.Sync, req.WorkflowID, "streams.json"),
			"--state", connectorConfigPath(types.Sync, req.WorkflowID, "state.json"),
		}
	case types.ClearDestination:
		req.Args = []string{
			"clear-destination",
			"--streams", connectorConfigPath(types.ClearDestination, req.WorkflowID, "streams.json"),
			"--state", connectorConfigPath(types.ClearDestination, req.WorkflowID, "state.json"),
			"--destination", connectorConfigPath(types.ClearDestination, req.WorkflowID, "destination.json"),
		}
	}
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
			return json.Marshal(unwrapZerologProtocolMessage(result))
		}
	}

	return nil, fmt.Errorf("no valid JSON block found in output")
}

// unwrapZerologProtocolMessage returns the inner OLake protocol object when stdout is
// S3-mode zerolog JSON: {"level":"info","message":{"type":"CONNECTION_STATUS",...}}.
// NFS console output already yields the inner object, so it is returned unchanged.
func unwrapZerologProtocolMessage(result map[string]interface{}) map[string]interface{} {
	if _, ok := result["level"].(string); !ok {
		return result
	}
	message, ok := result["message"].(map[string]interface{})
	if !ok || message == nil {
		return result
	}
	return message
}

// IsStateEmpty returns true if the state is empty or an empty JSON object
func IsStateEmpty(state string) bool {
	state = strings.TrimSpace(state)
	return state == "" || state == "{}"
}

// RemoveFlagFromArgs returns a new slice with the given flag
// and its associated value removed.
func RemoveFlagFromArgs(arguments []string, flagName string) []string {
	result := make([]string, 0, len(arguments))

	for idx := 0; idx < len(arguments); idx++ {
		if arguments[idx] == flagName {
			idx++ // skip the value
			continue
		}
		result = append(result, arguments[idx])
	}

	return result
}

// PrepareWorkflowLogger ensures the workflow directory exists and initializes the workflow logger.
// It returns the new context with the workflow logger attached, and the log file handle that must be closed when the workflow finishes.
func PrepareWorkflowLogger(ctx context.Context, workflowID string, command types.Command, newWorkerLogCollector func(ctx context.Context, workflowID, workDir string) (*RuntimeLogCollector, error), newConnectorLogCollector func(ctx context.Context, workflowID, workDir string, command types.Command) (*RuntimeLogCollector, error)) (context.Context, *logger.WorkflowLogFile, error) {
	_, workdirPath := GetWorkflowDirAndSubDir(workflowID, command)
	workflowLogPath := filepath.Join(workdirPath, "logs")
	if err := SetupWorkDirectory(workflowLogPath); err != nil {
		return ctx, nil, err
	}

	switch storagemode.Get() {
	case constants.StorageModeS3:
		releaseCollectors, err := acquireWorkflowLogCollectors(ctx, workflowID, workdirPath, command, newWorkerLogCollector, newConnectorLogCollector)
		if err != nil {
			return ctx, nil, err
		}

		return logger.InitWorkflowLoggerForS3(ctx, workflowID, string(command), io.Discard, func() error {
			return releaseCollectors(ctx)
		})
	case constants.StorageModeNFS:
		return logger.InitWorkflowLoggerForNFS(ctx, workflowLogPath)
	default:
		return ctx, nil, nil
	}
}
