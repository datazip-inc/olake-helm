package utils

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/acarl005/stripansi"
	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"github.com/spf13/viper"
	"golang.org/x/mod/semver"
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

func UpdateConfigWithJobDetails(jobData types.JobData, req *types.ExecutionRequest) {
	req.Version = jobData.Version

	updates := map[string]string{
		"source.json":      jobData.Source,
		"destination.json": jobData.Destination,
		"state.json":       jobData.State,
	}

	// the job's catalog format decides the flags
	setCatalog(req, updates, jobData.Streams, jobData.AvailableStreams, jobData.SelectedStreams)

	ApplyConfigUpdates(req, updates, nil)
}

func UpdateConfigForClearDestination(ctx context.Context, jobDetails types.JobData, req *types.ExecutionRequest) error {
	req.Version = jobDetails.Version

	if req.TempPath == "" {
		return nil
	}

	// olake-ui stages the catalog in the format the clear-destination runs with: streams.json, or
	// available_streams.json + selected_streams.json, in the temp path's directory
	streams, available, selected, err := readStagedCatalog(ctx, filepath.Dir(req.TempPath))
	if err != nil {
		return err
	}

	updates := map[string]string{
		"destination.json": jobDetails.Destination,
		"state.json":       jobDetails.State,
	}
	setCatalog(req, updates, streams, available, selected)

	ApplyConfigUpdates(req, updates, nil)
	return nil
}

// readStagedCatalog reads the catalog staged in dir (relative to the config dir / S3 prefix):
// streams.json, or available_streams.json + selected_streams.json. A missing file reads as empty;
// nothing staged at all is an error.
func readStagedCatalog(ctx context.Context, dir string) (streams, available, selected string, err error) {
	read := func(name string) (string, error) {
		data, err := storage.ReadFile(ctx, storage.ConfigDir(), filepath.Join(dir, name), true)
		if errors.Is(err, fs.ErrNotExist) {
			return "", nil
		}
		if err != nil {
			return "", fmt.Errorf("failed to read %s: %s", name, err)
		}
		return data, nil
	}

	if streams, err = read(constants.StreamsFile); err != nil {
		return "", "", "", err
	}
	if available, err = read(constants.AvailableStreamsFile); err != nil {
		return "", "", "", err
	}
	if selected, err = read(constants.SelectedStreamsFile); err != nil {
		return "", "", "", err
	}
	if streams == "" && available == "" && selected == "" {
		return "", "", "", fmt.Errorf("no catalog staged in %s", dir)
	}
	return streams, available, selected, nil
}

// setCatalog adds the catalog files to updates and points req.Args at them. olake-ui guarantees
// exactly one format: a split catalog (available + selected) or a legacy streams catalog.
func setCatalog(req *types.ExecutionRequest, updates map[string]string, streams, available, selected string) {
	split := available != "" && selected != ""
	if split {
		updates[constants.AvailableStreamsFile] = available
		updates[constants.SelectedStreamsFile] = selected
	} else {
		updates[constants.StreamsFile] = streams
	}
	req.Args = SetCatalogArgs(req.Args, split)
}

// SetCatalogArgs replaces whatever catalog flags args carries with the ones for the given format.
func SetCatalogArgs(args []string, split bool) []string {
	for _, flag := range []string{constants.CatalogFlag, constants.StreamsFlag, constants.AvailableStreamsFlag, constants.SelectedStreamsFlag} {
		args = RemoveFlagFromArgs(args, flag)
	}
	if split {
		return append(args,
			constants.AvailableStreamsFlag, filepath.Join(constants.ContainerMountDir, constants.AvailableStreamsFile),
			constants.SelectedStreamsFlag, filepath.Join(constants.ContainerMountDir, constants.SelectedStreamsFile),
		)
	}
	return append(args, constants.CatalogFlag, filepath.Join(constants.ContainerMountDir, constants.StreamsFile))
}

// GetWorkflowDirectory determines the directory name based on operation and workflow ID
func GetWorkflowDirectory(operation types.Command, originalWorkflowID string) string {
	if IsAsyncCommand(operation) {
		return fmt.Sprintf("%x", sha256.Sum256([]byte(originalWorkflowID)))
	} else {
		return originalWorkflowID
	}
}

func GetStateFileFromWorkdir(ctx context.Context, workflowID string, command types.Command) (string, error) {
	_, workDir := GetWorkflowDirAndSubDir(workflowID, command)

	stateFile, err := storage.ReadFile(ctx, workDir, "state.json", true)
	if err != nil {
		return "", fmt.Errorf("failed to read state file: %s", err)
	}
	return stateFile, nil
}

// GetTelemetryUserID reads the telemetry user ID from the active storage mode.
func GetTelemetryUserID(ctx context.Context) string {
	userID, err := storage.ReadFile(ctx, storage.ConfigDir(), constants.TelemetryUserIDPath, false)
	if err != nil {
		logger.Errorf("failed to read telemetry user ID: %s", err)
		return ""
	}
	return userID
}

// getHostOutputDir returns the host output directory
func GetHostOutputDir(outputDir string) string {
	hostPersistencePath := viper.GetString(constants.EnvHostPersistentDir)
	if hostPersistencePath != "" {
		persistencePath := storage.ConfigDir()
		hostOutputDir := strings.Replace(outputDir, persistencePath, hostPersistencePath, 1)
		return hostOutputDir
	}

	return outputDir
}

// s3 mode only supports connector versions that are at least the minimum version "v0.9.2"
func ValidateConnectorVersionForStorageMode(version string) error {
	if storage.Mode() != constants.StorageModeS3 {
		return nil
	}
	if !semver.IsValid(version) {
		return nil
	}
	if semver.Compare(version, constants.MinS3StorageModeVersion) < 0 {
		return fmt.Errorf("connector version %s does not support S3 storage mode: requires %s or later", version, constants.MinS3StorageModeVersion)
	}
	return nil
}

// WorkflowAlreadyLaunched reports whether this workflow has already started a connector run.
// Config files alone do not count — they are written before the container/pod is launched.
func WorkflowAlreadyLaunched(ctx context.Context, workdir string) (bool, error) {
	switch storage.Mode() {
	case constants.StorageModeS3:
		alreadyLaunched, err := workflowConnectorLogsExistInS3(ctx, workdir)
		if err != nil {
			return false, err
		}
		return alreadyLaunched, nil
	default:
		logDir := filepath.Join(workdir, "logs")
		// Comment: Check how functions error handling works here and can be improved.
		entries, err := os.ReadDir(logDir)
		if err != nil {
			return false, nil
		}

		for _, entry := range entries {
			if entry.IsDir() {
				olakeLogPath := filepath.Join(logDir, entry.Name(), "olake.log")
				if _, err := os.Stat(olakeLogPath); err == nil {
					return true, nil
				}
			}
		}
		return false, nil
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
	workdir := filepath.Join(storage.ConfigDir(), subdir)
	return subdir, workdir
}

// ConnectorConfigDir returns the S3 config path for S3 storage mode, empty string otherwise.
func ConnectorConfigDir(command types.Command, workflowID string) string {
	if storage.Mode() != constants.StorageModeS3 {
		return ""
	}
	bucket := strings.TrimSpace(viper.GetString(constants.EnvS3Bucket))
	key := GetWorkflowDirectory(command, workflowID)
	if prefix := strings.Trim(viper.GetString(constants.EnvS3Prefix), "/"); prefix != "" {
		key = path.Join(prefix, key)
	}
	return fmt.Sprintf("s3://%s/%s", bucket, key)
}

// RevertUpdatesInSchedule reverts the updates made to the schedule for clear-destination request.
// The catalog flags only keep the stored args well-formed: SyncActivity resets them from the
// job's catalog format on every run.
func RevertUpdatesInSchedule(req *types.ExecutionRequest) {
	split := slices.Contains(req.Args, constants.AvailableStreamsFlag)
	args := []string{
		"sync",
		"--config", "/mnt/config/source.json",
		"--destination", "/mnt/config/destination.json",
		"--state", "/mnt/config/state.json",
	}

	req.Command = types.Sync
	req.Args = SetCatalogArgs(args, split)
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
			// NFS console output: a debug line can carry JSON in its text (e.g. a telemetry
			// error response body) and must not be mistaken for the protocol message.
			if strings.Contains(stripansi.Strip(line[:start]), "DEBUG") {
				continue
			}
			jsonPart := line[start : end+1]
			var result map[string]interface{}
			if err := json.Unmarshal([]byte(jsonPart), &result); err != nil {
				continue // Skip invalid JSON
			}
			message, ok := unwrapZerologProtocolMessage(result)
			if !ok {
				continue // S3-mode plain log line (string message), not the protocol message
			}
			return json.Marshal(message)
		}
	}

	return nil, fmt.Errorf("no valid JSON block found in output")
}

// unwrapZerologProtocolMessage returns the inner OLake protocol object when stdout is
// S3-mode zerolog JSON: {"level":"info","message":{"type":"CONNECTION_STATUS",...}}.
// It reports false for a zerolog line whose message is not an object (a plain log line,
// e.g. a telemetry debug message printed after the result). NFS console output already
// yields the inner object, so it is returned unchanged.
func unwrapZerologProtocolMessage(result map[string]interface{}) (map[string]interface{}, bool) {
	if _, ok := result["level"].(string); !ok {
		return result, true // not a zerolog line (NFS output): use it as is
	}
	message, ok := result["message"].(map[string]interface{})
	if !ok || message == nil {
		return nil, false // zerolog line with a text message: not the result, skip
	}
	return message, true // zerolog line with an object message: that object is the result
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

// PrepareWorkflowLogger attaches a workflow logger to ctx. In S3 mode worker logs are written
// directly to S3 chunks and the returned handle is nil. In NFS mode it creates logs/ and
// opens worker.log; close that handle when the activity finishes.
func PrepareWorkflowLogger(ctx context.Context, workflowID string, command types.Command) (context.Context, *logger.WorkflowLogFile, error) {
	_, workdirPath := GetWorkflowDirAndSubDir(workflowID, command)

	switch storage.Mode() {
	case constants.StorageModeS3:
		workerWriter, err := acquireWorkerLogWriter(ctx, workdirPath)
		if err != nil {
			return ctx, nil, err
		}

		ctxWithLogger, err := logger.InitWorkflowLoggerForS3(ctx, workflowID, string(command), workerWriter, workerWriter.nextSeq)
		return ctxWithLogger, nil, err
	default:
		workflowLogPath := filepath.Join(workdirPath, "logs")
		if err := SetupWorkDirectory(workflowLogPath); err != nil {
			return ctx, nil, err
		}
		return logger.InitWorkflowLoggerForNFS(ctx, workflowLogPath)
	}
}

// IsAsyncCommand returns true if the command is an asynchronous command(sync, clear-destination)
func IsAsyncCommand(command types.Command) bool {
	return slices.Contains(constants.AsyncCommands, command)
}

// workflowConnectorLogsExistInS3 mirrors the NFS check for logs/sync_*/olake.log:
// true only when connector log chunks have been uploaded for this workflow.
// Worker retries before the first chunk is uploaded still look like a first launch.
// List/path errors are unknown, not "never launched".
func workflowConnectorLogsExistInS3(ctx context.Context, workDir string) (bool, error) {
	logsPath, err := storage.S3Key(workDir, "logs", true)
	if err != nil {
		return false, fmt.Errorf("failed to resolve logs path: %s", err)
	}

	s3Objects, err := storage.ListS3Objects(ctx, logsPath)
	if err != nil {
		return false, fmt.Errorf("failed to list objects in %s: %s", logsPath, err)
	}

	for _, s3object := range s3Objects {
		parts := strings.Split(strings.TrimPrefix(s3object.Key, logsPath), "/")
		if len(parts) != 2 {
			continue
		}
		if strings.HasPrefix(parts[0], constants.ConnectorLogDirPrefix) && strings.HasPrefix(parts[1], constants.PodLogFilenamePref) {
			return true, nil
		}
	}
	return false, nil
}
