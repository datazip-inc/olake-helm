package utils

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
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

// Unmarshal serializes and deserializes any from into the object
// return error if occurred
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

func UpdateConfigWithJobDetails(details types.JobData, req *types.ExecutionRequest) {
	jobDetails := map[string]interface{}{
		"streams":     details.Streams,
		"state":       details.State,
		"source":      details.Source,
		"destination": details.Destination,
	}

	for idx, config := range req.Configs {
		configName := strings.Split(config.Name, ".")[0]
		req.Configs[idx].Data = GetValueOrDefault(jobDetails, configName, config.Data)
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
// workdir/logs - not present -> workflow is running for the furst time
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
