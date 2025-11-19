package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
)

func SetupWorkDirectory(workingDir string, subDir string) (string, error) {
	dir := filepath.Join(workingDir, subDir)
	if err := os.MkdirAll(dir, constants.DefaultDirPermissions); err != nil {
		return "", fmt.Errorf("failed to create work directory: %s", err)
	}
	return dir, nil
}

// ReadFile parses a JSON file into a map
func ReadFile(filePath string) (string, error) {
	fileData, err := os.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to read file %s: %s", filePath, err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(fileData, &result); err != nil {
		return "", fmt.Errorf("failed to parse JSON from file %s: %s", filePath, err)
	}

	return string(fileData), nil
}

// CreateDirectory creates a directory with the specified permissions if it doesn't exist
func CreateDirectory(dirPath string) error {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dirPath, constants.DefaultDirPermissions); err != nil {
			return fmt.Errorf("failed to create directory %s: %s", dirPath, err)
		}
	}
	return nil
}

// WriteFile writes data to a file, creating the directory if necessary
func WriteFile(filePath string, data []byte) error {
	dirPath := filepath.Dir(filePath)
	if err := CreateDirectory(dirPath); err != nil {
		return err
	}

	if err := os.WriteFile(filePath, data, constants.DefaultFilePermissions); err != nil {
		return fmt.Errorf("failed to write to file %s: %s", filePath, err)
	}
	return nil
}

func DeleteDirectory(dirPath string) error {
	if err := os.RemoveAll(dirPath); err != nil {
		return fmt.Errorf("failed to delete directory %s: %s", dirPath, err)
	}
	return nil
}

func CleanupConfigFiles(workDir string, configs []types.JobConfig) {
	for _, config := range configs {
		filePath := filepath.Join(workDir, config.Name)
		_ = os.Remove(filePath) // Ignore error for cleanup
	}
}

func WriteConfigFiles(workDir string, configs []types.JobConfig) error {
	for _, config := range configs {
		filePath := filepath.Join(workDir, config.Name)
		if err := os.WriteFile(filePath, []byte(config.Data), constants.DefaultFilePermissions); err != nil {
			return fmt.Errorf("failed to write %s: %s", config.Name, err)
		}
	}
	return nil
}
