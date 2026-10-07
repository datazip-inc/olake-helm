package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
)

// WriteFiles writes configs under workDir (a path under ConfigDir) in the active storage mode.
func WriteFiles(ctx context.Context, workDir string, configs []types.JobConfig) error {
	if len(configs) == 0 {
		return nil
	}

	if Mode() == constants.StorageModeS3 {
		return writeFilesS3(ctx, workDir, configs)
	}

	for _, jobConfig := range configs {
		filePath := filepath.Join(workDir, jobConfig.Name)
		if err := os.MkdirAll(filepath.Dir(filePath), constants.DefaultDirPermissions); err != nil {
			return fmt.Errorf("failed to write %s: failed to create directory %s: %s", jobConfig.Name, filepath.Dir(filePath), err)
		}
		if err := os.WriteFile(filePath, []byte(jobConfig.Data), constants.DefaultFilePermissions); err != nil {
			return fmt.Errorf("failed to write %s: failed to write to file %s: %s", jobConfig.Name, filePath, err)
		}
	}
	return nil
}

// ReadFile reads relativePath under workDir (a path under ConfigDir) in the active storage mode.
// A missing file returns an error that matches fs.ErrNotExist in both modes. validateJSON also
// requires the content to parse as a JSON object.
func ReadFile(ctx context.Context, workDir, relativePath string, validateJSON bool) (string, error) {
	if Mode() == constants.StorageModeS3 {
		return readFileS3(ctx, workDir, relativePath, validateJSON)
	}

	filePath := filepath.Join(workDir, relativePath)
	data, err := os.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to read file %s: %w", filePath, err)
	}
	if validateJSON {
		var result map[string]interface{}
		if err := json.Unmarshal(data, &result); err != nil {
			return "", fmt.Errorf("failed to parse JSON from file %s: %s", filePath, err)
		}
	}
	return string(data), nil
}
