package docker

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
)

func GetDockerImageName(sourceType, version string) string {
	if version == "" {
		version = "latest"
	}
	prefix := utils.GetEnv(constants.EnvDockerImagePrefix, constants.DockerImagePrefix)
	return fmt.Sprintf("%s-%s:%s", prefix, sourceType, version)
}

func SetupWorkDirectory(baseDir, subDir string) (string, error) {
	dir := filepath.Join(baseDir, subDir)
	if err := os.MkdirAll(dir, constants.DefaultDirPermissions); err != nil {
		return "", fmt.Errorf("failed to create work directory: %w", err)
	}
	return dir, nil
}

func WriteConfigFiles(workDir string, configs []types.JobConfig) error {
	for _, cfg := range configs {
		path := filepath.Join(workDir, cfg.Name)
		if err := os.WriteFile(path, []byte(cfg.Data), 0o644); err != nil {
			return fmt.Errorf("failed to write %s: %w", cfg.Name, err)
		}
	}
	return nil
}

func DeleteConfigFiles(workDir string, configs []types.JobConfig) {
	for _, cfg := range configs {
		_ = os.Remove(filepath.Join(workDir, cfg.Name))
	}
}
