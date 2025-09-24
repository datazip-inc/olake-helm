package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/containerd/errdefs"
	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/executor"
	"github.com/datazip-inc/olake-helm/worker/logger"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
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
		if err := os.WriteFile(path, []byte(cfg.Data), constants.DefaultFilePermissions); err != nil {
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

func getHostOutputDir(outputDir string) string {
	if persistentDir := os.Getenv(constants.EnvPersistentDir); persistentDir != "" {
		hostOutputDir := strings.Replace(outputDir, constants.DefaultConfigDir, persistentDir, 1)
		return hostOutputDir
	}
	return outputDir
}

// ReadJSONFile parses a JSON file into a map
func ReadJSONFile(filePath string) (string, error) {
	fileData, err := os.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to read file %s: %v", filePath, err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(fileData, &result); err != nil {
		return "", fmt.Errorf("failed to parse JSON from file %s: %v", filePath, err)
	}

	return string(fileData), nil
}

func (d *DockerExecutor) RunContainer(ctx context.Context, req *executor.ExecutionRequest, workDir string) (string, error) {
	imageName := GetDockerImageName(req.ConnectorType, req.Version)
	logger.Infof("ImageName: %s", imageName)

	if err := d.PullImage(ctx, imageName, req.Version); err != nil {
		return "", err
	}

	containerConfig := &container.Config{
		Image: imageName,
		Cmd:   req.Args,
	}
	hostConfig := &container.HostConfig{}
	if workDir != "" {
		hostOutputDir := getHostOutputDir(workDir)
		hostConfig.Mounts = []mount.Mount{
			{Type: mount.TypeBind, Source: hostOutputDir, Target: constants.ContainerMountDir},
		}
	}

	resp, err := d.client.ContainerCreate(ctx, containerConfig, hostConfig, nil, nil, "")
	if err != nil {
		return "", fmt.Errorf("container create: %s", err)
	}
	defer d.client.ContainerRemove(ctx, resp.ID, container.RemoveOptions{Force: true})

	if err := d.client.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return "", fmt.Errorf("container start: %s", err)
	}

	// wait until the container stops
	statusCh, errCh := d.client.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			return "", fmt.Errorf("wait error: %s", err)
		}
	case <-statusCh:
	}

	// fetch logs after completion
	logReader, err := d.client.ContainerLogs(ctx, resp.ID, container.LogsOptions{ShowStdout: true, ShowStderr: true})
	if err != nil {
		return "", fmt.Errorf("container logs: %s", err)
	}
	defer logReader.Close()

	b, _ := io.ReadAll(logReader)
	return string(b), nil
}

func (d *DockerExecutor) PullImage(ctx context.Context, imageName, version string) error {
	// Always pull if version is "latest"
	if strings.EqualFold(version, "latest") {
		logger.Infof("Pulling latest image: %s", imageName)
		rc, err := d.client.ImagePull(ctx, imageName, image.PullOptions{})
		if err != nil {
			return fmt.Errorf("image pull %s: %s", imageName, err)
		}
		if rc != nil {
			io.Copy(io.Discard, rc)
			rc.Close()
		}
		return nil
	}

	// For other versions, pull only if not exists locally
	_, err := d.client.ImageInspect(ctx, imageName)
	if errdefs.IsNotFound(err) {
		logger.Infof("Image not found locally, pulling: %s", imageName)
		rc, err := d.client.ImagePull(ctx, imageName, image.PullOptions{})
		if err != nil {
			return fmt.Errorf("image pull %s: %s", imageName, err)
		}
		if rc != nil {
			io.Copy(io.Discard, rc)
			rc.Close()
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("image inspect %s: %s", imageName, err)
	}

	logger.Infof("Using local image: %s", imageName)
	return nil
}
