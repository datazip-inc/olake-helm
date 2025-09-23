package docker

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/executor"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
)

type DockerExecutor struct {
	client     *client.Client
	WorkingDir string
}

func NewDockerExecutor() (*DockerExecutor, error) {
	client, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("failed to create docker client: %s", err)
	}
	return &DockerExecutor{client: client}, nil
}

func (d *DockerExecutor) Execute(ctx context.Context, req *executor.ExecutionRequest) (map[string]interface{}, error) {
	// Prepare workdir (use workflowID as subdir; client sends fully-formed Args)
	var workDir string
	var err error
	if len(req.Configs) > 0 {
		baseDir := utils.GetEnv(constants.EnvPersistentDir, constants.DefaultConfigDir)
		workDir, err = SetupWorkDirectory(baseDir, req.WorkflowID)
		if err != nil {
			return nil, err
		}

		// Write provided config files
		if err := WriteConfigFiles(workDir, req.Configs); err != nil {
			return nil, err
		}
		defer DeleteConfigFiles(workDir, req.Configs)
	}

	// Resolve image name and optionally pull (latest)
	imageName := GetDockerImageName(req.ConnectorType, req.Version)
	if strings.EqualFold(req.Version, "latest") {
		rc, err := d.client.ImagePull(ctx, imageName, image.PullOptions{})
		if err != nil {
			return nil, fmt.Errorf("image pull %s: %s", imageName, err)
		}
		if rc != nil {
			io.Copy(io.Discard, rc)
			rc.Close()
		}
	}

	// Run container with provided args; mount workDir -> /mnt/config
	containerConfig := &container.Config{
		Image: imageName,
		Cmd:   req.Args, // client-crafted args (must reference /mnt/config/...)
	}
	hostConfig := &container.HostConfig{
		AutoRemove: true,
	}
	if workDir != "" {
		hostConfig.Mounts = []mount.Mount{
			{Type: mount.TypeBind, Source: workDir, Target: "/mnt/config"},
		}
	}

	resp, err := d.client.ContainerCreate(ctx, containerConfig, hostConfig, nil, nil, "")
	if err != nil {
		return nil, fmt.Errorf("container create: %s", err)
	}
	if err := d.client.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return nil, fmt.Errorf("container start: %s", err)
	}

	// Wait and return raw logs (no parsing)
	statusCh, errCh := d.client.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			return nil, fmt.Errorf("wait error: %s", err)
		}
	case <-statusCh:
	}

	logReader, err := d.client.ContainerLogs(ctx, resp.ID, container.LogsOptions{ShowStdout: true, ShowStderr: true})
	if err != nil {
		return nil, fmt.Errorf("container logs: %s", err)
	}
	defer logReader.Close()

	b, _ := io.ReadAll(logReader)
	return map[string]interface{}{
		"response": string(b),
	}, nil
}

func (d *DockerExecutor) Close() error {
	return d.client.Close()
}

func init() {
	executor.RegisteredExecutors[executor.Docker] = func() (executor.Executor, error) {
		return NewDockerExecutor()
	}
}
