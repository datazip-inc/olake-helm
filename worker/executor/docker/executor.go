package docker

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/executor"
	environment "github.com/datazip-inc/olake-helm/worker/executor/enviroment"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
)

type DockerExecutor struct {
	client     *client.Client
	workingDir string
}

func NewDockerExecutor() (*DockerExecutor, error) {
	client, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("failed to create docker client: %s", err)
	}

	return &DockerExecutor{client: client, workingDir: utils.GetConfigDir()}, nil
}

func (d *DockerExecutor) Execute(ctx context.Context, req *types.ExecutionRequest, workdir string) (string, error) {
	imageName := utils.GetDockerImageName(req.ConnectorType, req.Version)
	containerName := utils.GetWorkflowDirectory(req.Command, req.WorkflowID)
	logger.Infof("running container - command: %s, image: %s, name: %s", req.Command, imageName, containerName)

	if req.Command == types.Sync {
		startSync, err := d.shouldStartSync(ctx, req, containerName, workdir)
		if err != nil {
			return "", err
		}
		if !startSync.OK {
			return startSync.Message, nil
		}
	}
	
	if err := d.PullImage(ctx, imageName, req.Version); err != nil {
		return "", err
	}

	// Environment variables propagation
	var envs []string
	for k, v := range utils.GetWorkerEnvVars() {
		envs = append(envs, fmt.Sprintf("%s=%s", k, v))
	}

	containerConfig := &container.Config{
		Image: imageName,
		Cmd:   req.Args,
		Env:   envs,
	}

	hostConfig := &container.HostConfig{}
	if workdir != "" {
		hostOutputDir := utils.GetHostOutputDir(workdir)
		hostConfig.Mounts = []mount.Mount{
			{Type: mount.TypeBind, Source: hostOutputDir, Target: constants.ContainerMountDir},
		}
	}

	logger.Infof("running Docker container with image: %s, name: %s, command: %v", imageName, containerName, req.Args)

	containerID, err := d.getOrCreateContainer(ctx, containerConfig, hostConfig, containerName)
	if err != nil {
		return "", err
	}
	if req.Command != types.Sync {
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second*constants.ContainerCleanupTimeout)
			defer cancel()

			if err := d.client.ContainerRemove(cleanupCtx, containerID, container.RemoveOptions{Force: true}); err != nil {
				logger.Warnf("failed to remove container: %s", err)
			}
		}()
	}

	if err := d.startContainer(ctx, containerID); err != nil {
		return "", err
	}

	if err := d.waitForContainerCompletion(ctx, containerID, req.HeartbeatFunc); err != nil {
		return "", err
	}

	output, err := d.getContainerLogs(ctx, containerID)
	if err != nil {
		return "", err
	}

	logger.Debugf("Docker container output: %s", string(output))

	return string(output), nil
}

func (d *DockerExecutor) Cleanup(ctx context.Context, req *types.ExecutionRequest) error {
	logger.Infof("stopping container for cleanup %s", req.WorkflowID)
	if err := d.StopContainer(ctx, req.WorkflowID); err != nil {
		return fmt.Errorf("failed to stop container: %s", err)
	}
	return nil
}

func (d *DockerExecutor) Close() error {
	return d.client.Close()
}

func init() {
	executor.RegisteredExecutors[environment.Docker] = func() (executor.Executor, error) {
		return NewDockerExecutor()
	}
}
