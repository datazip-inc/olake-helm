package docker

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
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
	log := logger.Log(ctx)
	imageName := utils.GetDockerImageName(req.ConnectorType, req.Version)
	containerName := utils.GetWorkflowDirectory(req.Command, req.WorkflowID)
	log.Info("running container", "command", req.Command, "image", imageName, "containerName", containerName)

	if slices.Contains(constants.AsyncCommands, req.Command) {
		startOperation, err := d.shouldStartOperation(ctx, req, containerName, workdir)
		if err != nil {
			log.Error("failed to check operation status", "containerName", containerName, "error", err)
			return "", err
		}
		if !startOperation.OK {
			return startOperation.Message, nil
		}
	}

	if err := d.PullImage(ctx, imageName, req.Version); err != nil {
		log.Error("failed to pull image", "image", imageName, "error", err)
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

	log.Info("creating docker container", "image", imageName, "containerName", containerName, "command", req.Args)

	containerID, err := d.getOrCreateContainer(ctx, containerConfig, hostConfig, containerName)
	if err != nil {
		log.Error("failed to create container", "containerName", containerName, "error", err)
		return "", err
	}
	if !slices.Contains(constants.AsyncCommands, req.Command) {
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second*constants.ContainerCleanupTimeout)
			defer cancel()

			if err := d.client.ContainerRemove(cleanupCtx, containerID, container.RemoveOptions{Force: true}); err != nil {
				log.Warn("failed to remove container", "containerID", containerID, "error", err)
			}
		}()
	}

	if err := d.startContainer(ctx, containerID); err != nil {
		log.Error("failed to start container", "containerID", containerID, "error", err)
		return "", err
	}

	if err := d.waitForContainerCompletion(ctx, containerID, req.HeartbeatFunc); err != nil {
		log.Error("container failed to complete", "containerID", containerID, "error", err)
		return "", err
	}

	output, err := d.getContainerLogs(ctx, containerID)
	if err != nil {
		log.Error("failed to get container logs", "containerID", containerID, "error", err)
		return "", err
	}

	return string(output), nil
}

func (d *DockerExecutor) Cleanup(ctx context.Context, req *types.ExecutionRequest) error {
	log := logger.Log(ctx)
	log.Info("stopping container for cleanup", "workflowID", req.WorkflowID)

	if err := d.StopContainer(ctx, req.WorkflowID); err != nil {
		log.Error("failed to stop container", "workflowID", req.WorkflowID, "error", err)
		return fmt.Errorf("failed to stop container: %s", err)
	}

	log.Info("container cleanup completed", "workflowID", req.WorkflowID)
	return nil
}

func (d *DockerExecutor) Close() error {
	return d.client.Close()
}
