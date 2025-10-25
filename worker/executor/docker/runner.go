package docker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/executor"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
)

func (d *DockerExecutor) RunContainer(ctx context.Context, req *executor.ExecutionRequest, workDir string) (string, error) {
	imageName := utils.GetDockerImageName(req.ConnectorType, req.Version)
	containerName := utils.GetWorkflowDirectory(req.Command, req.WorkflowID)

	logger.Infof("running container - command: %s, image: %s, name: %s", req.Command, imageName, containerName)

	if req.Command == types.Sync {
		return d.runSyncContainer(ctx, req, imageName, containerName, workDir)
	}

	return d.executeContainer(ctx, containerName, imageName, req, workDir)
}

func (d *DockerExecutor) runSyncContainer(ctx context.Context, req *executor.ExecutionRequest, imageName, containerName, workDir string) (string, error) {
	// Marker to indicate we have launched once
	oldContainerExists := filepath.Join(workDir, "logs")

	// Inspect container state
	state := d.getContainerState(ctx, containerName, req.WorkflowID)

	// 1) If container is running, adopt and wait for completion
	if state.Exists && state.Running {
		logger.Infof("workflowID %s: adopting running container %s", req.WorkflowID, containerName)
		if err := d.waitForContainerCompletion(ctx, containerName, req.HeartbeatFunc); err != nil {
			return "", err
		}
		state = d.getContainerState(ctx, containerName, req.WorkflowID)
	}

	// 2) If container exists and exited, treat as finished: cleanup and return status
	if state.Exists && !state.Running && state.ExitCode != nil {
		logger.Infof("workflowID %s: container %s exited with code %d", req.WorkflowID, containerName, *state.ExitCode)
		if *state.ExitCode == 0 {
			return "sync status: completed", nil
		}
		return "", fmt.Errorf("workflowID %s: container %s exit %d", req.WorkflowID, containerName, *state.ExitCode)
	}

	// 4) First launch path: only if we never launched and nothing is running
	if _, err := os.Stat(oldContainerExists); os.IsNotExist(err) {
		logger.Infof("workflowID %s: first launch path, creating container", req.WorkflowID)
		return d.executeContainer(ctx, containerName, imageName, req, workDir)
	}

	// Skip if container is not running, was already launched (logs exist), and no new run is needed.
	logger.Infof("workflowID %s: container %s already handled, skipping launch", req.WorkflowID, containerName)
	return "sync status: skipped", nil
}

func (d *DockerExecutor) executeContainer(ctx context.Context, containerName, imageName string, req *executor.ExecutionRequest, workDir string) (string, error) {
	if err := utils.WriteConfigFiles(workDir, req.Configs); err != nil {
		return "", err
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
	if workDir != "" {
		hostOutputDir := utils.GetHostOutputDir(workDir)
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
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
