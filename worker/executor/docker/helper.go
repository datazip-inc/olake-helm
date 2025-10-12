package docker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/executor"
	"github.com/datazip-inc/olake-helm/worker/logger"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/pkg/stdcopy"
)

type ContainerState struct {
	Exists   bool
	Running  bool
	ExitCode *int
}

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
	launchedMarker := filepath.Join(workDir, "logs")

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
	if _, err := os.Stat(launchedMarker); os.IsNotExist(err) {
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
				logger.Warnf("failed to remove container: %v", err)
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

func (d *DockerExecutor) PullImage(ctx context.Context, imageName, version string) error {
	_, err := d.client.ImageInspect(ctx, imageName)
	if err != nil {
		// Image doesn't exist, pull it
		logger.Infof("image %s not found locally, pulling...", imageName)
		reader, err := d.client.ImagePull(ctx, imageName, image.PullOptions{})
		if err != nil {
			return fmt.Errorf("image pull %s: %s", imageName, err)
		}
		defer reader.Close()

		if _, err = io.Copy(io.Discard, reader); err != nil {
			logger.Warnf("failed to read image pull output: %v", err)
		}
		return nil
	}

	logger.Infof("using existing local image: %s", imageName)
	return nil
}

// getOrCreateContainer creates a container or returns the ID of an existing one
func (d *DockerExecutor) getOrCreateContainer(ctx context.Context, containerConfig *container.Config, hostConfig *container.HostConfig, containerName string) (string, error) {
	resp, err := d.client.ContainerCreate(ctx, containerConfig, hostConfig, nil, nil, containerName)
	if err != nil {
		if !errdefs.IsAlreadyExists(err) {
			return "", fmt.Errorf("failed to create container: %v", err)
		}
		// Container already exists, use the name as ID
		logger.Infof("container %s already exists, resuming", containerName)
		return containerName, nil
	}
	logger.Debugf("created container %s (ID: %s)", containerName, resp.ID)
	return resp.ID, nil
}

// getContainerLogs retrieves and properly parses logs from a container using stdcopy
func (d *DockerExecutor) getContainerLogs(ctx context.Context, containerID string) ([]byte, error) {
	reader, err := d.client.ContainerLogs(ctx, containerID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
	})
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	var stdoutBuf, stderrBuf bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdoutBuf, &stderrBuf, reader); err != nil {
		return nil, err
	}

	// Prefer stdout, but include stderr if present
	if stderrBuf.Len() > 0 && stdoutBuf.Len() == 0 {
		return stderrBuf.Bytes(), nil
	}
	if stderrBuf.Len() > 0 {
		return append(stdoutBuf.Bytes(), []byte("\n"+stderrBuf.String())...), nil
	}
	return stdoutBuf.Bytes(), nil
}

// getContainerState inspects a container and returns its state
func (d *DockerExecutor) getContainerState(ctx context.Context, name, workflowID string) ContainerState {
	inspect, err := d.client.ContainerInspect(ctx, name)
	if err != nil || inspect.ContainerJSONBase == nil || inspect.State == nil {
		logger.Warnf("workflowID %s: container inspect failed or state missing for %s: %v", workflowID, name, err)
		return ContainerState{Exists: false}
	}

	running := inspect.State.Running
	var ec *int
	if !running && inspect.State.ExitCode != 0 {
		code := inspect.State.ExitCode
		ec = &code
	}
	return ContainerState{Exists: true, Running: running, ExitCode: ec}
}

// StopContainer stops a container by name, falling back to kill if needed (for cleanup activity)
func (d *DockerExecutor) StopContainer(ctx context.Context, workflowID string) error {
	containerName := utils.WorkflowHash(workflowID)
	logger.Infof("workflowID %s: stop request received for container %s", workflowID, containerName)

	if strings.TrimSpace(containerName) == "" {
		logger.Warnf("workflowID %s: empty container name", workflowID)
		return fmt.Errorf("empty container name")
	}

	// Graceful stop with timeout
	timeout := 5
	if err := d.client.ContainerStop(ctx, containerName, container.StopOptions{Timeout: &timeout}); err != nil {
		logger.Warnf("workflowID %s: docker stop failed for %s: %s", workflowID, containerName, err)
		if kerr := d.client.ContainerKill(ctx, containerName, "SIGKILL"); kerr != nil {
			logger.Errorf("workflowID %s: docker kill failed for %s: %s", workflowID, containerName, kerr)
			return fmt.Errorf("docker kill failed: %s", kerr)
		}
	}

	// Remove container
	if err := d.client.ContainerRemove(ctx, containerName, container.RemoveOptions{Force: true}); err != nil {
		logger.Warnf("workflowID %s: docker rm failed for %s: %s", workflowID, containerName, err)
	} else {
		logger.Infof("workflowID %s: container %s removed successfully", workflowID, containerName)
	}
	return nil
}

func (d *DockerExecutor) startContainer(ctx context.Context, containerID string) error {
	err := d.client.ContainerStart(ctx, containerID, container.StartOptions{})
	if err != nil && !errdefs.IsAlreadyExists(err) {
		return fmt.Errorf("failed to start container %s: %w", containerID, err)
	}
	logger.Debugf("container %s started", containerID)
	return nil
}

func (d *DockerExecutor) waitForContainerCompletion(ctx context.Context, containerID string, heartbeatFunc func(context.Context, ...interface{})) error {
	statusCh, errCh := d.client.ContainerWait(ctx, containerID, container.WaitConditionNotRunning)

	for {
		if heartbeatFunc != nil {
			heartbeatFunc(ctx, fmt.Sprintf("waiting for container %s", containerID))
		}

		select {
		case err := <-errCh:
			if err != nil {
				return fmt.Errorf("error waiting for container %s: %w", containerID, err)
			}
			return nil

		case status := <-statusCh:
			if status.StatusCode != 0 {
				logOutput, _ := d.getContainerLogs(ctx, containerID)
				return fmt.Errorf("%w: container %s exited with status %d: %s",
					constants.ErrExecutionFailed,
					containerID,
					status.StatusCode,
					string(logOutput))
			}
			return nil

		case <-ctx.Done():
			logger.Warnf("context cancelled while waiting for container %s", containerID)
			return ctx.Err()

		case <-time.After(5 * time.Second):
			// continue
		}
	}
}
