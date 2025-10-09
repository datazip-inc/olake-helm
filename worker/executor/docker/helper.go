package docker

import (
	"bytes"
	"context"
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

	logger.Infof("Running container - Command: %s, Image: %s, Name: %s", req.Command, imageName, containerName)

	if req.Command == types.Sync {
		return d.runSyncContainer(ctx, req, imageName, containerName, workDir)
	}

	return d.runSimpleContainer(ctx, req, imageName, containerName, workDir)
}

func (d *DockerExecutor) runSyncContainer(ctx context.Context, req *executor.ExecutionRequest, imageName, containerName, workDir string) (string, error) {
	// Marker to indicate we have launched once
	launchedMarker := filepath.Join(workDir, "logs")

	// Inspect container state
	state := d.getContainerState(ctx, containerName, req.WorkflowID)

	// 1) If container is running, adopt and wait for completion
	if state.Exists && state.Running {
		logger.Infof("workflowID %s: adopting running container %s", req.WorkflowID, containerName)
		if err := d.waitContainer(ctx, containerName, req.WorkflowID); err != nil {
			logger.Errorf("workflowID %s: container wait failed: %s", req.WorkflowID, err)
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

		if err := utils.WriteConfigFiles(workDir, req.Configs); err != nil {
			return "", err
		}

		if err := d.PullImage(ctx, imageName, req.Version); err != nil {
			return "", err
		}

		return d.executeContainer(ctx, containerName, imageName, req, workDir)
	}

	// Skip if container is not running, was already launched (logs exist), and no new run is needed.
	logger.Infof("workflowID %s: container %s already handled, skipping launch", req.WorkflowID, containerName)
	return "sync status: skipped", nil
}

// runSimpleContainer handles non-sync operations (spec, check, discover,...)
func (d *DockerExecutor) runSimpleContainer(ctx context.Context, req *executor.ExecutionRequest, imageName, containerName, workDir string) (string, error) {
	if err := utils.WriteConfigFiles(workDir, req.Configs); err != nil {
		return "", err
	}

	if err := d.PullImage(ctx, imageName, req.Version); err != nil {
		return "", err
	}

	return d.executeContainer(ctx, containerName, imageName, req, workDir)
}

func (d *DockerExecutor) executeContainer(ctx context.Context, containerName, imageName string, req *executor.ExecutionRequest, workDir string) (string, error) {
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

	logger.Infof("Running Docker container with image: %s, name: %s, command: %v", imageName, containerName, req.Args)

	resp, err := d.client.ContainerCreate(ctx, containerConfig, hostConfig, nil, nil, containerName)
	if err != nil && !errdefs.IsAlreadyExists(err) {
		return "", fmt.Errorf("failed to create container: %v", err)
	}
	containerID := resp.ID
	if containerID == "" {
		// Container might already exist
		containerID = containerName
	}
	defer d.client.ContainerRemove(ctx, containerID, container.RemoveOptions{Force: true})

	if err := d.client.ContainerStart(ctx, containerID, container.StartOptions{}); err != nil {
		// If it's already running, continue
		if !errdefs.IsAlreadyExists(err) {
			return "", fmt.Errorf("failed to start container: %v", err)
		}
	}

	statusCh, errCh := d.client.ContainerWait(ctx, containerID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			return "", fmt.Errorf("error waiting for container: %v", err)
		}
	case status := <-statusCh:
		if status.StatusCode != 0 {
			// Get container logs for error debugging
			logOutput, _ := d.getContainerLogs(ctx, containerID)
			return "", fmt.Errorf("container exited with status %d: %s", status.StatusCode, string(logOutput))
		}
	}

	output, err := d.getContainerLogs(ctx, containerID)
	if err != nil {
		return "", fmt.Errorf("failed to get container logs: %v", err)
	}

	logger.Infof("Docker container output: %s", string(output))

	return string(output), nil
}

func (d *DockerExecutor) PullImage(ctx context.Context, imageName, version string) error {
	// Always pull if version is "latest"
	if strings.EqualFold(version, "latest") {
		logger.Infof("Pulling latest image: %s", imageName)
		reader, err := d.client.ImagePull(ctx, imageName, image.PullOptions{})
		if err != nil {
			return fmt.Errorf("image pull %s: %s", imageName, err)
		}
		defer reader.Close()

		// Read the pull output (optional, for logging)
		if _, err = io.Copy(io.Discard, reader); err != nil {
			logger.Warnf("Failed to read image pull output: %v", err)
		}
		return nil
	}

	logger.Infof("Using image: %s", imageName)
	return nil
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

func (d *DockerExecutor) waitContainer(ctx context.Context, name, workflowID string) error {
	statusCh, errCh := d.client.ContainerWait(ctx, name, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			logger.Errorf("workflowID %s: container wait failed for %s: %s", workflowID, name, err)
			return fmt.Errorf("docker wait failed: %s", err)
		}
		return nil
	case status := <-statusCh:
		if status.StatusCode != 0 {
			return fmt.Errorf("workflowID %s: container %s exited with code %d", workflowID, name, status.StatusCode)
		}
		return nil
	}
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
