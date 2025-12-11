package docker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/pkg/stdcopy"
)

const (
	DockerPullTimeout = 2 * time.Minute
)

type ContainerState struct {
	Exists   bool
	Running  bool
	ExitCode *int
}

func (d *DockerExecutor) PullImage(ctx context.Context, imageName, version string) error {
	log := logger.Log(ctx)
	_, err := d.client.ImageInspect(ctx, imageName)
	if err != nil {
		pullCtx, cancel := context.WithTimeout(ctx, DockerPullTimeout)
		defer cancel()

		// Image doesn't exist, pull it
		log.Info("image not found locally, pulling", "image", imageName)
		reader, err := d.client.ImagePull(pullCtx, imageName, image.PullOptions{})
		if err != nil {
			if errors.Is(pullCtx.Err(), context.DeadlineExceeded) {
				log.Error("image pull timed out", "image", imageName)
				return fmt.Errorf("image pull for %s timed out", imageName)
			}
			log.Error("image pull failed", "image", imageName, "error", err)
			return fmt.Errorf("image pull %s: %s", imageName, err)
		}
		defer reader.Close()

		if _, err = io.Copy(io.Discard, reader); err != nil {
			if errors.Is(pullCtx.Err(), context.DeadlineExceeded) {
				log.Error("image pull timed out", "image", imageName)
				return fmt.Errorf("image pull for %s timed out", imageName)
			}
			log.Warn("failed to read image pull output", "image", imageName, "error", err)
		}
		return nil
	}

	log.Info("using existing local image", "image", imageName)
	return nil
}

// getOrCreateContainer creates a container or returns the ID of an existing one
func (d *DockerExecutor) getOrCreateContainer(ctx context.Context, containerConfig *container.Config, hostConfig *container.HostConfig, containerName string) (string, error) {
	log := logger.Log(ctx)
	resp, err := d.client.ContainerCreate(ctx, containerConfig, hostConfig, nil, nil, containerName)
	if err != nil {
		if errdefs.IsAlreadyExists(err) || errdefs.IsConflict(err) {
			log.Info("container already exists, resuming", "containerName", containerName)
			return containerName, nil
		}

		log.Error("failed to create container", "containerName", containerName, "error", err)
		return "", fmt.Errorf("failed to create container: %s", err)
	}

	log.Debug("created container", "containerName", containerName, "containerID", resp.ID)
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
	log := logger.Log(ctx)
	inspect, err := d.client.ContainerInspect(ctx, name)
	if err != nil || inspect.ContainerJSONBase == nil || inspect.State == nil {
		log.Debug("container inspect failed or state missing", "workflowID", workflowID, "containerName", name, "error", err)
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
	log := logger.Log(ctx)
	containerName := utils.WorkflowHash(workflowID)
	log.Info("stop request received for container", "workflowID", workflowID, "containerName", containerName)

	if strings.TrimSpace(containerName) == "" {
		log.Warn("empty container name", "workflowID", workflowID)
		return fmt.Errorf("empty container name")
	}

	// Graceful stop with timeout
	timeout := constants.ContainerStopTimeout
	if err := d.client.ContainerStop(ctx, containerName, container.StopOptions{Timeout: &timeout}); err != nil {
		log.Warn("docker stop failed, attempting kill", "workflowID", workflowID, "containerName", containerName, "error", err)
		if kerr := d.client.ContainerKill(ctx, containerName, "SIGKILL"); kerr != nil {
			log.Error("docker kill failed", "workflowID", workflowID, "containerName", containerName, "error", kerr)
			return fmt.Errorf("docker kill failed: %s", kerr)
		}
	}

	// Remove container
	if err := d.client.ContainerRemove(ctx, containerName, container.RemoveOptions{Force: true}); err != nil {
		log.Error("docker rm failed", "workflowID", workflowID, "containerName", containerName, "error", err)
		return fmt.Errorf("workflowID %s: docker rm failed for %s: %s", workflowID, containerName, err)
	}

	log.Info("container removed successfully", "workflowID", workflowID, "containerName", containerName)
	return nil
}

func (d *DockerExecutor) startContainer(ctx context.Context, containerID string) error {
	log := logger.Log(ctx)
	err := d.client.ContainerStart(ctx, containerID, container.StartOptions{})
	if err != nil && !errdefs.IsAlreadyExists(err) {
		log.Error("failed to start container", "containerID", containerID, "error", err)
		return fmt.Errorf("failed to start container %s: %s", containerID, err)
	}
	log.Debug("container started", "containerID", containerID)
	return nil
}

func (d *DockerExecutor) waitForContainerCompletion(ctx context.Context, containerID string, heartbeatFunc func(context.Context, ...interface{})) error {
	log := logger.Log(ctx)
	statusCh, errCh := d.client.ContainerWait(ctx, containerID, container.WaitConditionNotRunning)

	for {
		if heartbeatFunc != nil {
			heartbeatFunc(ctx, fmt.Sprintf("waiting for container %s", containerID))
		}

		select {
		case <-ctx.Done():
			log.Warn("context cancelled while waiting for container", "containerID", containerID)
			return ctx.Err()

		case status := <-statusCh:
			if status.StatusCode != 0 {
				logOutput, _ := d.getContainerLogs(ctx, containerID)
				log.Error("container exited with non-zero status", "containerID", containerID, "statusCode", status.StatusCode)
				return fmt.Errorf("%w: container %s exited with status %d: %s",
					constants.ErrExecutionFailed,
					containerID,
					status.StatusCode,
					string(logOutput))
			}
			return nil

		case err := <-errCh:
			if err != nil {
				// CRITICAL: Check if error is because context was cancelled
				if ctx.Err() != nil {
					log.Info("container wait failed due to context cancellation", "containerID", containerID, "dockerError", err)
					return ctx.Err() // Return cancellation error, not docker error
				}
				log.Error("error waiting for container", "containerID", containerID, "error", err)
				return fmt.Errorf("error waiting for container %s: %s", containerID, err)
			}
			return nil

		case <-time.After(5 * time.Second):
			// continue
		}
	}
}

func (d *DockerExecutor) shouldStartOperation(ctx context.Context, req *types.ExecutionRequest, containerName, workDir string) (*types.Result, error) {
	log := logger.Log(ctx)
	// Inspect container state
	state := d.getContainerState(ctx, containerName, req.WorkflowID)

	// If container is running, adopt and wait for completion
	if state.Exists && state.Running {
		log.Info("adopting running container", "workflowID", req.WorkflowID, "containerName", containerName)
		if err := d.waitForContainerCompletion(ctx, containerName, req.HeartbeatFunc); err != nil {
			return nil, err
		}
		state = d.getContainerState(ctx, containerName, req.WorkflowID)
	}

	// If container exists and exited, treat as finished: cleanup and return status
	if state.Exists && !state.Running && state.ExitCode != nil {
		log.Info("container exited", "workflowID", req.WorkflowID, "containerName", containerName, "exitCode", *state.ExitCode)

		if *state.ExitCode == 0 {
			return &types.Result{OK: false, Message: "sync status: completed"}, nil
		}

		if req.Command == types.ClearDestination {
			log.Info("removing old container for clear-destination", "workflowID", req.WorkflowID, "containerName", containerName)
			if err := d.client.ContainerRemove(ctx, containerName, container.RemoveOptions{Force: true}); err != nil {
				log.Error("failed to remove old container", "containerName", containerName, "error", err)
				return nil, fmt.Errorf("failed to remove old container: %w", err)
			}
			return &types.Result{OK: true}, nil
		}

		return nil, fmt.Errorf("workflowID %s: container %s exit %d", req.WorkflowID, containerName, *state.ExitCode)
	}

	// First launch path: only if we never launched and nothing is running
	if !utils.WorkflowAlreadyLaunched(workDir) {
		return &types.Result{OK: true}, nil
	}

	// Skip if container is not running, was already launched (logs exist), and no new run is needed.
	log.Info("container already handled, skipping launch", "workflowID", req.WorkflowID, "containerName", containerName)
	return &types.Result{OK: false, Message: "sync status: skipped"}, nil
}
