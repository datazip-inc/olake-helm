package docker

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/client"
)

func NewContainerLogCollector(ctx context.Context, d *DockerExecutor, containerID, workDir string) (*utils.RuntimeLogCollector, error) {
	return utils.NewConnectorLogCollector(
		ctx,
		workDir,
		func(ctx context.Context, lastLogTimestamp time.Time, follow bool) (io.Reader, error) {
			return openDockerLogStream(ctx, d, containerID, lastLogTimestamp, follow)
		},
		func(ctx context.Context) bool {
			state := d.getContainerState(ctx, containerID, "")
			if !state.Exists {
				// Collector starts before the container is created; keep retrying until it exists.
				return true
			}
			return state.Running
		},
	)
}

func NewWorkerLogCollector(ctx context.Context, d *DockerExecutor, workflowID, workDir string) (*utils.RuntimeLogCollector, error) {
	containerID, err := workerContainerID()
	if err != nil {
		return nil, err
	}

	return utils.NewWorkerLogCollector(ctx, workflowID, workDir,
		func(ctx context.Context, lastLogTimestamp time.Time, follow bool) (io.Reader, error) {
			return openDockerLogStream(ctx, d, containerID, lastLogTimestamp, follow)
		},
	)
}

func RecoverWorkerLogs(ctx context.Context, d *DockerExecutor) error {
	containerID, err := workerContainerID()
	if err != nil {
		return err
	}

	logger.Infof("recovering worker logs from container %s", containerID)
	return utils.RecoverWorkerLogs(ctx, func(ctx context.Context) (io.Reader, error) {
		return openDockerLogStream(ctx, d, containerID, time.Time{}, false)
	})
}

func workerContainerID() (string, error) {
	return os.Hostname()
}

func openDockerLogStream(ctx context.Context, d *DockerExecutor, containerID string, since time.Time, follow bool) (io.Reader, error) {
	opts := client.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     follow,
		Timestamps: true,
	}
	if !since.IsZero() {
		opts.Since = fmt.Sprintf("%d.%09d", since.Unix(), since.Nanosecond())
	}

	reader, err := d.client.ContainerLogs(ctx, containerID, opts)
	if err != nil {
		return nil, err
	}

	logStreamReader, pipeWriter := io.Pipe()
	go func() {
		defer reader.Close()
		defer pipeWriter.Close()
		_, _ = stdcopy.StdCopy(pipeWriter, pipeWriter, reader)
	}()
	return logStreamReader, nil
}
