// GitOps: failure-indicator containers for CR validation errors (spawn/delete via IndicatorWorkflow).
package docker

import (
	"context"
	"fmt"

	"github.com/containerd/errdefs"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

const (
	indicatorDockerImage    = "busybox"
	indicatorTerminationMax = 4096
	labelIndicatorDocker    = "olake.io/indicator"
	labelKindDocker         = "olake.io/kind"
	labelCRDocker           = "olake.io/cr"
	labelPhaseDocker        = "olake.io/phase"
	// Log the error, then stay alive so the container remains visible in `docker ps`.
	// Phase=Failed label marks it as a GitOps failure indicator (not a healthy workload).
	indicatorDockerCmd = `printf '%s\n' "$OLAKE_ERROR" | tee /dev/termination-log >&2; exec sleep infinity`
)

func (d *DockerExecutor) Indicator(ctx context.Context, req *types.IndicatorRequest) error {
	switch req.Action {
	case "delete":
		return d.deleteIndicatorContainer(ctx, req.Name)
	case "spawn":
		return d.spawnIndicatorContainer(ctx, req)
	default:
		return fmt.Errorf("unknown indicator action %q", req.Action)
	}
}

func (d *DockerExecutor) deleteIndicatorContainer(ctx context.Context, name string) error {
	_, err := d.client.ContainerRemove(ctx, name, client.ContainerRemoveOptions{Force: true})
	if err != nil && !errdefs.IsNotFound(err) {
		return err
	}
	return nil
}

func (d *DockerExecutor) spawnIndicatorContainer(ctx context.Context, req *types.IndicatorRequest) error {
	log := logger.Log(ctx)
	name := req.Name
	// remove any existing container with the same name to update the error message
	_, _ = d.client.ContainerRemove(ctx, name, client.ContainerRemoveOptions{Force: true})

	if err := d.PullImage(ctx, indicatorDockerImage, ""); err != nil {
		log.Error("pull indicator image failed", "image", indicatorDockerImage, "error", err)
		return fmt.Errorf("pull indicator image %s: %w", indicatorDockerImage, err)
	}

	msg := truncateIndicatorDocker(req.Message, indicatorTerminationMax)
	containerConfig := &container.Config{
		Image: indicatorDockerImage,
		Cmd:   []string{"sh", "-c", indicatorDockerCmd},
		Env:   []string{fmt.Sprintf("OLAKE_ERROR=%s", msg)},
		Labels: map[string]string{
			labelIndicatorDocker: "true",
			labelKindDocker:      req.Kind,
			labelCRDocker:        req.CRName,
			labelPhaseDocker:     "Failed",
		},
	}

	resp, err := d.client.ContainerCreate(ctx, client.ContainerCreateOptions{
		Name:   name,
		Config: containerConfig,
	})
	if err != nil {
		log.Error("create indicator container failed", "name", name, "error", err)
		return fmt.Errorf("create indicator container: %w", err)
	}

	if _, err := d.client.ContainerStart(ctx, resp.ID, client.ContainerStartOptions{}); err != nil {
		log.Error("start indicator container failed", "name", name, "error", err)
		return fmt.Errorf("start indicator container: %w", err)
	}
	return nil
}

func truncateIndicatorDocker(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
