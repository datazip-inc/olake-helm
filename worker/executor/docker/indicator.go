// GitOps: failure-indicator containers for ConfigMap/Secret validation errors (spawn/delete via IndicatorWorkflow).
package docker

import (
	"context"
	"fmt"

	"github.com/containerd/errdefs"
	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

// Log the error, then stay alive so the container remains visible in `docker ps`.
// The phase=Failed label marks it as a GitOps failure indicator (not a healthy workload).
const indicatorCmd = `printf '%s\n' "$OLAKE_ERROR" | tee /dev/termination-log >&2; exec sleep infinity`

func (d *DockerExecutor) Indicator(ctx context.Context, req *types.IndicatorRequest) error {
	switch req.Action {
	case types.IndicatorActionDelete:
		return d.deleteIndicatorContainer(ctx, req.Name)
	case types.IndicatorActionSpawn:
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
	// read the image first, so a failed read keeps the previous indicator in place
	image, err := d.indicatorImage(ctx)
	if err != nil {
		log.Error("read worker image failed", "error", err)
		return fmt.Errorf("read worker image: %w", err)
	}

	// remove any existing container with the same name to update the error message
	_, _ = d.client.ContainerRemove(ctx, name, client.ContainerRemoveOptions{Force: true})

	containerConfig := &container.Config{
		Image:      image,
		Entrypoint: []string{"sh", "-c"},
		Cmd:        []string{indicatorCmd},
		Env:        []string{fmt.Sprintf("OLAKE_ERROR=%s", utils.Truncate(req.Message, constants.IndicatorMessageMax))},
		Labels: map[string]string{
			constants.LabelIndicator:         "true",
			constants.LabelIndicatorKind:     req.Kind,
			constants.LabelIndicatorResource: req.ResourceName,
			constants.LabelIndicatorPhase:    "Failed",
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

	return d.startContainer(ctx, resp.ID)
}

// indicatorImage returns the image of the worker's own container. It is already on this Docker host,
// so the indicator needs no pull, which keeps it working offline.
func (d *DockerExecutor) indicatorImage(ctx context.Context) (string, error) {
	containerID, err := workerContainerID()
	if err != nil {
		return "", err
	}
	inspect, err := d.client.ContainerInspect(ctx, containerID, client.ContainerInspectOptions{})
	if err != nil {
		return "", err
	}
	return inspect.Container.Config.Image, nil
}
