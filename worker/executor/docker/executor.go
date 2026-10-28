package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/storage"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"
	"github.com/spf13/viper"
)

type DockerExecutor struct {
	client     *client.Client
	workingDir string
	// bindMounts are the user-supplied host mounts added to every connector container.
	bindMounts []mount.Mount
}

// jobBindMount is one user-supplied bind mount for connector containers.
type jobBindMount struct {
	Source   string `json:"source"`
	Target   string `json:"target"`
	ReadOnly bool   `json:"readOnly"`
}

func NewDockerExecutor() (*DockerExecutor, error) {
	client, err := client.New(client.FromEnv)
	if err != nil {
		return nil, fmt.Errorf("failed to create docker client: %s", err)
	}

	// A bad value fails startup: silently dropping the mounts would leave connector
	// containers without the certificates they expect.
	bindMounts, err := parseJobBindMounts(viper.GetString(constants.EnvJobBindMounts))
	if err != nil {
		return nil, err
	}

	return &DockerExecutor{client: client, workingDir: storage.ConfigDir(), bindMounts: bindMounts}, nil
}

func (d *DockerExecutor) Execute(ctx context.Context, req *types.ExecutionRequest, workdir string) (string, error) {
	log := logger.Log(ctx)
	imageName := utils.GetDockerImageName(req.ConnectorType, req.Version)
	containerName := utils.GetWorkflowDirectory(req.Command, req.WorkflowID)
	log.Info("running container", "command", req.Command, "image", imageName, "containerName", containerName)

	if utils.IsAsyncCommand(req.Command) {
		startOperation, err := d.shouldStartOperation(ctx, req, containerName, workdir)
		if err != nil {
			log.Error("failed to check operation status", "containerName", containerName, "error", err)
			return "", err
		}
		if !startOperation.OK {
			return startOperation.Message, nil
		}
	}

	if err := d.PullImage(ctx, imageName, req.Version, req.HeartbeatFunc); err != nil {
		log.Error("failed to pull image", "image", imageName, "error", err)
		return "", err
	}

	indexMount, err := d.ensureIndexMount(req.JobID, req.Command, req.IndexRequired)
	if err != nil {
		log.Error("failed to prepare index directory", "jobID", req.JobID, "error", err)
		return "", err
	}

	// Environment variables propagation
	envVars := utils.GetWorkerEnvVars()
	envVars[constants.EnvS3ConfigFolder] = utils.ConnectorConfigDir(req.Command, req.WorkflowID)
	if indexMount != nil {
		envVars[constants.EnvIndexDBDir] = indexMount.Target

		if envVars[constants.EnvIndexDBCacheSize] == "" {
			envVars[constants.EnvIndexDBCacheSize] = strconv.Itoa(constants.DefaultIndexCacheSizeMB)
		}

		if envVars[constants.EnvIndexDBMaxOpenFiles] == "" {
			envVars[constants.EnvIndexDBMaxOpenFiles] = strconv.Itoa(constants.DefaultIndexMaxOpenFiles)
		}
	}

	var envs []string
	for k, v := range envVars {
		envs = append(envs, fmt.Sprintf("%s=%s", k, v))
	}

	containerConfig := &container.Config{
		Image: imageName,
		Cmd:   req.Args,
		Env:   envs,
	}

	hostConfig := &container.HostConfig{}
	if workdir != "" && storage.Mode() == constants.StorageModeNFS {
		hostOutputDir := utils.GetHostOutputDir(workdir)
		hostConfig.Mounts = []mount.Mount{
			{Type: mount.TypeBind, Source: hostOutputDir, Target: constants.ContainerMountDir},
		}
	}

	if indexMount != nil {
		hostConfig.Mounts = append(hostConfig.Mounts, *indexMount)
	}
	hostConfig.Mounts = append(hostConfig.Mounts, d.bindMounts...)

	log.Info("creating docker container", "image", imageName, "containerName", containerName, "command", req.Args)

	containerID, err := d.getOrCreateContainer(ctx, containerConfig, hostConfig, containerName)
	if err != nil {
		log.Error("failed to create container", "containerName", containerName, "error", err)
		return "", err
	}
	if !utils.IsAsyncCommand(req.Command) {
		defer func() {
			utils.ReleaseConnectorLogCollector(workdir)
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second*constants.ContainerCleanupTimeout)
			defer cancel()

			if _, err := d.client.ContainerRemove(cleanupCtx, containerID, client.ContainerRemoveOptions{Force: true}); err != nil {
				log.Warn("failed to remove container", "containerID", containerID, "error", err)
			}
		}()
	}

	if err := d.startContainer(ctx, containerID); err != nil {
		log.Error("failed to start container", "containerID", containerID, "error", err)
		return "", err
	}

	if storage.Mode() == constants.StorageModeS3 {
		// Stream connector logs to S3 while the container runs; Release later stops it with a final catch-up and flush.
		err := utils.AcquireConnectorLogCollector(ctx, workdir, func() (*utils.ConnectorLogCollector, error) {
			return NewContainerLogCollector(ctx, d, containerID, workdir)
		})
		if err != nil {
			log.Error("failed to start connector log collector", "containerID", containerID, "error", err)
			return "", fmt.Errorf("failed to start connector log collector: %s", err)
		}
	}

	log.Info(fmt.Sprintf("Container %s started", containerName))

	if err := d.waitForContainerCompletion(ctx, containerID, req.HeartbeatFunc); err != nil {
		if req.Command != types.Sync { // SyncActivity explains sync failures itself
			log.Error("container failed to complete", "containerID", containerID, "error", err)
		}
		return "", err
	}
	log.Info(fmt.Sprintf("Container %s completed successfully", containerName))

	output, err := d.getContainerLogs(ctx, containerID, "")
	if err != nil {
		log.Error("failed to get container logs", "containerID", containerID, "error", err)
		return "", err
	}

	return string(output), nil
}

// flushExitedConnectorLogs uploads leftover connector logs while the container still exists.
// Acquire reuses or starts a follow collector (follow ends by itself on an exited container);
// Release then stops it and runs the final catch-up and flush before returning.
func (d *DockerExecutor) flushExitedConnectorLogs(ctx context.Context, workDir, containerName string) error {
	if storage.Mode() != constants.StorageModeS3 {
		return nil
	}
	err := utils.AcquireConnectorLogCollector(ctx, workDir, func() (*utils.ConnectorLogCollector, error) {
		return NewContainerLogCollector(ctx, d, containerName, workDir)
	})
	if err != nil {
		return fmt.Errorf("failed to start connector log collector: %s", err)
	}
	utils.ReleaseConnectorLogCollector(workDir)
	return nil
}

// ensureIndexMount returns the bind mount that carries a job's Pebble index, or
// nil when the job gets none.
//
// The index instead gets its own persistence-root directory keyed
// on JobID alone, matching the per-job claim the kubernetes executor mounts.
func (d *DockerExecutor) ensureIndexMount(jobID int, operation types.Command, indexRequired bool) (*mount.Mount, error) {
	// Opt-in per job, and only for the operations that touch the index.
	if !utils.IsAsyncCommand(operation) || !indexRequired {
		return nil, nil
	}

	indexDir := filepath.Join(storage.ConfigDir(), constants.IndexDirName, fmt.Sprintf("olake-index-%d", jobID))
	if err := os.MkdirAll(indexDir, constants.DefaultDirPermissions); err != nil {
		return nil, fmt.Errorf("failed to create index directory %s: %s", indexDir, err)
	}

	for _, m := range d.bindMounts {
		if m.Target == path.Clean(constants.DefaultIndexMountPath) {
			return nil, fmt.Errorf("job bind mount target %q is the same as the index mount path %s for job %d; choose a different path",
				m.Target, constants.DefaultIndexMountPath, jobID)
		}
	}

	return &mount.Mount{
		Type:   mount.TypeBind,
		Source: utils.GetHostOutputDir(indexDir),
		Target: constants.DefaultIndexMountPath,
	}, nil
}

func (d *DockerExecutor) Cleanup(ctx context.Context, req *types.ExecutionRequest) error {
	log := logger.Log(ctx)
	log.Info("stopping container for cleanup", "workflowID", req.WorkflowID)

	_, workDir := utils.GetWorkflowDirAndSubDir(req.WorkflowID, req.Command)
	utils.ReleaseConnectorLogCollector(workDir)

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

// parseJobBindMounts decodes and validates the user-supplied connector container bind
// mounts (OLAKE_JOB_BIND_MOUNTS). Source is a path on the Docker host, since the worker
// creates connector containers through the host's Docker daemon. Each mount is
// writable unless it sets readOnly. An empty input yields nothing.
func parseJobBindMounts(mountsJSON string) ([]mount.Mount, error) {
	if strings.TrimSpace(mountsJSON) == "" {
		return nil, nil
	}

	var entries []jobBindMount
	dec := json.NewDecoder(strings.NewReader(mountsJSON))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&entries); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job bind mounts: %w", err)
	}

	configDir := path.Clean(constants.ContainerMountDir)
	targets := make(map[string]struct{}, len(entries))
	mounts := make([]mount.Mount, 0, len(entries))
	for _, e := range entries {
		if !path.IsAbs(e.Source) {
			return nil, fmt.Errorf("job bind mount source %q must be an absolute host path", e.Source)
		}
		// Container paths are POSIX regardless of where the worker runs.
		target := path.Clean(e.Target)
		if !path.IsAbs(e.Target) || target == "/" {
			return nil, fmt.Errorf("job bind mount target %q must be an absolute container path other than the root", e.Target)
		}
		if target == configDir || strings.HasPrefix(target, configDir+"/") {
			return nil, fmt.Errorf("job bind mount target %q would shadow the shared config directory %s", e.Target, configDir)
		}
		if _, dup := targets[target]; dup {
			return nil, fmt.Errorf("job bind mount target %q is used more than once", e.Target)
		}
		targets[target] = struct{}{}

		mounts = append(mounts, mount.Mount{
			Type:     mount.TypeBind,
			Source:   e.Source,
			Target:   target,
			ReadOnly: e.ReadOnly,
		})
	}

	return mounts, nil
}
