package docker

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/executor"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
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

func (d *DockerExecutor) Execute(ctx context.Context, req *executor.ExecutionRequest) (map[string]interface{}, error) {
	subDir := utils.GetWorkflowDirectory(req.Command, req.WorkflowID)
	workDir, err := utils.SetupWorkDirectory(d.workingDir, subDir)
	if err != nil {
		return nil, err
	}

	// Q: Since we are not writing files, when sync is already launched
	// it is moved inside the container methods.
	// if err := utils.WriteConfigFiles(workDir, req.Configs); err != nil {
	// 	return nil, err
	// }
	// Question: Telemetry requires streams.json, so cleaning up fails telemetry. Do we need cleanup?
	// defer utils.CleanupConfigFiles(workDir, req.Configs)

	out, err := d.RunContainer(ctx, req, workDir)
	if err != nil {
		return nil, err
	}

	if req.OutputFile != "" {
		fileContent, err := utils.ReadFile(filepath.Join(workDir, req.OutputFile))
		if err != nil {
			return nil, fmt.Errorf("failed to parse JSON file: %s", err)
		}
		return map[string]interface{}{
			"response": fileContent,
		}, nil
	}

	return map[string]interface{}{
		"response": out,
	}, nil
}

func (d *DockerExecutor) SyncCleanup(ctx context.Context, req *executor.ExecutionRequest) error {
	// Stop container gracefully
	logger.Infof("stopping container for cleanup %s", req.WorkflowID)
	if err := d.StopContainer(ctx, req.WorkflowID); err != nil {
		return fmt.Errorf("failed to stop container: %s", err)
	}

	stateFile, err := utils.GetStateFileFromWorkdir(d.workingDir, req.WorkflowID, req.Command)
	if err != nil {
		return err
	}

	if err := database.GetDB().UpdateJobState(ctx, req.JobID, stateFile, true); err != nil {
		return err
	}

	logger.Infof("successfully cleaned up sync for job %d", req.JobID)
	return nil
}

func (d *DockerExecutor) Close() error {
	return d.client.Close()
}

func init() {
	executor.RegisteredExecutors[executor.Docker] = func() (executor.Executor, error) {
		return NewDockerExecutor()
	}
}
