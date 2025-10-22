package docker

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/executor"
	"github.com/datazip-inc/olake-helm/worker/logger"
	"github.com/datazip-inc/olake-helm/worker/utils"
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

	// TODO: Check if we can separate hostPersistence and containerConfigPath
	// TODO: check backward compatibility after changing ENV VARIABLE NAME
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

	stateFilePath := filepath.Join(d.workingDir, utils.GetWorkflowDirectory(req.Command, req.WorkflowID), "state.json")
	stateFile, err := utils.ReadFile(stateFilePath)
	if err != nil {
		return fmt.Errorf("failed to read state file: %s", err)
	}

	if err := database.GetDB().UpdateJobState(req.JobID, stateFile, true); err != nil {
		return fmt.Errorf("failed to update job state: %s", err)
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
