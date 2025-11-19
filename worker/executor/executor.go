package executor

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/executor/docker"
	"github.com/datazip-inc/olake-helm/worker/executor/kubernetes"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
)

// Executor interface for k8s and docker executor
type Executor interface {
	Execute(ctx context.Context, req *types.ExecutionRequest, workdir string) (string, error)
	Cleanup(ctx context.Context, req *types.ExecutionRequest) error
	Close() error
}

type AbstractExecutor struct {
	executor Executor
	db       *database.DB
}

// NewExecutor creates and returns the executor client based on the executor environment
func NewExecutor(db *database.DB) (*AbstractExecutor, error) {
	executorEnv := utils.GetExecutorEnvironment()

	var exec Executor
	var err error

	switch executorEnv {
	case string(types.Docker):
		exec, err = docker.NewDockerExecutor()
	case string(types.Kubernetes):
		exec, err = kubernetes.NewKubernetesExecutor()
	default:
		exec, err = nil, fmt.Errorf("invalid executor environment: %s", executorEnv)
	}
	if err != nil {
		return nil, err
	}
	return &AbstractExecutor{executor: exec, db: db}, nil
}

func (a *AbstractExecutor) Execute(ctx context.Context, req *types.ExecutionRequest) (*types.ExecutorResponse, error) {
	subdir := utils.GetWorkflowDirectory(req.Command, req.WorkflowID)
	workdir, err := utils.SetupWorkDirectory(utils.GetConfigDir(), subdir)
	if err != nil {
		return nil, err
	}

	// write config files only for the first/scheduled workflow execution (not for retries)
	if !utils.WorkflowAlreadyLaunched(workdir) && req.Configs != nil {
		if err := utils.WriteConfigFiles(workdir, req.Configs); err != nil {
			return nil, err
		}
	}

	out, err := a.executor.Execute(ctx, req, workdir)
	if err != nil {
		return nil, err
	}

	// output path has the dockerPersistent dir as base path because this response
	// is read in the bff (olake-ui) which reads the file from the dockerPersistent dir.

	// generated file as response
	if req.OutputFile != "" {
		filePath := filepath.Join(constants.DockerPersistentDir, subdir, req.OutputFile)
		return &types.ExecutorResponse{Response: filePath}, nil
	}

	outJSON, err := utils.ExtractJSONAndMarshal(out)
	if err != nil {
		return nil, err
	}

	outputPath := filepath.Join(workdir, constants.OutputFileName)
	if err := utils.WriteFile(outputPath, outJSON); err != nil {
		return nil, err
	}

	// logs as response
	return &types.ExecutorResponse{Response: filepath.Join(constants.DockerPersistentDir, subdir, constants.OutputFileName)}, nil
}

// CleanupAndPersistState stops the container/pod and saves the state file in the database
func (a *AbstractExecutor) CleanupAndPersistState(ctx context.Context, req *types.ExecutionRequest) error {
	if err := a.executor.Cleanup(ctx, req); err != nil {
		return err
	}

	stateFile, err := utils.GetStateFileFromWorkdir(req.WorkflowID, req.Command)
	if err != nil {
		return err
	}

	if err := a.db.UpdateJobState(ctx, req.JobID, stateFile); err != nil {
		return err
	}

	logger.Infof("successfully cleaned up sync for job %d", req.JobID)
	return nil
}

func (a *AbstractExecutor) Close() {
	a.executor.Close()
}
