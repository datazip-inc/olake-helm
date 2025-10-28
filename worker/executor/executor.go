package executor

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/datazip-inc/olake-helm/worker/database"
	environment "github.com/datazip-inc/olake-helm/worker/executor/enviroment"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"

	"github.com/datazip-inc/olake-helm/worker/utils"
)

// Executor interface for k8s and docker executor
type Executor interface {
	Execute(ctx context.Context, req *types.ExecutionRequest, workdir string) (string, error)
	Cleanup(ctx context.Context, req *types.ExecutionRequest) error
	Close() error
}

type AbstractExecutor struct {
	executor Executor
}

type NewFunc func() (Executor, error)

var RegisteredExecutors = map[environment.ExecutorEnvironment]NewFunc{}

// NewExecutor creates and returns the executor client based on the executor environment
func NewExecutor() (*AbstractExecutor, error) {
	executorEnv := environment.GetExecutorEnvironment()
	newFunc, ok := RegisteredExecutors[environment.ExecutorEnvironment(executorEnv)]
	if !ok {
		return nil, fmt.Errorf("invalid executor environment: %s", executorEnv)
	}

	executor, err := newFunc()
	if err != nil {
		return nil, err
	}
	return &AbstractExecutor{executor: executor}, nil
}

func (a *AbstractExecutor) Execute(ctx context.Context, req *types.ExecutionRequest) (*types.ExecutorResponse, error) {
	subdir := utils.GetWorkflowDirectory(req.Command, req.WorkflowID)
	workdir, err := utils.SetupWorkDirectory(utils.GetConfigDir(), subdir)
	if err != nil {
		return nil, err
	}

	// write config files only for the first/scheduled workflow execution (not for retries)
	if !utils.WorkflowAlreadyLaunched(workdir) {
		if err := utils.WriteConfigFiles(workdir, req.Configs); err != nil {
			return nil, err
		}
	}

	out, err := a.executor.Execute(ctx, req, workdir)
	if err != nil {
		return nil, err
	}

	// generated file as response
	if req.OutputFile != "" {
		fileContent, err := utils.ReadFile(filepath.Join(workdir, req.OutputFile))
		if err != nil {
			return nil, fmt.Errorf("failed to parse JSON file: %s", err)
		}
		return &types.ExecutorResponse{Response: fileContent}, nil
	}

	// logs as response
	return &types.ExecutorResponse{Response: out}, nil
}

// SyncCleanup stops the container/pod and saves the state file in the database
func (a *AbstractExecutor) SyncCleanup(ctx context.Context, req *types.ExecutionRequest) error {
	if err := a.executor.Cleanup(ctx, req); err != nil {
		return err
	}

	stateFile, err := utils.GetStateFileFromWorkdir(req.WorkflowID, req.Command)
	if err != nil {
		return err
	}

	if err := database.GetDB().UpdateJobState(ctx, req.JobID, stateFile, true); err != nil {
		return err
	}

	logger.Infof("successfully cleaned up sync for job %d", req.JobID)
	return nil
}

func (a *AbstractExecutor) Close() {
	a.executor.Close()
}
