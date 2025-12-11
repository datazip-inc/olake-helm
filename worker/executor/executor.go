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
func NewExecutor(ctx context.Context, db *database.DB) (*AbstractExecutor, error) {
	executorEnv := utils.GetExecutorEnvironment()

	var exec Executor
	var err error

	switch executorEnv {
	case string(types.Docker):
		exec, err = docker.NewDockerExecutor()
	case string(types.Kubernetes):
		exec, err = kubernetes.NewKubernetesExecutor(ctx)
	default:
		exec, err = nil, fmt.Errorf("invalid executor environment: %s", executorEnv)
	}
	if err != nil {
		return nil, err
	}
	return &AbstractExecutor{executor: exec, db: db}, nil
}

func (a *AbstractExecutor) Execute(ctx context.Context, req *types.ExecutionRequest) (*types.ExecutorResponse, error) {
	log := logger.Log(ctx)
	subdir, workdir := utils.GetWorkflowDirAndSubDir(req.WorkflowID, req.Command)

	// write config files only for the first/scheduled workflow execution (not for retries)
	if !utils.WorkflowAlreadyLaunched(workdir) && req.Configs != nil {
		if err := utils.WriteConfigFiles(workdir, req.Configs); err != nil {
			log.Error("failed to write config files", "workdir", workdir, "error", err)
			return nil, err
		}
	}

	output, err := a.executor.Execute(ctx, req, workdir)
	if err != nil {
		log.Error("executor failed", "command", req.Command, "error", err)
		return nil, err
	}
	if req.Command != types.Sync {
		log.Info("executor output", "environment", utils.GetExecutorEnvironment(), "output", output)
	}

	// generated file as response
	if req.OutputFile != "" {
		filePath := filepath.Join(subdir, req.OutputFile)
		return &types.ExecutorResponse{Response: filePath}, nil
	}

	outputJSON, err := utils.ExtractJSONAndMarshal(output)
	if err != nil {
		log.Error("failed to extract JSON from output", "error", err)
		return nil, err
	}

	outputPath := filepath.Join(workdir, constants.OutputFileName)
	if err := utils.WriteFile(outputPath, outputJSON); err != nil {
		log.Error("failed to write output file", "path", outputPath, "error", err)
		return nil, err
	}

	// logs as response
	return &types.ExecutorResponse{Response: filepath.Join(subdir, constants.OutputFileName)}, nil
}

// CleanupAndPersistState stops the container/pod and saves the state file in the database
func (a *AbstractExecutor) CleanupAndPersistState(ctx context.Context, req *types.ExecutionRequest) error {
	log := logger.Log(ctx)

	if err := a.executor.Cleanup(ctx, req); err != nil {
		log.Error("failed to cleanup executor", "workflowID", req.WorkflowID, "error", err)
		return err
	}

	stateFile, err := utils.GetStateFileFromWorkdir(req.WorkflowID, req.Command)
	if err != nil {
		log.Error("failed to read state file", "workflowID", req.WorkflowID, "error", err)
		return err
	}

	if err := a.db.UpdateJobState(ctx, req.JobID, stateFile); err != nil {
		log.Error("failed to update job state in database", "jobID", req.JobID, "error", err)
		return err
	}

	log.Info("successfully cleaned up and persisted state", "jobID", req.JobID)
	return nil
}

func (a *AbstractExecutor) Close() {
	a.executor.Close()
}
