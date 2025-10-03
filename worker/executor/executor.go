package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/spf13/viper"
)

type Executor interface {
	Execute(ctx context.Context, req *ExecutionRequest) (map[string]interface{}, error)
	Close() error
}

type ExecutionRequest struct {
	Type          string            `json:"type"`
	Command       types.Command     `json:"command"`
	ConnectorType string            `json:"connector_type"`
	Version       string            `json:"version"`
	Args          []string          `json:"args"`
	Configs       []types.JobConfig `json:"configs"`
	WorkflowID    string            `json:"workflow_id"`
	JobID         int               `json:"job_id"`
	Timeout       time.Duration     `json:"timeout"`
	OutputFile    string            `json:"output_file"`
}

type ExecutorEnvironment string

const (
	Kubernetes ExecutorEnvironment = "kubernetes"
	Docker     ExecutorEnvironment = "docker"
)

type NewFunc func(db *database.DB) (Executor, error)

var RegisteredExecutors = map[ExecutorEnvironment]NewFunc{}

func NewExecutor(db *database.DB) (Executor, error) {
	executorEnv := viper.GetString(constants.EnvExecutorEnvironment)
	if executorEnv == "" {
		return nil, fmt.Errorf("executor environment is not set")
	}
	newFunc, ok := RegisteredExecutors[ExecutorEnvironment(executorEnv)]
	if !ok {
		return nil, fmt.Errorf("invalid executor environment: %s", executorEnv)
	}
	return newFunc(db)
}
