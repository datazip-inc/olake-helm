package executor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
)

type Executor interface {
	Execute(ctx context.Context, req *ExecutionRequest) (map[string]interface{}, error)
	Close() error
}

type NewFunc func() (Executor, error)

var RegisteredExecutors = map[ExecutorEnvironment]NewFunc{}

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

func NewExecutor() (Executor, error) {
	executorEnv := utils.GetExecutorEnvironment()
	newFunc, ok := RegisteredExecutors[ExecutorEnvironment(executorEnv)]
	if !ok {
		return nil, fmt.Errorf("invalid executor environment: %s", executorEnv)
	}
	return newFunc()
}

// Singleton instance of the executor
var (
	execOnce     sync.Once
	execInstance Executor
	execErr      error
)

func GetExecutor() (Executor, error) {
	execOnce.Do(func() {
		execInstance, execErr = NewExecutor()
	})
	return execInstance, execErr
}

func CloseExecutor() error {
	return execInstance.Close()
}
