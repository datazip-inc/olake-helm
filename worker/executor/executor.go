package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
)

type Executor interface {
	Execute(ctx context.Context, req *ExecutionRequest) (map[string]interface{}, error)
}

type NewFunc func() (Executor, error)

var RegisteredExecutors = map[ExecutorEnvironment]NewFunc{}

type ExecutionRequest struct {
	Type          ExecutorEnvironment
	Command       string
	ConnectorType string
	Version       string
	Args          []string
	Configs       []types.JobConfig
	WorkflowID    string
	JobID         int
	Timeout       time.Duration
}

type ExecutorEnvironment string

const (
	Kubernetes ExecutorEnvironment = "kubernetes"
	Docker     ExecutorEnvironment = "docker"
)

func NewExecutor() (Executor, error) {
	executorEnv := utils.GetEnv(constants.EnvExecutorEnvironment, constants.DefaultExecutorEnvironment)
	newFunc, ok := RegisteredExecutors[ExecutorEnvironment(executorEnv)]
	if !ok {
		return nil, fmt.Errorf("invalid executor environment: %s", executorEnv)
	}
	return newFunc()
}
