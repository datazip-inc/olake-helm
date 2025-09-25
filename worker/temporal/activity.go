package temporal

import (
	"context"

	"github.com/datazip-inc/olake-helm/worker/executor"
)

func ExecuteActivity(ctx context.Context, req *executor.ExecutionRequest) (map[string]interface{}, error) {
	exec, err := executor.GetExecutor()
	if err != nil {
		return nil, err
	}
	return exec.Execute(ctx, req)
}
