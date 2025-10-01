package temporal

import (
	"context"

	"github.com/datazip-inc/olake-helm/worker/executor"
	"go.temporal.io/sdk/activity"
)

func ExecuteActivity(ctx context.Context, req *executor.ExecutionRequest) (map[string]interface{}, error) {
	activityLogger := activity.GetLogger(ctx)
	activityLogger.Info("Executing %s activity", req.Command)
	exec, err := executor.GetExecutor()
	if err != nil {
		return nil, err
	}
	return exec.Execute(ctx, req)
}
