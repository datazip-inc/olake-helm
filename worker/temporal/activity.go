package temporal

import (
	"context"

	"github.com/datazip-inc/olake-helm/worker/executor"
	"go.temporal.io/sdk/activity"
)

type Activity struct {
	executor executor.Executor
}

func NewActivity(e executor.Executor) *Activity {
	return &Activity{executor: e}
}

func (a *Activity) ExecuteActivity(ctx context.Context, req *executor.ExecutionRequest) (map[string]interface{}, error) {
	activityLogger := activity.GetLogger(ctx)
	activityLogger.Info("Executing %s activity", req.Command)
	// Record Activity
	activity.RecordHeartbeat(ctx, "Executing %s activity", req.Command)
	return a.executor.Execute(ctx, req)
}
