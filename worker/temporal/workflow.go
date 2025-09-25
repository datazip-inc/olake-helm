package temporal

import (
	"time"

	"github.com/datazip-inc/olake-helm/worker/executor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

var DefaultRetryPolicy = &temporal.RetryPolicy{
	InitialInterval:    time.Second * 15,
	BackoffCoefficient: 2.0,
	MaximumInterval:    time.Minute * 10,
	MaximumAttempts:    1,
}

// TODO: Check if we can follow the current approach or have separate workflows and a single activity
func ExecuteWorkflow(ctx workflow.Context, req *executor.ExecutionRequest) (map[string]interface{}, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: req.Timeout,
		RetryPolicy:         DefaultRetryPolicy,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	// Each scheduled workflow execution gets a unique WorkflowID.
	// We're assigning that here to distinguish between different scheduled executions.
	req.WorkflowID = workflow.GetInfo(ctx).WorkflowExecution.ID

	var result map[string]interface{}
	if err := workflow.ExecuteActivity(ctx, "ExecuteActivity", req).Get(ctx, &result); err != nil {
		return nil, err
	}
	return result, nil
}
