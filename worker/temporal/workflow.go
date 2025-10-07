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

// QUESTION: Is the single workflow approach fine or should we create separate workflows
func ExecuteWorkflow(ctx workflow.Context, req *executor.ExecutionRequest) (map[string]interface{}, error) {
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: req.Timeout,
		RetryPolicy:         DefaultRetryPolicy,
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	var result map[string]interface{}
	if err := workflow.ExecuteActivity(ctx, "ExecuteActivity", req).Get(ctx, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func ExecuteSyncWorkflow(ctx workflow.Context, req *executor.ExecutionRequest) (map[string]interface{}, error) {
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: req.Timeout,
		RetryPolicy:         DefaultRetryPolicy,
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Each scheduled workflow execution gets a unique WorkflowID.
	// We're assigning that here to distinguish between different scheduled executions.
	req.WorkflowID = workflow.GetInfo(ctx).WorkflowExecution.ID

	var result map[string]interface{}
	if err := workflow.ExecuteActivity(ctx, "ExecuteSyncActivity", req).Get(ctx, &result); err != nil {
		return nil, err
	}

	return result, nil
}
