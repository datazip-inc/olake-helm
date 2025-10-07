package temporal

import (
	"fmt"
	"time"

	"github.com/datazip-inc/olake-helm/worker/executor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// Retry policy for non-sync activities (discover, test, spec, cleanup)
var DefaultRetryPolicy = &temporal.RetryPolicy{
	InitialInterval:    time.Second * 5,
	BackoffCoefficient: 2.0,
	MaximumInterval:    time.Minute * 5,
	MaximumAttempts:    1,
}

// Retry policy for sync activity with infinite retries
var SyncRetryPolicy = &temporal.RetryPolicy{
	InitialInterval:    time.Second * 5,
	BackoffCoefficient: 2.0,
	MaximumInterval:    time.Minute * 5,
	MaximumAttempts:    0,
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

func ExecuteSyncWorkflow(ctx workflow.Context, req *executor.ExecutionRequest) (result map[string]interface{}, err error) {
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: req.Timeout,
		RetryPolicy:         SyncRetryPolicy,
		HeartbeatTimeout:    time.Minute,
		WaitForCancellation: true,
	}

	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Each scheduled workflow execution gets a unique WorkflowID.
	// We're assigning that here to distinguish between different scheduled executions.
	req.WorkflowID = workflow.GetInfo(ctx).WorkflowExecution.ID

	// Defer cleanup - runs on both normal completion and cancellation
	defer func() {
		newCtx, _ := workflow.NewDisconnectedContext(ctx)
		cleanupOtions := workflow.ActivityOptions{
			StartToCloseTimeout: time.Minute * 15,
			RetryPolicy:         DefaultRetryPolicy,
		}
		newCtx = workflow.WithActivityOptions(newCtx, cleanupOtions)
		cleanupErr := workflow.ExecuteActivity(newCtx, "SyncCleanupActivity", req).Get(newCtx, nil)
		if cleanupErr != nil {
			if err != nil {
				err = fmt.Errorf("sync failed: %v, cleanup also failed: %v", err, cleanupErr)
			} else {
				err = fmt.Errorf("sync failed: %v", cleanupErr)
			}
		}
	}()

	err = workflow.ExecuteActivity(ctx, "ExecuteSyncActivity", req).Get(ctx, &result)
	return result, err
}
