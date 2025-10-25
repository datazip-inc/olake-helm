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
		HeartbeatTimeout:    time.Minute,
		WaitForCancellation: true,
		// Sync workflows are critical and should not stop on transient errors.
		// Setting MaximumAttempts to 0 means infinite retries with exponential backoff.
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second * 5,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute * 5,
			MaximumAttempts:    0,
		},
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
				err = fmt.Errorf("sync failed: %s, cleanup also failed: %s", err, cleanupErr)
			} else {
				err = fmt.Errorf("cleanup failed: %s", cleanupErr)
			}
		}
	}()

	err = workflow.ExecuteActivity(ctx, "ExecuteSyncActivity", req).Get(ctx, &result)
	return result, err
}

func ExecuteClearWorkflow(ctx workflow.Context, req *executor.ExecutionRequest) (err error) {
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: req.Timeout,
		HeartbeatTimeout:    time.Minute,
		WaitForCancellation: true,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second * 5,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute * 5,
			MaximumAttempts:    1,
		},
	}

	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	defer func() {
		newCtx, _ := workflow.NewDisconnectedContext(ctx)
		cleanupOtions := workflow.ActivityOptions{
			StartToCloseTimeout: time.Minute * 15,
			RetryPolicy:         DefaultRetryPolicy,
		}
		newCtx = workflow.WithActivityOptions(newCtx, cleanupOtions)
		cleanupErr := workflow.ExecuteActivity(newCtx, "ClearCleanupActivity", req).Get(newCtx, nil)
		if cleanupErr != nil {
			if err != nil {
				err = fmt.Errorf("clear failed: %s, cleanup also failed: %s", err, cleanupErr)
			} else {
				err = fmt.Errorf("cleanup failed: %s", cleanupErr)
			}
		}
	}()

	err = workflow.ExecuteActivity(ctx, "ExecuteActivity", req).Get(ctx, nil)
	return err
}
