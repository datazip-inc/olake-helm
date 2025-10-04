package workflows

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/datazip-inc/olake-ui/olake-workers/k8s/logger"
	"github.com/datazip-inc/olake-ui/olake-workers/k8s/shared"
	"github.com/datazip-inc/olake-ui/olake-workers/k8s/utils/helpers"
)

// Retry policy for non-sync activities (discover, test, spec, cleanup)
var DefaultRetryPolicy = &temporal.RetryPolicy{
	InitialInterval:    time.Second * 5,
	BackoffCoefficient: 2.0,
	MaximumInterval:    time.Minute * 5,
	MaximumAttempts:    1,
}

// Retry policy for sync activity with unlimited retries
var SyncRetryPolicy = &temporal.RetryPolicy{
	InitialInterval:    time.Second * 5,
	BackoffCoefficient: 2.0,
	MaximumInterval:    time.Minute * 5,
	MaximumAttempts:    0,
}

// DiscoverCatalogWorkflow is a workflow for discovering catalogs using K8s Jobs
func DiscoverCatalogWorkflow(ctx workflow.Context, params *shared.ActivityParams) (map[string]interface{}, error) {
	options := workflow.ActivityOptions{
		StartToCloseTimeout: helpers.GetActivityTimeout("discover"),
		RetryPolicy:         DefaultRetryPolicy,
	}
	ctx = workflow.WithActivityOptions(ctx, options)

	var result map[string]interface{}
	err := workflow.ExecuteActivity(ctx, "DiscoverCatalogActivity", params).Get(ctx, &result)
	return result, err
}

// TestConnectionWorkflow is a workflow for testing connections using K8s Jobs
func TestConnectionWorkflow(ctx workflow.Context, params *shared.ActivityParams) (map[string]interface{}, error) {
	options := workflow.ActivityOptions{
		StartToCloseTimeout: helpers.GetActivityTimeout("test"),
		RetryPolicy:         DefaultRetryPolicy,
	}
	ctx = workflow.WithActivityOptions(ctx, options)

	var result map[string]interface{}
	err := workflow.ExecuteActivity(ctx, "TestConnectionActivity", params).Get(ctx, &result)
	return result, err
}

// RunSyncWorkflow is a workflow for running data synchronization using K8s Jobs
func RunSyncWorkflow(ctx workflow.Context, jobID int) (map[string]interface{}, error) {
	options := workflow.ActivityOptions{
		StartToCloseTimeout: helpers.GetActivityTimeout("sync"),
		HeartbeatTimeout:    time.Minute,
		RetryPolicy:         SyncRetryPolicy,
		WaitForCancellation: true,
	}
	params := shared.SyncParams{
		JobID:      jobID,
		WorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID,
	}

	ctx = workflow.WithActivityOptions(ctx, options)
	var result map[string]interface{}

	// Defer cleanup - runs on both normal completion and cancellation
	defer func() {
		newCtx, _ := workflow.NewDisconnectedContext(ctx)
		cleanupOptions := workflow.ActivityOptions{
			StartToCloseTimeout: time.Minute * 15,
			RetryPolicy:         DefaultRetryPolicy,
		}
		newCtx = workflow.WithActivityOptions(newCtx, cleanupOptions)
		err := workflow.ExecuteActivity(newCtx, "SyncCleanupActivity", params).Get(newCtx, nil)
		if err != nil {
			logger.Errorf("SyncCleanupActivity failed: %v", err)
		}
	}()

	err := workflow.ExecuteActivity(ctx, "SyncActivity", params).Get(ctx, &result)
	return result, err
}

// FetchSpecWorkflow is a workflow for fetching connector specifications using K8s Jobs
func FetchSpecWorkflow(ctx workflow.Context, params *shared.ActivityParams) (map[string]interface{}, error) {
	options := workflow.ActivityOptions{
		StartToCloseTimeout: helpers.GetActivityTimeout("spec"),
		RetryPolicy:         DefaultRetryPolicy,
	}
	ctx = workflow.WithActivityOptions(ctx, options)

	var result map[string]interface{}
	err := workflow.ExecuteActivity(ctx, "FetchSpecActivity", params).Get(ctx, &result)
	return result, err
}
