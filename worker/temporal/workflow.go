package temporal

import (
	"fmt"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	ExecuteActivity                 = "ExecuteActivity"
	SyncActivity                    = "SyncActivity"
	PostSyncActivity                = "PostSyncActivity"
	PostClearActivity               = "PostClearActivity"
	SendWebhookNotificationActivity = "SendWebhookNotificationActivity"
)

// Retry policy for non-sync activities (discover, test, spec, cleanup)
var (
	DefaultRetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    time.Second * 5,
		BackoffCoefficient: 2.0,
		MaximumInterval:    time.Minute * 5,
		MaximumAttempts:    1,
	}

	SyncRetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    time.Second * 5,
		BackoffCoefficient: 2.0,
		MaximumInterval:    time.Minute * 5,
		MaximumAttempts:    0,
	}
)

func ExecuteWorkflow(ctx workflow.Context, req *types.ExecutionRequest) (*types.ExecutorResponse, error) {
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: req.Timeout,
		RetryPolicy:         DefaultRetryPolicy,
	}

	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	var result *types.ExecutorResponse
	if err := workflow.ExecuteActivity(ctx, ExecuteActivity, req).Get(ctx, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// RunSyncWorkflow is a Temporal workflow that orchestrates long-running data operations:
//
// Supported Commands:
//   - Sync: Performs data replication from source to destination
//   - ClearDestination: Clears data from the destination
//
// Features:
//   - Infinite retries (MaximumAttempts: 0) with exponential backoff for transient errors
//   - Heartbeat monitoring (30s) to detect worker failures
//   - Graceful cleanup via deferred activity (runs even on cancellation)
//
// HeartbeatTimeout: 30 seconds
// Heartbeats are throttled at timeout * 0.8 = 24s intervals.
// Faster heartbeats enable quicker cancellation detection and worker failure recovery.
func RunSyncWorkflow(ctx workflow.Context, args interface{}) (result *types.ExecutorResponse, err error) {
	workflowLogger := workflow.GetLogger(ctx)
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: constants.DefaultSyncTimeout,
		HeartbeatTimeout:    30 * time.Second,
		WaitForCancellation: true,
		RetryPolicy:         SyncRetryPolicy,
	}

	req, err := utils.BuildSyncReqForLegacyOrNew(args)
	if err != nil {
		return nil, err
	}

	ctx = workflow.WithActivityOptions(ctx, activityOptions)
	req.WorkflowID = workflow.GetInfo(ctx).WorkflowExecution.ID

	var activity, cleanupActivity string
	switch req.Command {
	case types.Sync:
		activity, cleanupActivity = SyncActivity, PostSyncActivity
	case types.ClearDestination:
		activity, cleanupActivity = ExecuteActivity, PostClearActivity
	default:
		return nil, fmt.Errorf("invalid command: %s", req.Command)
	}

	// Defer cleanup - runs on both normal completion and cancellation
	defer func() {
		newCtx, _ := workflow.NewDisconnectedContext(ctx)
		cleanupOtions := workflow.ActivityOptions{
			StartToCloseTimeout: time.Minute * 15,
			RetryPolicy:         SyncRetryPolicy,
		}
		newCtx = workflow.WithActivityOptions(newCtx, cleanupOtions)
		cleanupErr := workflow.ExecuteActivity(newCtx, cleanupActivity, req).Get(newCtx, nil)
		if cleanupErr != nil {
			cleanupErr = fmt.Errorf("cleanup failed: %s", cleanupErr)
			if err != nil {
				err = fmt.Errorf("sync failed: %s, cleanup also failed: %s", err, cleanupErr)
			}
		}
	}()

	// set search attributes to differentiate between sync and clear operation
	opTypeKey := temporal.NewSearchAttributeKeyKeyword(constants.OperationTypeKey)
	if err := workflow.UpsertTypedSearchAttributes(ctx, opTypeKey.ValueSet(string(req.Command))); err != nil {
		workflowLogger.Error("failed to upsert search attributes", "error", err)
	}

	err = workflow.ExecuteActivity(ctx, activity, req).Get(ctx, &result)
	if err != nil {
		// Skip webhook for cancellations
		if temporal.IsCanceledError(err) {
			workflowLogger.Info("sync workflow cancelled, skipping webhook notification", "jobID", req.JobID)
			return nil, err
		}

		// Create a separate short-lived context for webhook alert
		// use disconnected context to avoid blocking the main workflow
		disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
		webhookCtx := workflow.WithActivityOptions(disconnectedCtx, workflow.ActivityOptions{
			StartToCloseTimeout: time.Minute * 1,
			RetryPolicy:         DefaultRetryPolicy, // only one retry
		})
		// Trigger webhook alert asynchronously
		lastRunTime := workflow.Now(ctx)
		webhookArgs := types.WebhookNotificationArgs{
			JobID:        req.JobID,
			ProjectID:    req.ProjectID,
			LastRunTime:  lastRunTime,
			ErrorMessage: err.Error(),
		}
		workflow.ExecuteActivity(webhookCtx, SendWebhookNotificationActivity, webhookArgs)
		return nil, err

	}
	return result, err
}
