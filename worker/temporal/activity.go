package temporal

import (
	"context"
	"errors"
	"fmt"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/executor"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/telemetry"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

type Activity struct {
	executor   *executor.AbstractExecutor
	db         *database.DB
	tempClient client.Client
}

func NewActivity(e *executor.AbstractExecutor, db *database.DB, c *Temporal) *Activity {
	return &Activity{executor: e, db: db, tempClient: c.GetClient()}
}

func (a *Activity) ExecuteActivity(ctx context.Context, req *types.ExecutionRequest) (*types.ExecutorResponse, error) {
	activityLogger := activity.GetLogger(ctx)
	activityLogger.Debug("executing", req.Command, "activity",
		"sourceType", req.ConnectorType,
		"version", req.Version,
		"workflowID", req.WorkflowID)

	activity.RecordHeartbeat(ctx, "executing %s activity", req.Command)
	req.HeartbeatFunc = activity.RecordHeartbeat

	result, err := a.executor.Execute(ctx, req)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (a *Activity) SyncActivity(ctx context.Context, req *types.ExecutionRequest) (*types.ExecutorResponse, error) {
	activityLogger := activity.GetLogger(ctx)
	activityLogger.Debug("executing sync activity for job", "jobID", req.JobID)

	// Record heartbeat before execution
	activity.RecordHeartbeat(ctx, "executing sync for job %d", req.JobID)
	req.HeartbeatFunc = activity.RecordHeartbeat

	// Update the configs with latest
	jobDetails, err := a.db.GetJobData(ctx, req.JobID)
	if err != nil {
		errMsg := fmt.Sprintf("failed to get job data: %s", err)
		return nil, temporal.NewNonRetryableApplicationError(errMsg, "DatabaseError", err)
	}

	// mapping request type of deprecated workflow to new request type
	// old scheduled sync workflow has no connector type set
	if req.ConnectorType == "" {
		utils.UpdateSyncRequestForLegacy(jobDetails, req)
	}

	// update the configs with latest job details
	utils.UpdateConfigWithJobDetails(jobDetails, req)

	// Send telemetry event - "sync started"
	telemetry.SendEvent(req.JobID, req.WorkflowID, "started")

	result, err := a.executor.Execute(ctx, req)
	if err != nil {
		// CRITICAL: Check if error is because context was cancelled
		if ctx.Err() != nil {
			activityLogger.Info("sync activity cancelled", "jobID", req.JobID)
			return nil, temporal.NewCanceledError("sync activity cancelled")
		}

		if errors.Is(err, constants.ErrExecutionFailed) {
			telemetry.SendEvent(req.JobID, req.WorkflowID, "failed")
			return nil, temporal.NewNonRetryableApplicationError("execution failed", "ExecutionFailed", err)
		}

		activityLogger.Error("sync command failed", "error", err)
		telemetry.SendEvent(req.JobID, req.WorkflowID, "failed")
		return nil, temporal.NewNonRetryableApplicationError("execution failed", "ExecutionFailed", err)
	}

	return result, nil
}

func (a *Activity) PostSyncActivity(ctx context.Context, req *types.ExecutionRequest) error {
	activityLogger := activity.GetLogger(ctx)
	activityLogger.Info("cleaning up sync for job", "jobID", req.JobID)

	jobDetails, err := a.db.GetJobData(ctx, req.JobID)
	if err != nil {
		return err
	}

	if req.ConnectorType == "" {
		utils.UpdateSyncRequestForLegacy(jobDetails, req)
	}

	if err := a.executor.CleanupAndPersistState(ctx, req); err != nil {
		return temporal.NewNonRetryableApplicationError(err.Error(), "cleanup failed", err)
	}

	telemetry.SendEvent(req.JobID, req.WorkflowID, "completed")
	return nil
}

// CRITICAL: Restore the schedule to its normal sync operation state
//
// When clear-destination is triggered, the backend (olake-ui) temporarily:
// 1. Updates the sync schedule's metadata to run clear-destination instead
// 2. Pauses the schedule to prevent the next scheduled run during the operation
//
// After clear-destination completes (success or failure), we must restore the schedule:
// 1. Revert metadata back to sync operation
// 2. Unpause the schedule to resume normal operations
//
// Without these steps, the schedule would remain paused and stuck in clear-destination mode,
// preventing all future sync runs.
func (a *Activity) PostClearActivity(ctx context.Context, req *types.ExecutionRequest) error {
	activityLogger := activity.GetLogger(ctx)
	activityLogger.Info("cleaning up clear-destination for job", "jobID", req.JobID)

	if err := a.executor.CleanupAndPersistState(ctx, req); err != nil {
		return err
	}

	utils.RevertUpdatesInSchedule(req)

	// update the schedule
	workflowID := fmt.Sprintf("sync-%s-%d", req.ProjectID, req.JobID)
	scheduleID := fmt.Sprintf("schedule-%s", workflowID)
	handle := a.tempClient.ScheduleClient().GetHandle(ctx, scheduleID)

	err := handle.Update(ctx, client.ScheduleUpdateOptions{
		DoUpdate: func(input client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
			input.Description.Schedule.Action = &client.ScheduleWorkflowAction{
				ID:        workflowID,
				Workflow:  RunSyncWorkflow,
				Args:      []any{req},
				TaskQueue: constants.TaskQueue,
			}
			return &client.ScheduleUpdate{
				Schedule: &input.Description.Schedule,
			}, nil
		},
	})
	if err != nil {
		activityLogger.Error("failed to update schedule action", "error", err)
		return err
	}
	activityLogger.Debug("updated schedule action to sync for job", "jobID", req.JobID, "scheduleID", scheduleID)

	// unpause schedule
	err = handle.Unpause(ctx, client.ScheduleUnpauseOptions{
		Note: "resumed schedule after clear-destination",
	})
	if err != nil {
		activityLogger.Error("failed to unpause schedule", "error", err)
		return err
	}
	activityLogger.Debug("resumed schedule for job", "jobID", req.JobID, "scheduleID", scheduleID)

	return nil
}
