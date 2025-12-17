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
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"github.com/datazip-inc/olake-helm/worker/utils/notifications"
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
	log := logger.Log(ctx)
	log.Info("executing activity",
		"command", req.Command,
		"sourceType", req.ConnectorType,
		"version", req.Version,
		"workflowID", req.WorkflowID,
	)

	activity.RecordHeartbeat(ctx, "executing %s activity", req.Command)
	req.HeartbeatFunc = activity.RecordHeartbeat

	if req.Command == types.ClearDestination {
		jobDetails, err := a.db.GetJobData(ctx, req.JobID)
		if err != nil {
			return nil, err
		}

		if err := utils.UpdateConfigForClearDestination(jobDetails, req); err != nil {
			return nil, err
		}
	}

	return a.executor.Execute(ctx, req)
}

func (a *Activity) SyncActivity(ctx context.Context, req *types.ExecutionRequest) (*types.ExecutorResponse, error) {
	log := logger.Log(ctx)
	log.Info("executing sync activity", "jobID", req.JobID)

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
	telemetry.SendEvent(req.JobID, utils.GetExecutorEnvironment(), req.WorkflowID, telemetry.TelemetryEventStarted)

	result, err := a.executor.Execute(ctx, req)
	if err != nil {
		// CRITICAL: Check if error is because context was cancelled
		if ctx.Err() != nil {
			log.Info("sync activity cancelled", "jobID", req.JobID)
			return nil, temporal.NewCanceledError("sync activity cancelled")
		}

		if errors.Is(err, constants.ErrExecutionFailed) {
			telemetry.SendEvent(req.JobID, utils.GetExecutorEnvironment(), req.WorkflowID, telemetry.TelemetryEventFailed)
			return nil, temporal.NewNonRetryableApplicationError("execution failed", "ExecutionFailed", err)
		}

		log.Error("sync command failed", "error", err)
		telemetry.SendEvent(req.JobID, utils.GetExecutorEnvironment(), req.WorkflowID, telemetry.TelemetryEventFailed)
		return nil, temporal.NewNonRetryableApplicationError("execution failed", "ExecutionFailed", err)
	}

	return result, nil
}

func (a *Activity) PostSyncActivity(ctx context.Context, req *types.ExecutionRequest) error {
	log := logger.Log(ctx)
	log.Info("cleaning up sync for job", "jobID", req.JobID)

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

	telemetry.SendEvent(req.JobID, utils.GetExecutorEnvironment(), req.WorkflowID, telemetry.TelemetryEventCompleted)
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
	log := logger.Log(ctx)
	log.Info("cleaning up clear-destination for job", "jobID", req.JobID)

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

			if input.Description.Schedule.State != nil {
				input.Description.Schedule.State.Paused = false
				input.Description.Schedule.State.Note = "Restored to sync after clear-destination"
			}

			return &client.ScheduleUpdate{
				Schedule: &input.Description.Schedule,
			}, nil
		},
	})
	if err != nil {
		log.Error("failed to update schedule", "jobID", req.JobID, "scheduleID", scheduleID, "error", err)
		return err
	}

	// Verify the schedule is actually unpaused
	desc, err := handle.Describe(ctx)
	if err != nil {
		log.Error("failed to describe schedule after update", "jobID", req.JobID, "scheduleID", scheduleID, "error", err)
		return err
	}
	if desc.Schedule.State.Paused {
		log.Error("schedule still paused after update", "jobID", req.JobID, "scheduleID", scheduleID)
		return fmt.Errorf("schedule %s, jobID: %d still paused after update", scheduleID, req.JobID)
	}

	log.Info("successfully updated schedule (clear-destination to sync)", "jobID", req.JobID, "scheduleID", scheduleID)

	return nil
}

func (a *Activity) SendWebhookNotificationActivity(ctx context.Context, req types.WebhookNotificationArgs) error {
	log := logger.Log(ctx)
	log.Info("Sending webhook alert", "jobID", req.JobID, "projectID", req.ProjectID)

	projectID := req.ProjectID
	if projectID == "" {
		// TODO: introduce a dedicated migration to backfill project_id into schedules for older jobs and remove this hardcoded fallback.
		projectID = "123"
		log.Info("project_id is empty, defaulting to fallback project_id", "jobID", req.JobID, "fallbackProjectID", projectID)
	}

	settings, err := a.db.GetProjectSettingsByProjectID(ctx, projectID)
	if err != nil {
		return fmt.Errorf("failed to get project settings: %w", err)
	}

	jobDetails, err := a.db.GetJobData(ctx, req.JobID)
	if err != nil {
		log.Warn("failed to get job data for webhook notification", "jobID", req.JobID, "error", err)
	}
	jobName := jobDetails.JobName

	if err := notifications.SendWebhookNotification(ctx, req, jobName, settings.WebhookAlertURL); err != nil {
		return fmt.Errorf("failed to send webhook notification: %w", err)
	}
	return nil
}
