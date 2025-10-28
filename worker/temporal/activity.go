package temporal

import (
	"context"
	"errors"
	"fmt"

	"github.com/datazip-inc/olake-helm/worker/api"
	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/executor"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

type Activity struct {
	executor   executor.Executor
	tempClient client.Client
}

func NewActivity(e executor.Executor, c *Temporal) *Activity {
	return &Activity{executor: e, tempClient: c.GetClient()}
}

func (a *Activity) ExecuteActivity(ctx context.Context, req *types.ExecutionRequest) (*types.ExecutorResponse, error) {
	activityLogger := activity.GetLogger(ctx)
	activityLogger.Debug("executing", req.Command, "activity",
		"sourceType", req.ConnectorType,
		"version", req.Version,
		"workflowID", req.WorkflowID)

	activity.RecordHeartbeat(ctx, "executing %s activity", req.Command)
	req.HeartbeatFunc = activity.RecordHeartbeat

	return a.executor.Execute(ctx, req)
}

func (a *Activity) ExecuteSyncActivity(ctx context.Context, req *types.ExecutionRequest) (*types.ExecutorResponse, error) {
	activityLogger := activity.GetLogger(ctx)
	activityLogger.Debug("executing sync activity for job", "jobID", req.JobID, "workflowID", req.WorkflowID)

	// Update the configs with latest details from the server
	jobDetails, err := database.GetDB().GetJobData(ctx, req.JobID)
	if err != nil {
		errMsg := fmt.Sprintf("failed to get job data: %s", err)
		return nil, temporal.NewNonRetryableApplicationError(errMsg, "DatabaseError", err)
	}

	if req.Command == "" {
		utils.UpdateSyncRequest(jobDetails, req)
	}

	utils.UpdateConfigWithJobDetails(jobDetails, req)

	// Record heartbeat before execution
	activity.RecordHeartbeat(ctx, "executing sync for job %d", req.JobID)

	req.HeartbeatFunc = activity.RecordHeartbeat

	// Send telemetry event - "sync started"
	api.SendTelemetryEvents(req.JobID, req.WorkflowID, "started")

	result, err := a.executor.Execute(ctx, req)
	if err != nil {
		// CRITICAL: Check if error is because context was cancelled
		if ctx.Err() != nil {
			activityLogger.Info("sync activity cancelled", "jobID", req.JobID, "workflowID", req.WorkflowID)
			return nil, ctx.Err()
		}

		// execution failed
		if errors.Is(err, constants.ErrExecutionFailed) {
			api.SendTelemetryEvents(req.JobID, req.WorkflowID, "failed")
			return nil, temporal.NewNonRetryableApplicationError("execution failed", "ExecutionFailed", err)
		}

		activityLogger.Error("sync command failed", "error", err)
		api.SendTelemetryEvents(req.JobID, req.WorkflowID, "failed")
		return nil, temporal.NewNonRetryableApplicationError("execution failed", "ExecutionFailed", err)
	}

	return result, nil
}

func (a *Activity) SyncCleanupActivity(ctx context.Context, req *types.ExecutionRequest) error {
	activityLogger := activity.GetLogger(ctx)
	activityLogger.Info("cleaning up sync for job", "jobID", req.JobID, "workflowID", req.WorkflowID)

	jobDetails, err := database.GetDB().GetJobData(ctx, req.JobID)
	if err != nil {
		return err
	}

	if req.Command == "" {
		utils.UpdateSyncRequest(jobDetails, req)
	}

	if err := a.executor.SyncCleanup(ctx, req); err != nil {
		return temporal.NewNonRetryableApplicationError(err.Error(), "cleanup failed", err)
	}

	api.SendTelemetryEvents(req.JobID, req.WorkflowID, "completed")
	return nil
}

func (a *Activity) ClearCleanupActivity(ctx context.Context, req *executor.ExecutionRequest) error {
	activityLogger := activity.GetLogger(ctx)
	activityLogger.Info("cleaning up clear-destination for job", "jobID", req.JobID, "workflowID", req.WorkflowID)

	if err := a.executor.SyncCleanup(ctx, req); err != nil {
		activityLogger.Warn("cleanup warning (container cleanup failed)", "error", err)
	}

	// unpause sync schedule
	syncScheduleID := fmt.Sprintf("schedule-sync-%s-%d", req.ProjectID, req.JobID)
	handle := a.tempClient.ScheduleClient().GetHandle(ctx, syncScheduleID)
	_ = handle.Unpause(ctx, client.ScheduleUnpauseOptions{
		Note: "Clear destination cleanup completed",
	})

	return nil
}
