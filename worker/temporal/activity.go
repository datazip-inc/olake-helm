package temporal

import (
	"context"
	"errors"
	"fmt"

	"github.com/datazip-inc/olake-helm/worker/api"
	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/executor"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

type Activity struct {
	executor executor.Executor
}

func NewActivity(e executor.Executor) *Activity {
	return &Activity{executor: e}
}

func (a *Activity) ExecuteActivity(ctx context.Context, req *executor.ExecutionRequest) (map[string]interface{}, error) {
	activityLogger := activity.GetLogger(ctx)
	activityLogger.Debug("Executing", req.Command, "activity",
		"sourceType", req.ConnectorType,
		"version", req.Version,
		"workflowID", req.WorkflowID)

	activity.RecordHeartbeat(ctx, "Executing %s activity", req.Command)

	if utils.GetExecutorEnvironment() == string(executor.Kubernetes) {
		req.HeartbeatFunc = activity.RecordHeartbeat
	}

	return a.executor.Execute(ctx, req)
}

func (a *Activity) ExecuteSyncActivity(ctx context.Context, req *executor.ExecutionRequest) (map[string]interface{}, error) {
	activityLogger := activity.GetLogger(ctx)
	activityLogger.Debug("Executing sync activity for job", "jobID", req.JobID, "workflowID", req.WorkflowID)

	// Update the configs with latest details from the server
	jobDetails, err := database.GetDB().GetJobData(req.JobID)
	if err != nil {
		errMsg := fmt.Sprintf("failed to get job data: %v", err)
		return nil, temporal.NewNonRetryableApplicationError(errMsg, "DatabaseError", err)
	}

	if err := utils.UpdateConfigWithJobDetails(jobDetails, req); err != nil {
		return nil, fmt.Errorf("failed to update config with job details: %v", err)
	}

	// Record heartbeat before execution
	activity.RecordHeartbeat(ctx, "Executing sync for job %d", req.JobID)

	if utils.GetExecutorEnvironment() == string(executor.Kubernetes) {
		req.HeartbeatFunc = activity.RecordHeartbeat
	}

	// Send telemetry event - "sync started"
	api.SendTelemetryEvents(req.JobID, req.WorkflowID, "started")

	// Execute the sync operation
	result, err := a.executor.Execute(ctx, req)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			// TODO: add telemetry for cancel
			return nil, temporal.NewCanceledError("sync cancelled")
		}
		if errors.Is(err, constants.ErrPodFailed) {
			// Send telemetry event - "sync failed"
			api.SendTelemetryEvents(req.JobID, req.WorkflowID, "failed")
			return nil, temporal.NewNonRetryableApplicationError("sync failed", "Pod failed", err)
		}
		api.SendTelemetryEvents(req.JobID, req.WorkflowID, "failed")
		return result, err
	}
	return result, nil
}

func (a *Activity) SyncCleanupActivity(ctx context.Context, req *executor.ExecutionRequest) error {
	activityLogger := activity.GetLogger(ctx)
	activityLogger.Info("Cleaning up sync for job", "jobID", req.JobID, "workflowID", req.WorkflowID)

	return a.executor.SyncCleanup(ctx, req)
}
