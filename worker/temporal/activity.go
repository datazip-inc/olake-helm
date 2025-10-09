package temporal

import (
	"context"
	"errors"
	"fmt"
	"time"

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

	req.HeartbeatFunc = activity.RecordHeartbeat

	// Send telemetry event - "sync started"
	api.SendTelemetryEvents(req.JobID, req.WorkflowID, "started")

	type result struct {
		data map[string]interface{}
		err  error
	}
	done := make(chan result, 1)
	// executing sync in a goroutine to prevent blocking and monitoring the sync progress
	go func() {
		data, err := a.executor.Execute(ctx, req)
		done <- result{data: data, err: err}
	}()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			activityLogger.Info("SyncActivity canceled, deferring cleanup to SyncCleanupActivity")
			// TODO: add telemetry for cancel
			return nil, ctx.Err()

		case r := <-done:
			if r.err != nil {
				// CRITICAL: Check if error is because context was cancelled
				if ctx.Err() != nil {
					activityLogger.Info("Goroutine failed due to context cancellation", "executorError", r.err)
					api.SendTelemetryEvents(req.JobID, req.WorkflowID, "failed")
					return nil, ctx.Err()
				}

				// kubernetes executor specific error
				if errors.Is(r.err, constants.ErrPodFailed) {
					api.SendTelemetryEvents(req.JobID, req.WorkflowID, "failed")
					return nil, temporal.NewNonRetryableApplicationError("sync failed", "Pod failed", r.err)
				}

				activityLogger.Error("Sync command failed", "error", r.err)
				api.SendTelemetryEvents(req.JobID, req.WorkflowID, "failed")
				return r.data, r.err
			}
			return r.data, nil

		case <-ticker.C:
			activity.RecordHeartbeat(ctx, "sync in progress")
		}
	}
}

func (a *Activity) SyncCleanupActivity(ctx context.Context, req *executor.ExecutionRequest) error {
	activityLogger := activity.GetLogger(ctx)
	activityLogger.Info("Cleaning up sync for job", "jobID", req.JobID, "workflowID", req.WorkflowID)

	if err := a.executor.SyncCleanup(ctx, req); err != nil {
		return temporal.NewNonRetryableApplicationError(err.Error(), "cleanup failed", err)
	}

	api.SendTelemetryEvents(req.JobID, req.WorkflowID, "completed")
	return nil
}
