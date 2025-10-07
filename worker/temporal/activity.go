package temporal

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake-helm/worker/api"
	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/executor"
	"github.com/datazip-inc/olake-helm/worker/logger"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"go.temporal.io/sdk/activity"
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

	return a.executor.Execute(ctx, req)
}

func (a *Activity) ExecuteSyncActivity(ctx context.Context, req *executor.ExecutionRequest) (map[string]interface{}, error) {
	activityLogger := activity.GetLogger(ctx)
	activityLogger.Debug("Executing sync activity for job", "jobID", req.JobID, "workflowID", req.WorkflowID)

	// Update the configs with latest details from the server
	jobDetails, err := database.GetDB().GetJobData(req.JobID)
	if err != nil {
		logger.Errorf("Failed to get job details: %v", err)
		return nil, fmt.Errorf("failed to get job details: %v", err)
	}

	if err := utils.UpdateConfigWithJobDetails(jobDetails, req); err != nil {
		logger.Errorf("Failed to update config with job details: %v", err)
		return nil, fmt.Errorf("failed to update config with job details: %v", err)
	}

	// Record heartbeat before execution
	activity.RecordHeartbeat(ctx, "Executing sync for job %d", req.JobID)

	// Send telemetry event - "sync started"
	api.SendTelemetryEvents(req.JobID, req.WorkflowID, "started")

	// Execute the sync operation
	result, err := a.executor.Execute(ctx, req)
	if err != nil {
		logger.Errorf("Sync execution failed: %v", err)
		// Send telemetry event - "sync failed"
		api.SendTelemetryEvents(req.JobID, req.WorkflowID, "failed")
		return nil, err
	}

	// Extract and validate the new state file
	newStateFile, ok := result["response"].(string)
	if !ok {
		logger.Errorf("Invalid response format from worker: %+v", result)
		api.SendTelemetryEvents(req.JobID, req.WorkflowID, "failed")
		return nil, fmt.Errorf("invalid response format from worker")
	}

	// Update the state file with the new state from response
	if err := database.GetDB().UpdateJobState(req.JobID, newStateFile, true); err != nil {
		logger.Errorf("Failed to update state file: %v", err)
		api.SendTelemetryEvents(req.JobID, req.WorkflowID, "failed")
		return nil, fmt.Errorf("failed to update state file: %v", err)
	}

	// Send telemetry event - "sync completed"
	api.SendTelemetryEvents(req.JobID, req.WorkflowID, "completed")

	return result, nil
}
