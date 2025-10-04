package temporal

import (
	"fmt"
	"time"

	"github.com/datazip-inc/olake-helm/worker/api"
	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/executor"
	"github.com/datazip-inc/olake-helm/worker/logger"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

var DefaultRetryPolicy = &temporal.RetryPolicy{
	InitialInterval:    time.Second * 15,
	BackoffCoefficient: 2.0,
	MaximumInterval:    time.Minute * 10,
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

func ExecuteSyncWorkflow(ctx workflow.Context, req *executor.ExecutionRequest) (map[string]interface{}, error) {
	logger.Infof("Executing sync workflow for job %d", req.JobID)

	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: req.Timeout,
		RetryPolicy:         DefaultRetryPolicy,
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Each scheduled workflow execution gets a unique WorkflowID.
	// We're assigning that here to distinguish between different scheduled executions.
	req.WorkflowID = workflow.GetInfo(ctx).WorkflowExecution.ID

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

	// send telemetry event - "sync started"
	api.SendTelemetryEvents(req.JobID, req.WorkflowID, "started")

	var result map[string]interface{}
	if err := workflow.ExecuteActivity(ctx, "ExecuteActivity", req).Get(ctx, &result); err != nil {
		logger.Errorf("Activity execution failed: %v", err)
		// send telemetry event - "sync failed"
		api.SendTelemetryEvents(req.JobID, req.WorkflowID, "failed")
		return nil, err
	}

	newStateFile, ok := result["response"].(string)
	if !ok {
		logger.Errorf("Invalid response format from worker: %+v", result)
		return nil, fmt.Errorf("invalid response format from worker")
	}

	// update the state file with the new state from response
	if err := database.GetDB().UpdateJobState(req.JobID, newStateFile, true); err != nil {
		logger.Errorf("Failed to update state file: %v", err)
		return nil, fmt.Errorf("failed to update state file: %v", err)
	}

	// send telemetry event - "sync completed"
	api.SendTelemetryEvents(req.JobID, req.WorkflowID, "completed")

	return result, nil
}
