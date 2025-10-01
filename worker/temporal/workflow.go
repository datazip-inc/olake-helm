package temporal

import (
	"fmt"
	"time"

	"github.com/datazip-inc/olake-helm/worker/api"
	"github.com/datazip-inc/olake-helm/worker/executor"
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
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: req.Timeout,
		RetryPolicy:         DefaultRetryPolicy,
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Each scheduled workflow execution gets a unique WorkflowID.
	// We're assigning that here to distinguish between different scheduled executions.
	req.WorkflowID = workflow.GetInfo(ctx).WorkflowExecution.ID

	// update the configs with latest details from the server
	jobDetails, err := api.FetchJobDetails(req.JobID)
	if err != nil {
		return nil, fmt.Errorf("failed to get job details: %v", err)
	}
	if err := api.UpdateConfigWithJobDetails(jobDetails, req); err != nil {
		return nil, fmt.Errorf("failed to update config with job details: %v", err)
	}

	// send telemetry event - "sync started"
	api.SendTelemetryEvents(req.JobID, req.WorkflowID, "started")

	var result map[string]interface{}
	if err := workflow.ExecuteActivity(ctx, "ExecuteActivity", req).Get(ctx, &result); err != nil {
		// send telemetry event - "sync failed"
		api.SendTelemetryEvents(req.JobID, req.WorkflowID, "failed")
		return nil, err
	}

	newStateFile, ok := result["response"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid response format from worker")
	}

	// update the state file with the new state from response
	if err := api.PostSyncUpdate(req.JobID, newStateFile); err != nil {
		return nil, fmt.Errorf("failed to update state file: %v", err)
	}

	// send telemetry event - "sync completed"
	api.SendTelemetryEvents(req.JobID, req.WorkflowID, "completed")

	return result, nil
}
