package temporal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/executor"
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

// TODO: Check if we can follow the current approach or have separate workflows and a single activity
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

	// update state file for scheduled workflow

	var result map[string]interface{}
	if err := workflow.ExecuteActivity(ctx, "ExecuteActivity", req).Get(ctx, &result); err != nil {
		return nil, err
	}

	// we get state file in response
	newStateFile, ok := result["response"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid response format from worker")
	}

	// update with the new state from response
	if err := PostSyncUpdate(req.JobID, newStateFile); err != nil {
		return nil, fmt.Errorf("failed to update state file: %v", err)
	}

	return result, nil
}

func GetJobDetails(jobId int) (map[string]interface{}, error) {
	url := fmt.Sprintf(
		"%s/presync/%d",
		utils.GetEnv(constants.EnvOlakeCallbackURL, constants.DefaultOlakeCallbackURL),
		jobId,
	)

	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to get job details: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("job details fetch failed with status %d: %s", resp.StatusCode, string(body))
	}

	var response struct {
		Success bool                   `json:"success"`
		Data    map[string]interface{} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode job details response: %v", err)
	}

	if !response.Success {
		return nil, fmt.Errorf("job details fetch failed")
	}

	return response.Data, nil
}

func UpdateConfigWithJobDetails(details map[string]interface{}, req *executor.ExecutionRequest) error {
	for idx, config := range req.Configs {
		configName := strings.Split(config.Name, ".")[0]
		req.Configs[idx].Data = GetValueOrDefault(details, configName, config.Data)
	}
	return nil
}

func GetValueOrDefault(m map[string]interface{}, key string, defaultValue string) string {
	if value, ok := m[key]; ok {
		return value.(string)
	}
	return defaultValue
}

func PostSyncUpdate(jobId int, state string) error {
	url := fmt.Sprintf("%s/postsync", utils.GetEnv(constants.EnvOlakeCallbackURL, constants.DefaultOlakeCallbackURL))

	payload := map[string]interface{}{
		"job_id": jobId,
		"state":  state,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %v", err)
	}

	resp, err := http.Post(url, "application/json", bytes.NewReader(jsonData))
	if err != nil {
		return fmt.Errorf("failed to post sync update: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("post sync update failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}
