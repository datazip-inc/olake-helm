package activities

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"golang.org/x/mod/semver"

	"github.com/datazip-inc/olake-helm/olake-workers/k8s/config"
	"github.com/datazip-inc/olake-helm/olake-workers/k8s/database"
	"github.com/datazip-inc/olake-helm/olake-workers/k8s/logger"
	"github.com/datazip-inc/olake-helm/olake-workers/k8s/pods"
	"github.com/datazip-inc/olake-helm/olake-workers/k8s/shared"
	"github.com/datazip-inc/olake-helm/olake-workers/k8s/utils/filesystem"
	"github.com/datazip-inc/olake-helm/olake-workers/k8s/utils/helpers"
)

// Activities holds the dependencies for activity functions
type Activities struct {
	db         *database.DB
	podManager *pods.K8sPodManager
	config     *config.Config
}

// NewActivities creates a new Activities instance with injected dependencies
func NewActivities(db *database.DB, podManager *pods.K8sPodManager, cfg *config.Config) *Activities {
	return &Activities{
		db:         db,
		podManager: podManager,
		config:     cfg,
	}
}

// DiscoverCatalogActivity discovers data source catalog using Kubernetes Pod
func (a *Activities) DiscoverCatalogActivity(ctx context.Context, params shared.ActivityParams) (map[string]interface{}, error) {
	activityLogger := activity.GetLogger(ctx)
	activityLogger.Debug("Starting K8s discover catalog activity")

	// Transform Temporal activity parameters into Kubernetes pod execution request
	// Maps connector type/version to container image, mounts config as files, sets operation-specific timeout
	imageName, err := a.podManager.GetDockerImageName(params.SourceType, params.Version)
	if err != nil {
		return nil, fmt.Errorf("failed to get docker image name: %v", err)
	}

	// Build arguments for discover command
	args := []string{string(shared.Discover), "--config", "/mnt/config/config.json"}
	configs := []shared.JobConfig{
		{Name: "config.json", Data: params.Config},
	}

	if params.JobName != "" && semver.Compare(params.Version, "v0.2.0") >= 0 {
		args = append(args, "--destination-database-prefix", params.JobName)
	}

	// Add streams configuration if provided (for stream merging)
	if params.StreamsConfig != "" {
		args = append(args, "--catalog", "/mnt/config/streams.json")
		configs = append(configs, shared.JobConfig{
			Name: "streams.json",
			Data: params.StreamsConfig,
		})
	}

	// Add encryption key if configured
	if a.config.Kubernetes.OLakeSecretKey != "" {
		args = append(args, "--encryption-key", a.config.Kubernetes.OLakeSecretKey)
	}

	request := pods.PodActivityRequest{
		WorkflowID:    params.WorkflowID,
		JobID:         params.JobID,
		Operation:     shared.Discover,
		ConnectorType: params.SourceType,
		Image:         imageName,
		Args:          args,
		Configs:       configs,
		Timeout:       helpers.GetActivityTimeout("discover"),
		HeartbeatFunc: activity.RecordHeartbeat,
	}

	// Execute discover operation by creating K8s pod, wait for completion, retrieve results from streams.json file
	return a.podManager.ExecutePodActivity(ctx, request)
}

// TestConnectionActivity tests data source connection using Kubernetes Pod
func (a *Activities) TestConnectionActivity(ctx context.Context, params shared.ActivityParams) (map[string]interface{}, error) {
	activityLogger := activity.GetLogger(ctx)
	activityLogger.Debug("Starting K8s test connection activity",
		"sourceType", params.SourceType,
		"version", params.Version,
		"workflowID", params.WorkflowID)

	// Record heartbeat
	activity.RecordHeartbeat(ctx, "Creating Kubernetes Pod for connection test")

	// Transform Temporal activity parameters into Kubernetes pod execution request
	// Maps connector type/version to container image, includes flag parameter, mounts config as files
	imageName, err := a.podManager.GetDockerImageName(params.SourceType, params.Version)
	if err != nil {
		return nil, fmt.Errorf("failed to get docker image name: %v", err)
	}

	// Build args slice with encryption key if configured
	args := []string{
		string(shared.Check),
		fmt.Sprintf("--%s", params.Flag),
		"/mnt/config/config.json",
	}
	if a.config.Kubernetes.OLakeSecretKey != "" {
		args = append(args, "--encryption-key", a.config.Kubernetes.OLakeSecretKey)
	}

	request := pods.PodActivityRequest{
		WorkflowID:    params.WorkflowID,
		JobID:         params.JobID,
		Operation:     shared.Check,
		ConnectorType: params.SourceType,
		Image:         imageName,
		Args:          args,
		Configs: []shared.JobConfig{
			{Name: "config.json", Data: params.Config},
		},
		Timeout:       helpers.GetActivityTimeout("test"),
		HeartbeatFunc: activity.RecordHeartbeat,
	}

	// Execute check operation by creating K8s pod, wait for completion, retrieve results from pod logs
	return a.podManager.ExecutePodActivity(ctx, request)
}

// SyncActivity syncs data using Kubernetes Pod
func (a *Activities) SyncActivity(ctx context.Context, params shared.SyncParams) (map[string]interface{}, error) {
	activityLogger := activity.GetLogger(ctx)
	activityLogger.Debug("Starting K8s sync activity",
		"jobId", params.JobID,
		"workflowID", params.WorkflowID)

	// Record heartbeat
	activity.RecordHeartbeat(ctx, "Creating Kubernetes Pod for data sync")

	// Retrieve job configuration from database to get all required sync parameters
	jobData, err := a.db.GetJobData(ctx, params.JobID)
	if err != nil {
		errMsg := fmt.Sprintf("failed to get job data for jobID %d", params.JobID)
		return nil, temporal.NewNonRetryableApplicationError(errMsg, "DatabaseError", err)
	}

	// Validate and fix empty/null state
	stateData := jobData["state"].(string)
	if stateData == "" || stateData == "null" || stateData == "NULL" {
		stateData = "{}"
		logger.Infof("Job %d has empty/null state, defaulting to: {}", params.JobID)
	}

	// Transform job data and Temporal activity parameters into Kubernetes pod execution request
	// Maps all sync configuration files (config, catalog, destination, state) as mounted files
	imageName, err := a.podManager.GetDockerImageName(jobData["source_type"].(string), jobData["source_version"].(string))
	if err != nil {
		errMsg := fmt.Sprintf("failed to get docker image name for jobID %d", params.JobID)
		return nil, temporal.NewNonRetryableApplicationError(errMsg, "ConfigurationError", err)
	}

	// Build args slice with encryption key if configured
	args := []string{
		string(shared.Sync),
		"--config", "/mnt/config/config.json",
		"--catalog", "/mnt/config/streams.json",
		"--destination", "/mnt/config/writer.json",
		"--state", "/mnt/config/state.json",
	}
	if a.config.Kubernetes.OLakeSecretKey != "" {
		args = append(args, "--encryption-key", a.config.Kubernetes.OLakeSecretKey)
	}

	request := pods.PodActivityRequest{
		WorkflowID:    params.WorkflowID,
		JobID:         params.JobID,
		Operation:     shared.Sync,
		ConnectorType: jobData["source_type"].(string),
		Image:         imageName,
		Args:          args,
		Configs: []shared.JobConfig{
			{Name: "config.json", Data: jobData["source_config"].(string)},
			{Name: "streams.json", Data: jobData["streams_config"].(string)},
			{Name: "writer.json", Data: jobData["dest_config"].(string)},
			{Name: "state.json", Data: stateData},
		},
		Timeout:       helpers.GetActivityTimeout("sync"),
		HeartbeatFunc: activity.RecordHeartbeat,
	}

	// Note: Final state saving is now handled by SyncCleanupActivity in the workflow defer block
	result, err := a.podManager.ExecutePodActivity(ctx, request)
	if err != nil && errors.Is(err, pods.ErrPodFailed) {
		return nil, temporal.NewNonRetryableApplicationError("sync connector failed", "PodFailed", err)
	}
	return result, err
}

// SyncCleanupActivity cleans up Kubernetes pod resources and saves final state after sync workflow
func (a *Activities) SyncCleanupActivity(ctx context.Context, params shared.SyncParams) error {
	activityLogger := activity.GetLogger(ctx)
	activityLogger.Info("Starting sync cleanup activity",
		"jobID", params.JobID,
		"workflowID", params.WorkflowID)

	// Step 1: Delete the pod
	podName := k8s.SanitizeName(params.WorkflowID)
	if err := a.podManager.CleanupPod(ctx, podName); err != nil {
		logger.Errorf("Failed to cleanup pod %s for job %d: %v", podName, params.JobID, err)
		return fmt.Errorf("failed to cleanup pod: %v", err)
	}
	logger.Infof("Successfully cleaned up pod %s for job %d", podName, params.JobID)

	// Step 2: Save final state from filesystem to database
	fsHelper := filesystem.NewHelper()
	if stateData, readErr := fsHelper.ReadAndValidateStateFile(params.WorkflowID); readErr != nil {
		// Missing or invalid state file is a critical error - fail the workflow
		logger.Errorf("Could not read state file for job %d: %v", params.JobID, readErr)
		return readErr
	} else {
		// Save state to database
		if updateErr := a.db.UpdateJobState(ctx, params.JobID, string(stateData), true); updateErr != nil {
			logger.Errorf("Failed to save final state for job %d: %v", params.JobID, updateErr)
			return updateErr
		}
		logger.Infof("Successfully saved final state for job %d", params.JobID)
	}

	return nil
}

// FetchSpecActivity fetches connector specifications using Kubernetes Pod
func (a *Activities) FetchSpecActivity(ctx context.Context, params shared.ActivityParams) (map[string]interface{}, error) {
	activityLogger := activity.GetLogger(ctx)
	activityLogger.Debug("Starting K8s spec activity",
		"sourceType", params.SourceType,
		"version", params.Version,
		"workflowID", params.WorkflowID)

	activity.RecordHeartbeat(ctx, "Creating Kubernetes Pod for spec fetch")

	// Transform Temporal activity parameters into Kubernetes pod execution request
	// Maps connector type/version to container image for spec retrieval
	imageName, err := a.podManager.GetDockerImageName(params.SourceType, params.Version)
	if err != nil {
		return nil, fmt.Errorf("failed to get docker image name: %v", err)
	}

	// Build args slice for spec command
	args := []string{string(shared.Spec)}

	// Add destination-type flag if DestConfig is provided (for destination specs)
	if params.DestinationType != "" {
		args = append(args, "--destination-type", params.DestinationType)
	}
	// TODO: remove JobID in refactor
	request := pods.PodActivityRequest{
		WorkflowID:    params.WorkflowID,
		JobID:         params.JobID,
		Operation:     shared.Spec,
		ConnectorType: params.SourceType,
		Image:         imageName,
		Args:          args,
		Configs:       []shared.JobConfig{}, // No config files needed for spec
		Timeout:       helpers.GetActivityTimeout("spec"),
		HeartbeatFunc: activity.RecordHeartbeat,
	}

	// Execute spec operation by creating K8s pod, wait for completion, retrieve results from pod logs
	return a.podManager.ExecutePodActivity(ctx, request)
}
