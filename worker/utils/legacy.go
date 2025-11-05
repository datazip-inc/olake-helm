package utils

import (
	"fmt"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
)

// BuildSyncReqForLegacyOrNew builds execution request based on the workflow signature
//
// NEW workflow request
// - type: map[string]interface{}
// - return the req as it is
//
// OLD workflow request
// - Deprecated
// - type: int
// - maps the jobID to the new request format and returns it
func BuildSyncReqForLegacyOrNew(args interface{}) (*types.ExecutionRequest, error) {
	var req *types.ExecutionRequest

	switch v := args.(type) {
	case float64:
		logger.Warn("running old sync workflow [DEPRECATED]")
		req = &types.ExecutionRequest{
			Command: types.Sync,
			JobID:   int(v),
		}

	case map[string]interface{}:
		var tmp types.ExecutionRequest
		if err := Unmarshal(v, &tmp); err != nil {
			return nil, fmt.Errorf("invalid request format: %w", err)
		}
		req = &tmp

	default:
		logger.Errorf("unsupported input type %T", args)
		return nil, fmt.Errorf("unsupported input type %T", args)
	}

	return req, nil
}

// UpdateSyncRequest updates the ExecutionRequest for deprecated sync workflow
func UpdateSyncRequestForLegacy(job types.JobData, req *types.ExecutionRequest) {
	args := []string{
		"sync",
		"--config", "/mnt/config/source.json",
		"--destination", "/mnt/config/destination.json",
		"--catalog", "/mnt/config/streams.json",
		"--state", "/mnt/config/state.json",
	}

	req.ProjectID = job.ProjectID
	req.Command = types.Sync
	req.ConnectorType = job.Driver
	req.Version = job.Version
	req.Args = args
	req.Timeout = constants.DefaultSyncTimeout
}
