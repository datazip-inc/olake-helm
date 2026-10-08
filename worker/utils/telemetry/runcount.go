package telemetry

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/storagemode"
	"go.temporal.io/sdk/client"
)

const scheduleDescribeTimeout = 5 * time.Second

// jobRunCounter is the on-disk shape of a per-job counter file.
type jobRunCounter struct {
	RunCount   int  `json:"run_count"`
	Ineligible bool `json:"ineligible,omitempty"`
}

// jobCounterPath returns the relative path to the job counter file
func jobCounterPath(jobID int) string {
	return filepath.Join("telemetry", "job-counters", fmt.Sprintf("%d", jobID))
}

// readCounter returns (counter, true) if a counter file exists and parses;
// (zero value, false) if it's missing, unreadable, or unparsable.
func readCounter(ctx context.Context, path string) (jobRunCounter, bool) {
	var data string
	var err error
	switch storagemode.Get() {
	case constants.StorageModeS3:
		data, err = utils.ReadFileFromS3(ctx, "", path, false)
	default:
		data, err = utils.ReadFileFromNFS(utils.GetConfigDir(), path)
	}
	if err != nil {
		return jobRunCounter{}, false
	}
	var c jobRunCounter
	if err := json.Unmarshal([]byte(data), &c); err != nil {
		return jobRunCounter{}, false
	}
	return c, true
}

func writeCounter(ctx context.Context, path string, c jobRunCounter) {
	data, err := json.Marshal(c)
	if err != nil {
		return
	}
	_ = utils.WriteConfigFiles(ctx, utils.GetConfigDir(), []types.JobConfig{{Name: path, Data: string(data)}})
}

// GetOrIncrementSyncRunCount returns which run number this sync is for the
// job (1, 2, 3...), or 0 when unknown - callers must then omit the property.
//
// Counting only starts for jobs whose very first sync happens after this
// feature ships (checked once via ScheduleInfo.NumActions and cached in the
// counter file). Older jobs already have runs we never counted, so they are
// marked ineligible instead of reporting a misleadingly low number.
//
// Activity retries (attempt > 1) reuse the run number instead of
// incrementing it again.
func GetOrIncrementSyncRunCount(ctx context.Context, tempClient client.Client, req *types.ExecutionRequest, attempt int) int {
	path := jobCounterPath(req.JobID)

	if attempt > 1 {
		c, ok := readCounter(ctx, path)
		if !ok || c.Ineligible {
			return 0
		}
		return c.RunCount
	}

	c, ok := readCounter(ctx, path)
	switch {
	case !ok:
		numActions, err := scheduleActionCount(ctx, tempClient, req)
		if err != nil {
			// Unknown either way - don't persist a decision, try again next run.
			return 0
		}
		if numActions > 1 {
			writeCounter(ctx, path, jobRunCounter{Ineligible: true})
			return 0
		}
		c = jobRunCounter{RunCount: 1}
	case c.Ineligible:
		return 0
	default:
		c.RunCount++
	}

	writeCounter(ctx, path, c)
	return c.RunCount
}

func scheduleActionCount(ctx context.Context, tempClient client.Client, req *types.ExecutionRequest) (int, error) {
	describeCtx, cancel := context.WithTimeout(ctx, scheduleDescribeTimeout)
	defer cancel()

	projectID := req.ProjectID
	if projectID == "" {
		projectID = "123"
	}
	_, scheduleID := utils.SyncWorkflowAndScheduleID(projectID, req.JobID)

	desc, err := tempClient.ScheduleClient().GetHandle(describeCtx, scheduleID).Describe(describeCtx)
	if err != nil {
		return 0, err
	}
	return desc.Info.NumActions, nil
}

// ReadSyncRunCount reads without incrementing - reports the same ordinal
// TrackSyncStarted already assigned this run.
func ReadSyncRunCount(ctx context.Context, jobID int) int {
	c, ok := readCounter(ctx, jobCounterPath(jobID))
	if !ok || c.Ineligible {
		return 0
	}
	return c.RunCount
}
