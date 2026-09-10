package telemetry

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"github.com/spf13/viper"
	"golang.org/x/mod/semver"
)

const (
	schemaVersion          = 1
	serviceUI              = "UI"
	cliTelemetryMinVersion = "v0.9.4" // min CLI version that reads telemetry.json
)

// TelemetryPayload is the per-run payload handed to the connector via
// telemetry.json, and reused to build worker-emitted event properties.
type TelemetryPayload struct {
	WorkflowID         string `json:"-"` // needed for olake-ui
	SchemaVersion      int    `json:"schema_version"`
	Service            string `json:"service,omitempty"`
	DistinctID         string `json:"distinct_id,omitempty"`
	JobID              int    `json:"job_id,omitempty"`
	JobName            string `json:"job_name,omitempty"`
	Environment        string `json:"environment,omitempty"`
	SyncRunCount       int    `json:"sync_run_count,omitempty"`
	Frequency          string `json:"frequency,omitempty"`
	CreatedAt          string `json:"created_at,omitempty"`
	SourceName         string `json:"source_name,omitempty"`
	SourceVersion      string `json:"source_version,omitempty"`
	DestinationName    string `json:"destination_name,omitempty"`
	DestinationVersion string `json:"destination_version,omitempty"`
}

// BuildPayload assembles the full sync telemetry payload.
func BuildPayload(req *types.ExecutionRequest, job types.JobData, runCount int) TelemetryPayload {
	return TelemetryPayload{
		WorkflowID:         req.WorkflowID,
		SchemaVersion:      schemaVersion,
		Service:            serviceUI,
		DistinctID:         utils.GetTelemetryUserID(),
		JobID:              req.JobID,
		JobName:            job.JobName,
		Environment:        utils.GetExecutorEnvironment(),
		SyncRunCount:       runCount,
		Frequency:          job.Frequency,
		CreatedAt:          job.CreatedAt.Format(time.RFC3339),
		SourceName:         job.SourceName,
		SourceVersion:      job.Version,
		DestinationName:    job.DestinationName,
		DestinationVersion: job.DestinationVersion,
	}
}

// BasePayload assembles telemetry payload for commands with no job data
// (discover/check/spec), or as a fallback when job data can't be fetched.
func BasePayload(req *types.ExecutionRequest) TelemetryPayload {
	return TelemetryPayload{
		WorkflowID:    req.WorkflowID,
		SchemaVersion: schemaVersion,
		Service:       serviceUI,
		DistinctID:    utils.GetTelemetryUserID(),
		JobID:         req.JobID,
		Environment:   utils.GetExecutorEnvironment(),
	}
}

// Properties returns the payload as a map, for the "properties" field of a
// worker-emitted event sent to olake-ui.
func (p TelemetryPayload) Properties() map[string]any {
	b, err := json.Marshal(p)
	if err != nil {
		return map[string]any{}
	}
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	return m
}

// JSON marshals the payload for telemetry.json.
func (p TelemetryPayload) JSON() (string, error) {
	b, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// WriteConfigs merges user_id.txt and telemetry.json into req.Configs,
// unless telemetry is disabled. Added only if not already present, so
// retries reuse the same files. Unparsable versions (latest/dev) get no
// telemetry.json - CLI stays silent, worker owns the events.
func WriteConfigs(req *types.ExecutionRequest, payload TelemetryPayload) {
	if viper.GetBool(constants.EnvTelemetryDisabled) {
		return
	}

	configs := map[string]string{"user_id.txt": payload.DistinctID}
	if semver.IsValid(req.Version) {
		if j, err := payload.JSON(); err == nil {
			configs["telemetry.json"] = j
		} else {
			logger.Warnf("failed to marshal telemetry payload: %s", err)
		}
	}
	utils.ApplyConfigUpdates(req, nil, configs)
}

// SupportsCLITelemetry reports if connection version supports reading telemetry.json
// if version is unparsable then worker sends the events
func SupportsCLITelemetry(version string) bool {
	return semver.IsValid(version) && semver.Compare(version, cliTelemetryMinVersion) >= 0
}

// ExternalKillReason reports whether an ErrExecutionFailed error indicates
// the connector was killed externally rather than exiting on its own, by
// reading the reason kubernetes already puts in the error text. Returns ""
// when it wasn't an external kill, or the reason can't be determined.
//
// NOTE: Docker's wait path only reports a bare exit code, so OOM kills go
// undetected there. Needs an extra ContainerInspect for State.OOMKilled.
func ExternalKillReason(err error) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "OOMKilled"):
		return "oom_killed"
	case strings.Contains(msg, "Evicted"):
		return "evicted"
	case strings.Contains(msg, "DeadlineExceeded"):
		return "deadline_exceeded"
	default:
		return ""
	}
}
