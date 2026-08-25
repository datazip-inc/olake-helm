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
	serviceUI              = "ui"
	cliTelemetryMinVersion = "v0.9.4" // min CLI version that reads telemetry.json
)

// TelemetryContext is the per-run context handed to the connector via
// telemetry.json, and reused to build worker-emitted event properties.
type TelemetryContext struct {
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

// BuildContext assembles the full sync telemetry context.
func BuildContext(req *types.ExecutionRequest, job types.JobData, environment string, runCount int) TelemetryContext {
	return TelemetryContext{
		WorkflowID:         req.WorkflowID,
		SchemaVersion:      schemaVersion,
		Service:            serviceUI,
		DistinctID:         utils.GetTelemetryUserID(),
		JobID:              req.JobID,
		JobName:            job.JobName,
		Environment:        environment,
		SyncRunCount:       runCount,
		Frequency:          job.Frequency,
		CreatedAt:          job.CreatedAt.Format(time.RFC3339),
		SourceName:         job.SourceName,
		SourceVersion:      job.Version,
		DestinationName:    job.DestinationName,
		DestinationVersion: job.DestinationVersion,
	}
}

// BaseContext assembles telemetry context for commands with no job data
// (discover/check/spec), or as a fallback when job data can't be fetched.
func BaseContext(req *types.ExecutionRequest, environment string) TelemetryContext {
	return TelemetryContext{
		WorkflowID:    req.WorkflowID,
		SchemaVersion: schemaVersion,
		Service:       serviceUI,
		DistinctID:    utils.GetTelemetryUserID(),
		JobID:         req.JobID,
		Environment:   environment,
	}
}

// Properties returns the context as a map, for the "properties" field of a
// worker-emitted event sent to olake-ui.
func (c TelemetryContext) Properties() map[string]any {
	b, err := json.Marshal(c)
	if err != nil {
		return map[string]any{}
	}
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	return m
}

// JSON marshals the context for telemetry.json.
func (c TelemetryContext) JSON() (string, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// WriteConfigs merges user_id.txt and telemetry.json into req.Configs,
// unless telemetry is disabled. Added only if not already present, so
// retries reuse the same files. Unparsable versions (latest/dev) get no
// telemetry.json - CLI stays silent, worker owns the events.
func WriteConfigs(req *types.ExecutionRequest, ctx TelemetryContext) {
	if viper.GetBool(constants.EnvTelemetryDisabled) {
		return
	}

	configs := map[string]string{"user_id.txt": ctx.DistinctID}
	if semver.IsValid(req.Version) {
		if j, err := ctx.JSON(); err == nil {
			configs["telemetry.json"] = j
		} else {
			logger.Warnf("failed to marshal telemetry context: %s", err)
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
