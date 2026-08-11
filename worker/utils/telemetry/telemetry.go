package telemetry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"github.com/spf13/viper"
)

type TelemetryEvent string

const (
	TelemetryEventStarted   TelemetryEvent = "started"
	TelemetryEventCompleted TelemetryEvent = "completed"
	TelemetryEventFailed    TelemetryEvent = "failed"
	TelemetryEventCancelled TelemetryEvent = "cancelled"
)

var httpClient = &http.Client{Timeout: 10 * time.Second}

// EventPayload is the sync-telemetry callback payload sent to olake-ui.
// SyncRunCount is omitted from the JSON body when zero.
type EventPayload struct {
	JobID                int
	ExecutionEnvironment string
	WorkflowID           string
	SyncRunCount         int
	Event                TelemetryEvent
}

// SendEvent sends a sync-telemetry event to olake-ui.
func SendEvent(buildPayload func() EventPayload) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Warnf("recovered panic in telemetry SendEvent: %v", r)
			}
		}()

		payload := buildPayload()

		switch payload.Event {
		case TelemetryEventStarted, TelemetryEventCompleted, TelemetryEventFailed, TelemetryEventCancelled:
		default:
			logger.Warnf("invalid telemetry event: %s", payload.Event)
			return
		}

		url := fmt.Sprintf("%s/sync-telemetry",
			viper.GetString(constants.EnvCallbackURL),
		)

		body := map[string]interface{}{
			"job_id":      payload.JobID,
			"workflow_id": payload.WorkflowID,
			"environment": payload.ExecutionEnvironment,
			"event":       payload.Event,
		}
		if payload.SyncRunCount > 0 {
			body["sync_run_count"] = payload.SyncRunCount
		}

		jsonData, err := json.Marshal(body)
		if err != nil {
			logger.Warnf("failed to marshal request: %s", err)
			return
		}

		resp, err := httpClient.Post(url, "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			logger.Warnf("failed to update sync telemetry: %s", err)
			return
		}
		defer func() {
			if cerr := resp.Body.Close(); cerr != nil {
				logger.Warnf("failed to close response body: %s", cerr)
			}
		}()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			logger.Debugf("sync telemetry update failed: %d %s", resp.StatusCode, string(body))
		}
	}()
}
