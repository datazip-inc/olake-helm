package telemetry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"github.com/spf13/viper"
)

type TelemetryEvent string

const (
	TelemetryEventStarted   TelemetryEvent = "started"
	TelemetryEventCompleted TelemetryEvent = "completed"
	TelemetryEventFailed    TelemetryEvent = "failed"
)

// event = "started" | "completed" | "failed"
func SendEvent(jobId int, environment, workflowId string, event TelemetryEvent) {
	go func() {
		switch event {
		case TelemetryEventStarted, TelemetryEventCompleted, TelemetryEventFailed:
		default:
			logger.Warnf("invalid telemetry event: %s", event)
			return
		}

		url := fmt.Sprintf("%s/sync-telemetry",
			viper.GetString(constants.EnvCallbackURL),
		)

		payload := map[string]interface{}{
			"job_id":      jobId,
			"workflow_id": workflowId,
			"environment": environment,
			"event":       event,
		}

		jsonData, err := json.Marshal(payload)
		if err != nil {
			logger.Warnf("failed to marshal request: %s", err)
			return
		}

		resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
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
