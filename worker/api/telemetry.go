package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/logger"
	"github.com/spf13/viper"
)

// event = "started" | "completed" | "failed"
func SendTelemetryEvents(jobId int, workflowId string, event string) {
	url := fmt.Sprintf(
		"%s/sync-telemetry",
		viper.GetString(constants.EnvCallbackURL),
	)

	payload := map[string]interface{}{
		"job_id":      jobId,
		"workflow_id": workflowId,
		"event":       event,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		logger.Warnf("failed to marshal request: %s", err)
		return
	}
	go func() {
		resp, err := http.Post(url, "application/json", strings.NewReader(string(jsonData)))
		if err != nil {
			logger.Warnf("failed to update sync telemetry: %s", err)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			logger.Warnf("sync telemetry update failed with status %d: %s", resp.StatusCode, string(body))
			return
		}
	}()
}
