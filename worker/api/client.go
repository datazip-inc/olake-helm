package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/executor"
	"github.com/datazip-inc/olake-helm/worker/logger"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/spf13/viper"
)

func FetchJobDetails(jobId int) (map[string]interface{}, error) {
	url := fmt.Sprintf(
		"%s/presync/%d",
		viper.GetString(constants.EnvCallbackURL),
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
		req.Configs[idx].Data = utils.GetValueOrDefault(details, configName, config.Data)
	}
	return nil
}

func PostSyncUpdate(jobId int, state string) error {
	url := fmt.Sprintf("%s/postsync", viper.GetString(constants.EnvCallbackURL))

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
		logger.Warnf("failed to marshal request: %v", err)
		return
	}
	go func() {
		resp, err := http.Post(url, "application/json", strings.NewReader(string(jsonData)))
		if err != nil {
			logger.Warnf("failed to update sync telemetry: %v", err)
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
