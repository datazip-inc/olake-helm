package notifications

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/datazip-inc/olake-helm/worker/database"
)

type WebhookMessage struct {
	Text string `json:"text"`
}

func SendWebhookNotification(ctx context.Context, jobID int, projectID string, lastRunTime time.Time, jobName, errMsg string) error {
	// Get project settings to retrieve webhook URL
	settings, err := database.GetDB().GetProjectSettingsByProjectID(ctx, projectID)
	if err != nil {
		return fmt.Errorf("failed to get project settings for project_id %s: %w", projectID, err)
	}

	webhookURL := settings.WebhookAlertURL
	if webhookURL == "" {
		return fmt.Errorf("webhook_alert_url not configured for project_id %s", projectID)
	}

	message := fmt.Sprintf(
		"🚨 *Sync Failure Detected!*\n"+
			"-----------------------------------\n"+
			"• *Job ID:* `%d`\n"+
			"• *Job Name:* `%s`\n"+
			"• *Error:* ```%s```\n"+
			"• *Last Run Time:* %s\n"+
			"-----------------------------------",
		jobID,
		jobName,
		trimErrorLogs(errMsg),
		lastRunTime.Format("2006-01-02 15:04:05 MST"),
	)

	payload, _ := json.Marshal(WebhookMessage{Text: message})
	resp, err := http.Post(webhookURL, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("failed to send webhook notification: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned non-2xx status: %s", resp.Status)
	}
	return nil
}

func trimErrorLogs(logs string) string {
	lines := strings.Split(logs, "\n")
	var filtered []string
	for _, line := range lines {
		// Keep only FATAL or ERROR lines
		if strings.Contains(line, "FATAL") || strings.Contains(line, "ERROR") {
			filtered = append(filtered, line)
		}
	}
	if len(filtered) == 0 {
		return "No critical error lines found. See full logs for details."
	}
	return strings.Join(filtered, "\n")
}