package notifications

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type WebhookMessage struct {
	Text string `json:"text"`
}

// SendWebhookNotification sends a formatted sync failure message to the given webhook URL.
func SendWebhookNotification(ctx context.Context, jobID int, lastRunTime time.Time, jobName, errMsg, webhookURL string) error {
	if strings.TrimSpace(webhookURL) == "" {
		return fmt.Errorf("webhook_alert_url not configured")
	}

	message := fmt.Sprintf(
		"🚨 *Sync Failure Detected!* \n\n"+
			"------------------------------------------- \n\n"+
			"• *Job ID:* `%d` \n\n"+
			"• *Job Name:* `%s` \n\n"+
			"• *Error:* ```%s``` \n\n"+
			"• *Last Run Time:* %s \n\n"+
			"------------------------------------------- \n\n",
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
