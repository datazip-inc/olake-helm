package notifications

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

type SlackMessage struct {
	Text string `json:"text"`
}

func SendSlackNotification(jobID int, workflowID, errMsg string) error {
	webhookURL := os.Getenv("SLACK_WEBHOOK_URL")
	if webhookURL == "" {
		return fmt.Errorf("SLACK_WEBHOOK_URL not set")
	}
	message := fmt.Sprintf(
		"🚨 *Sync Failure Detected!*\n"+
			"-----------------------------------\n"+
			"• *Job ID:* `%d`\n"+
			"• *Workflow ID:* `%s`\n"+
			"• *Error:* ```%s```\n"+
			"• *Timestamp:* %s\n"+
			"-----------------------------------",
		jobID,
		workflowID,
		errMsg,
		time.Now().UTC().Format("2006-01-02 15:04:05 MST"),
	)

	payload, _ := json.Marshal(SlackMessage{Text: message})
	resp, err := http.Post(webhookURL, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("failed to send Slack notification: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("Slack webhook returned non-2xx status: %s", resp.Status)
	}
	return nil
}
