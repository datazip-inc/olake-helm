package database

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake-helm/worker/types"
)

const (
	projectQueryTimeout = 5 * time.Second
)

// GetProjectSettingsByProjectID fetches the project settings for a given project_id
func (db *DB) GetProjectSettingsByProjectID(ctx context.Context, projectID string) (*types.ProjectSettings, error) {
	if projectID == "" {
		return nil, fmt.Errorf("project_id is required")
	}

	cctx, cancel := context.WithTimeout(ctx, projectQueryTimeout)
	defer cancel()

	query := fmt.Sprintf(`
		SELECT id, project_id, webhook_alert_url 
		FROM %q 
		WHERE project_id = $1`,
		db.tables["project-settings"])

	settings := &types.ProjectSettings{}

	rows := db.client.QueryRowContext(cctx, query, projectID)
	if err := rows.Scan(&settings.ID, &settings.ProjectID, &settings.WebhookAlertURL); err != nil {
		return nil, fmt.Errorf("failed to get project settings for project_id %s: %w", projectID, err)
	}

	return settings, nil
}

