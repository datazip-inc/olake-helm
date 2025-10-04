package database

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake-helm/worker/logger"
)

func (db *DB) GetJobData(jobId int) (map[string]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT j.streams_config, j.state, s.config, d.config
		FROM %q j
		JOIN %q s ON j.source_id = s.id
		JOIN %q d ON j.dest_id = d.id
		WHERE j.id = $1
	`, db.tables["job"], db.tables["source"], db.tables["dest"])

	cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := db.client.QueryContext(cctx, query, jobId)
	if err != nil {
		return nil, fmt.Errorf("failed to get job data: %w", err)
	}
	defer rows.Close()

	var (
		sourceConfig  string
		destConfig    string
		streamsConfig string
		state         string
	)

	if !rows.Next() {
		logger.Warnf("No job found with ID: %d", jobId)
		return nil, fmt.Errorf("no job found with ID: %d", jobId)
	}

	if err := rows.Scan(&streamsConfig, &state, &sourceConfig, &destConfig); err != nil {
		return nil, fmt.Errorf("failed to scan job data: %w", err)
	}

	return map[string]interface{}{
		"streams":     streamsConfig,
		"state":       state,
		"source":      sourceConfig,
		"destination": destConfig,
	}, nil
}

func (db *DB) UpdateJobState(jobId int, state string, active bool) error {
	query := fmt.Sprintf(`UPDATE %q SET state = $1, active = $2, updated_at = NOW() WHERE id = $3`, db.tables["job"])

	cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := db.client.ExecContext(cctx, query, state, active, jobId)
	return err
}
