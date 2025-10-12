package database

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake-helm/worker/logger"
)

const (
	queryTimeout = 5 * time.Second
)

func (db *DB) GetJobData(jobId int) (map[string]interface{}, error) {
	query := db.JobDataQuery()

	cctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
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
		logger.Warnf("no job found with ID: %d", jobId)
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
	query := db.UpdateJobStateQuery()

	cctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()

	_, err := db.client.ExecContext(cctx, query, state, active, jobId)
	if err != nil {
		return fmt.Errorf("failed to update job state: %w", err)
	}

	return nil
}
