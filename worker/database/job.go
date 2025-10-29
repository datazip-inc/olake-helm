package database

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/lib/pq"
)

const (
	queryTimeout = 5 * time.Second
)

func (db *DB) GetJobData(ctx context.Context, jobId int) (types.JobData, error) {
	cctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	query := fmt.Sprintf(`
			SELECT j.streams_config, j.state, s.config, d.config, s.version, s.type
			FROM %q j
			JOIN %q s ON j.source_id = s.id
			JOIN %q d ON j.dest_id = d.id
			WHERE j.id = $1`,
		db.tables["job"], db.tables["source"], db.tables["dest"])

	rows := db.client.QueryRowContext(cctx, query, jobId)

	var jobData types.JobData
	if err := rows.Scan(&jobData.Streams, &jobData.State, &jobData.Source, &jobData.Destination, &jobData.Version, &jobData.Driver); err != nil {
		return types.JobData{}, fmt.Errorf("failed to scan job data: %w", err)
	}
	return jobData, nil
}

func (db *DB) UpdateJobState(ctx context.Context, jobId int, state string) error {
	tableName := pq.QuoteIdentifier(db.tables["job"])
	query := fmt.Sprintf(`
			UPDATE %s
			SET state = $1, updated_at = NOW() 
			WHERE id = $2`,
		tableName)

	cctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	_, err := db.client.ExecContext(cctx, query, state, jobId)
	if err != nil {
		return fmt.Errorf("failed to update job state: %w", err)
	}

	return nil
}
