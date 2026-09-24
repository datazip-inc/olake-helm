package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"github.com/lib/pq"
)

const queryTimeout = 5 * time.Second

const columnExistsQuery = `
SELECT EXISTS (
	SELECT 1
	FROM information_schema.columns
	WHERE table_schema = 'public'
	  AND table_name = $1
	  AND column_name = $2
)`

func columnExists(ctx context.Context, db *DB, table, column string) (bool, error) {
	var exists bool
	if err := db.client.QueryRowContext(ctx, columnExistsQuery, table, column).Scan(&exists); err != nil {
		return false, fmt.Errorf("failed to check column %s on %s: %w", column, table, err)
	}
	return exists, nil
}

// optionalColumn returns the select expression for a job column that may not exist yet.
func optionalColumn(ctx context.Context, db *DB, table, column string) (string, error) {
	exists, err := columnExists(ctx, db, table, column)
	if err != nil {
		return "", err
	}
	if !exists {
		return "CAST(NULL AS TEXT)", nil
	}
	return "j." + column, nil
}

// decryptJobData decrypts the Source and Destination config fields of a JobData.
// If OLAKE_SECRET_KEY is not configured, Decrypt returns the value unchanged.
func decryptJobData(jobData *types.JobData) error {
	decryptedSource, err := utils.Decrypt(jobData.Source)
	if err != nil {
		return fmt.Errorf("failed to decrypt source config: %s", err)
	}
	jobData.Source = decryptedSource

	decryptedDest, err := utils.Decrypt(jobData.Destination)
	if err != nil {
		return fmt.Errorf("failed to decrypt destination config: %s", err)
	}
	jobData.Destination = decryptedDest

	return nil
}

func (db *DB) GetJobData(ctx context.Context, jobId int) (types.JobData, error) {
	log := logger.Log(ctx)
	cctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	jobTable := db.tables["job"]
	// The catalog columns may not exist yet when the worker runs against a database the UI has
	// not migrated; read them as NULL then.
	selectedStreamsExpr, err := optionalColumn(cctx, db, jobTable, "selected_streams_config")
	if err != nil {
		log.Error("failed to check selected_streams_config column", "jobID", jobId, "error", err)
		return types.JobData{}, fmt.Errorf("failed to check selected_streams_config column: %w", err)
	}
	availableStreamsExpr, err := optionalColumn(cctx, db, jobTable, "available_streams_config")
	if err != nil {
		log.Error("failed to check available_streams_config column", "jobID", jobId, "error", err)
		return types.JobData{}, fmt.Errorf("failed to check available_streams_config column: %w", err)
	}

	query := fmt.Sprintf(`
			SELECT j.name, j.streams_config, %s, %s, j.state, j.project_id, s.config, d.config, s.version, s.type, COALESCE(j.advanced_settings::text, ''),
				j.frequency, j.created_at, d.version, s.name, d.name
			FROM %q j
			JOIN %q s ON j.source_id = s.id
			JOIN %q d ON j.dest_id = d.id
			WHERE j.id = $1`,
		selectedStreamsExpr, availableStreamsExpr, jobTable, db.tables["source"], db.tables["dest"])

	rows := db.client.QueryRowContext(cctx, query, jobId)

	var jobData types.JobData
	var selectedStreams, availableStreams sql.NullString
	if err := rows.Scan(&jobData.JobName, &jobData.Streams, &selectedStreams, &availableStreams, &jobData.State, &jobData.ProjectID, &jobData.Source, &jobData.Destination, &jobData.Version, &jobData.Driver, &jobData.AdvancedSettings,
		&jobData.Frequency, &jobData.CreatedAt, &jobData.DestinationVersion, &jobData.SourceName, &jobData.DestinationName); err != nil {
		log.Error("failed to get job data from database", "jobID", jobId, "error", err)
		return types.JobData{}, fmt.Errorf("failed to scan job data: %w", err)
	}
	jobData.SelectedStreams = selectedStreams.String
	jobData.AvailableStreams = availableStreams.String

	if err := decryptJobData(&jobData); err != nil {
		log.Error("failed to decrypt job data", "jobID", jobId, "error", err)
		return types.JobData{}, fmt.Errorf("failed to decrypt job data job_id[%d]: %s", jobId, err)
	}

	return jobData, nil
}

func (db *DB) UpdateJobState(ctx context.Context, jobId int, state string) error {
	log := logger.Log(ctx)

	log.Info("updating job state", "jobID", jobId, "state", state)

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
		log.Error("failed to update job state", "jobID", jobId, "error", err)
		return fmt.Errorf("failed to update job state: %s", err)
	}

	log.Info("successfully updated job state", "jobID", jobId, "state", state)

	return nil
}
