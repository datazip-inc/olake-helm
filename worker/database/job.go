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
	sourceTable := db.tables["source"]
	destTable := db.tables["dest"]

	hasSchemaConfig, err := columnExists(cctx, db, jobTable, "schema_config")
	if err != nil {
		log.Error("failed to check schema_config column", "jobID", jobId, "error", err)
		return types.JobData{}, fmt.Errorf("failed to check schema_config column: %w", err)
	}

	var jobData types.JobData
	var schemaConfig sql.NullString

	// TODO: make column-exist check and query dynamic to handle more fields which future may add
	if hasSchemaConfig {
		query := fmt.Sprintf(`
			SELECT j.name, j.streams_config, j.schema_config, j.state, j.project_id, s.config, d.config, s.version, s.type
			FROM %q j
			JOIN %q s ON j.source_id = s.id
			JOIN %q d ON j.dest_id = d.id
			WHERE j.id = $1`,
			jobTable, sourceTable, destTable)
		err = db.client.QueryRowContext(cctx, query, jobId).Scan(
			&jobData.JobName, &jobData.Streams, &schemaConfig, &jobData.State,
			&jobData.ProjectID, &jobData.Source, &jobData.Destination, &jobData.Version, &jobData.Driver,
		)
	} else {
		query := fmt.Sprintf(`
			SELECT j.name, j.streams_config, j.state, j.project_id, s.config, d.config, s.version, s.type
			FROM %q j
			JOIN %q s ON j.source_id = s.id
			JOIN %q d ON j.dest_id = d.id
			WHERE j.id = $1`,
			jobTable, sourceTable, destTable)
		err = db.client.QueryRowContext(cctx, query, jobId).Scan(
			&jobData.JobName, &jobData.Streams, &jobData.State,
			&jobData.ProjectID, &jobData.Source, &jobData.Destination, &jobData.Version, &jobData.Driver,
		)
	}
	if err != nil {
		log.Error("failed to get job data from database", "jobID", jobId, "error", err)
		return types.JobData{}, fmt.Errorf("failed to scan job data: %w", err)
	}
	if schemaConfig.Valid {
		jobData.Schema = schemaConfig.String
	}

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
