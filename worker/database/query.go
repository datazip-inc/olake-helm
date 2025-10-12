package database

import "fmt"

// JobDataQuery returns the query to get the job, source, and destination data
func (db *DB) JobDataQuery() string {
	return fmt.Sprintf(`
		SELECT j.streams_config, j.state, s.config, d.config
		FROM %q j
		JOIN %q s ON j.source_id = s.id
		JOIN %q d ON j.dest_id = d.id
		WHERE j.id = $1
	`, db.tables["job"], db.tables["source"], db.tables["dest"])
}

// UpdateJobStateQuery returns the query to update the job state
func (db *DB) UpdateJobStateQuery() string {
	return fmt.Sprintf(`
		UPDATE %q 
		SET state = $1, active = $2, updated_at = NOW() 
		WHERE id = $3
	`, db.tables["job"])
}
