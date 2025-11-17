package database

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"time"

	_ "github.com/lib/pq"
	"github.com/spf13/viper"

	"github.com/datazip-inc/olake-helm/worker/constants"
)

const (
	pingTimeout = 5 * time.Minute
)

type DB struct {
	client *sql.DB
	tables map[string]string
}

// creates a database connection instance.
func Init(ctx context.Context) (*DB, error) {
	connStr := buildConnectionString()
	tables := buildTablesMap()

	conn, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %s", err)
	}

	db := &DB{client: conn, tables: tables}

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %s", err)
	}

	if maxOpen := viper.GetInt(constants.EnvMaxOpenConnections); maxOpen > 0 {
		db.client.SetMaxOpenConns(maxOpen)
	}
	if maxIdle := viper.GetInt(constants.EnvMaxIdleConnections); maxIdle > 0 {
		db.client.SetMaxIdleConns(maxIdle)
	}
	if lifetime := viper.GetInt(constants.EnvConnectionMaxLifetime); lifetime > 0 {
		db.client.SetConnMaxLifetime(time.Duration(lifetime) * time.Second)
	}

	return db, nil
}

// buildConnectionString safely constructs the Postgres connection string.
func buildConnectionString() string {
	host := viper.GetString(constants.EnvDatabaseHost)
	port := viper.GetString(constants.EnvDatabasePort)
	user := viper.GetString(constants.EnvDatabaseUser)
	password := viper.GetString(constants.EnvDatabasePassword)
	database := viper.GetString(constants.EnvDatabaseDatabase)
	sslmode := viper.GetString(constants.EnvDatabaseSSLMode)

	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(user, password),
		Host:   fmt.Sprintf("%s:%s", host, port),
		Path:   "/" + database,
	}

	q := u.Query()
	q.Set("sslmode", sslmode)
	u.RawQuery = q.Encode()

	return u.String()
}

func buildTablesMap() map[string]string {
	runMode := viper.GetString(constants.EnvDatabaseRunMode)
	return map[string]string{
		"job":    fmt.Sprintf("olake-%s-job", runMode),
		"source": fmt.Sprintf("olake-%s-source", runMode),
		"dest":   fmt.Sprintf("olake-%s-destination", runMode),
	}
}

// Close closes the underlying database connection.
func (d *DB) Close() error {
	return d.client.Close()
}

func (d *DB) PingContext(ctx context.Context) error {
	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()

	return d.client.PingContext(pingCtx)
}
