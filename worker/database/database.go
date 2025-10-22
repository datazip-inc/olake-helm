package database

import (
	"database/sql"
	"fmt"
	"net/url"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/spf13/viper"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/logger"
)

type DB struct {
	client *sql.DB
	tables map[string]string
}

var (
	db   *DB
	once sync.Once
)

// GetDB returns a singleton database connection instance.
func GetDB() *DB {
	once.Do(func() {
		var err error
		db, err = newDatabase()
		if err != nil {
			logger.Fatalf("failed to create database: %s", err)
		}
	})
	return db
}

func newDatabase() (*DB, error) {
	connStr := buildConnectionString()

	conn, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	if err := conn.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	if maxOpen := viper.GetInt(constants.EnvMaxOpenConnections); maxOpen > 0 {
		conn.SetMaxOpenConns(maxOpen)
	}
	if maxIdle := viper.GetInt(constants.EnvMaxIdleConnections); maxIdle > 0 {
		conn.SetMaxIdleConns(maxIdle)
	}
	if lifetime := viper.GetInt(constants.EnvConnectionMaxLifetime); lifetime > 0 {
		conn.SetConnMaxLifetime(time.Duration(lifetime) * time.Second)
	}

	// runMode := viper.GetString(constants.EnvDatabaseRunMode)
	runMode := "localdev"
	tables := map[string]string{
		"job":    fmt.Sprintf("olake-%s-job", runMode),
		"source": fmt.Sprintf("olake-%s-source", runMode),
		"dest":   fmt.Sprintf("olake-%s-destination", runMode),
	}

	return &DB{
		client: conn,
		tables: tables,
	}, nil
}

// buildConnectionString safely constructs the Postgres connection string.
func buildConnectionString() string {
	host := viper.GetString(constants.EnVDatabaseHost)
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

// Close closes the underlying database connection.
func (d *DB) Close() error {
	return d.client.Close()
}

func (d *DB) Ping() error {
	return d.client.Ping()
}
