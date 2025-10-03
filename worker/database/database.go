package database

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/spf13/viper"
)

type DB struct {
	db *sql.DB
}

func NewDatabase() (*DB, error) {
	host := viper.GetString(constants.EnVDatabaseHost)
	port := viper.GetString(constants.EnvDatabasePort)
	user := viper.GetString(constants.EnvDatabaseUser)
	password := viper.GetString(constants.EnvDatabasePassword)
	database := viper.GetString(constants.EnvDatabaseDatabase)
	sslmode := viper.GetString(constants.EnvDatabaseSSLMode)

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", host, port, user, password, database, sslmode)

	conn, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	if err := conn.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Optional: apply pooling settings if provided
	if viper.GetInt(constants.EnvMaxOpenConnections) > 0 {
		conn.SetMaxOpenConns(viper.GetInt(constants.EnvMaxOpenConnections))
	}
	if viper.GetInt(constants.EnvMaxIdleConnections) > 0 {
		conn.SetMaxIdleConns(viper.GetInt(constants.EnvMaxIdleConnections))
	}
	if viper.GetInt(constants.EnvConnectionMaxLifetime) > 0 {
		conn.SetConnMaxLifetime(time.Duration(viper.GetInt(constants.EnvConnectionMaxLifetime)) * time.Second)
	}

	return &DB{db: conn}, nil
}

func (d *DB) Close() error {
	return d.db.Close()
}
