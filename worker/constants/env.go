package constants

const (
	// Database
	EnvDatabaseURL           = "DATABASE_URL"
	EnVDatabaseHost          = "DB_HOST"
	EnvDatabasePort          = "DB_PORT"
	EnvDatabaseUser          = "DB_USER"
	EnvDatabasePassword      = "DB_PASSWORD"
	EnvDatabaseDatabase      = "DB_NAME"
	EnvDatabaseSSLMode       = "DB_SSLMODE"
	EnvDatabaseRunMode       = "RUN_MODE"
	EnvMaxOpenConnections    = "DB_MAX_OPEN_CONNS"
	EnvMaxIdleConnections    = "DB_MAX_IDLE_CONNS"
	EnvConnectionMaxLifetime = "DB_CONN_MAX_LIFETIME"

	// temporal
	EnvTemporalAddress = "TEMPORAL_ADDRESS"

	// worker
	EnvLogRetentionPeriod = "LOG_RETENTION_PERIOD"
	EnvHostPersistentDir  = "PERSISTENT_DIR"

	// kubernetes
	EnvNamespace             = "WORKER_NAMESPACE"
	EnvStoragePVCName        = "OLAKE_STORAGE_PVC_NAME"
	EnvJobServiceAccountName = "JOB_SERVICE_ACCOUNT_NAME"
	EnvSecretKey             = "OLAKE_SECRET_KEY"
	EnvPodName               = "POD_NAME"
	EnvKubernetesServiceHost = "KUBERNETES_SERVICE_HOST"

	// logging
	EnvLogLevel  = "LOG_LEVEL"
	EnvLogFormat = "LOG_FORMAT"

	// api
	EnvCallbackURL = "OLAKE_CALLBACK_URL"
)
