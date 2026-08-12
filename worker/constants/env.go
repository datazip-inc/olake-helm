package constants

const (
	// Database
	EnvDatabaseURL           = "POSTGRES_DB"
	EnvDatabaseHost          = "DB_HOST"
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
	EnvTemporalAddress         = "TEMPORAL_ADDRESS"
	EnvTemporalRetentionPeriod = "TEMPORAL_RETENTION_PERIOD"
	EnvTemporalExternal        = "TEMPORAL_EXTERNAL"
	EnvTemporalAPIKey          = "TEMPORAL_API_KEY"
	EnvTemporalNamespace       = "TEMPORAL_NAMESPACE"
	EnvTemporalEnableTLS       = "TEMPORAL_ENABLE_TLS"
	EnvTemporalTaskQueue       = "TEMPORAL_TASK_QUEUE"

	// registry
	ContainerRegistryBase = "CONTAINER_REGISTRY_BASE"
	// Optional credentials for a generic private registry (Harbor, Nexus, Quay, GitLab,
	// registry:2). Used to authenticate Docker-mode image pulls without `docker login`.
	// Left empty for Docker Hub or for ECR/GCR (which authenticate via cloud IAM / host creds).
	EnvRegistryUsername = "CONTAINER_REGISTRY_USERNAME"
	EnvRegistryPassword = "CONTAINER_REGISTRY_PASSWORD"

	// worker
	EnvLogRetentionPeriod = "LOG_RETENTION_PERIOD"
	EnvHostPersistentDir  = "PERSISTENT_DIR"

	// kubernetes
	EnvNamespace             = "WORKER_NAMESPACE"
	EnvStorageMode           = "OLAKE_STORAGE_MODE"
	EnvStoragePVCName        = "OLAKE_STORAGE_PVC_NAME"
	EnvS3Bucket              = "OLAKE_S3_BUCKET"
	EnvS3Region              = "OLAKE_S3_REGION"
	EnvS3Prefix              = "OLAKE_S3_PREFIX"
	EnvS3Endpoint            = "OLAKE_S3_ENDPOINT"
	EnvS3AccessKeyID         = "OLAKE_S3_ACCESS_KEY_ID"
	EnvS3SecretAccessKey     = "OLAKE_S3_SECRET_ACCESS_KEY"
	EnvS3SessionToken        = "OLAKE_S3_SESSION_TOKEN"
	EnvS3CredentialsSecret   = "OLAKE_S3_CREDENTIALS_SECRET"
	EnvDockerNetwork         = "OLAKE_DOCKER_NETWORK"
	EnvJobServiceAccountName = "JOB_SERVICE_ACCOUNT_NAME"
	EnvSecretKey             = "OLAKE_SECRET_KEY"
	EnvPodName               = "POD_NAME"
	EnvKubernetesServiceHost = "KUBERNETES_SERVICE_HOST"

	// logging
	EnvLogLevel  = "LOG_LEVEL"
	EnvLogFormat = "LOG_FORMAT"

	// telemetry
	EnvTelemetryDisabled = "TELEMETRY_DISABLED"

	// api
	EnvCallbackURL = "OLAKE_CALLBACK_URL"

	// security context
	EnvPodSecurityContext = "POD_SECURITY_CONTEXT"

	// activity pod annotations
	EnvJobPodAnnotations = "OLAKE_JOB_POD_ANNOTATIONS"
)
