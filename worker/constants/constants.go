package constants

const (
	// Directory paths
	ContainerMountDir = "/mnt/config"

	// File and directory permissions
	DefaultDirPermissions  = 0755
	DefaultFilePermissions = 0644

	// Env

	// temporal
	EnvTemporalAddress = "TEMPORAL_ADDRESS"

	// worker
	EnvExecutorEnvironment    = "EXECUTOR_ENVIRONMENT"
	EnvHealthPort             = "HEALTH_PORT"
	EnvLogRetentionPeriod     = "LOG_RETENTION_PERIOD"
	EnvOlakeCallbackURL       = "OLAKE_CALLBACK_URL"
	EnvHostPersistentDir      = "HOST_PERSISTENT_DIR"      // TODO: check backward compatibility
	EnvContainerPersistentDir = "CONTAINER_PERSISTENT_DIR" // TODO: check backward compatibility

	// kubernetes
	EnvNamespace             = "WORKER_NAMESPACE"
	EnvStoragePVCName        = "OLAKE_STORAGE_PVC_NAME"
	EnvJobServiceAccountName = "JOB_SERVICE_ACCOUNT_NAME"
	EnvSecretKey             = "OLAKE_SECRET_KEY"

	// logging
	EnvLogLevel  = "LOG_LEVEL"
	EnvLogFormat = "LOG_FORMAT"

	// api
	EnvCallbackURL = "OLAKE_CALLBACK_URL"

	// docker
	EnvDockerImagePrefix = "DOCKER_IMAGE_PREFIX"
)
