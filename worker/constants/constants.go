package constants

const (
	// Default values
	DefaultOlakeCallbackURL    = "http://host.docker.internal:8000/internal/worker/callback"
	DefaultTemporalAddress     = "localhost:7233"
	DefaultExecutorEnvironment = "docker"
	DefaultHealthPort          = 8090
	DefaultLogRetentionPeriod  = 30

	// Environment variables
	EnvOlakeCallbackURL    = "OLAKE_CALLBACK_URL"
	EnvTemporalAddress     = "TEMPORAL_ADDRESS"
	EnvExecutorEnvironment = "EXECUTOR_ENVIRONMENT"
	EnvPersistentDir       = "PERSISTENT_DIR"
	EnvDockerImagePrefix   = "DOCKER_IMAGE_PREFIX"
	EnvHealthPort          = "HEALTH_PORT"
	EnvLogRetentionPeriod  = "LOG_RETENTION_PERIOD"

	// Directory paths
	DefaultConfigDir    = "/tmp/olake-config"
	DefaultK8sConfigDir = "/data/olake-jobs/"
	ContainerMountDir   = "/mnt/config"

	// File and directory permissions
	DefaultDirPermissions  = 0755
	DefaultFilePermissions = 0644

	// Docker
	DockerImagePrefix = "olakego/source"
)
