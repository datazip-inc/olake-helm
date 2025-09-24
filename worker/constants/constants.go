package constants

const (
	// Default values
	DefaultTemporalAddress     = "localhost:7233"
	DefaultExecutorEnvironment = "docker"
	DefaultOlakeUIWebhookURL   = "http://host.docker.internal:8000/internal/worker/sync/callback"

	// Directory paths
	DefaultConfigDir  = "/tmp/olake-config"
	ContainerMountDir = "/mnt/config"

	// Environment variables
	EnvTemporalAddress     = "TEMPORAL_ADDRESS"
	EnvExecutorEnvironment = "EXECUTOR_ENVIRONMENT"
	EnvPersistentDir       = "PERSISTENT_DIR"
	EnvDockerImagePrefix   = "DOCKER_IMAGE_PREFIX"
	EnvOlakeUIWebhookURL   = "OLAKE_UI_WEBHOOK_URL"

	// File and directory permissions
	DefaultDirPermissions  = 0755
	DefaultFilePermissions = 0644

	// Docker
	DockerImagePrefix = "olakego/source"
)
