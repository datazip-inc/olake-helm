package constants

const (
	// Default values
	DefaultTemporalAddress     = "localhost:7233"
	DefaultExecutorEnvironment = "k8s"

	// Directory paths
	DefaultConfigDir  = "/tmp/olake-config"
	ContainerMountDir = "/mnt/config"

	// Environment variables
	EnvTemporalAddress     = "TEMPORAL_ADDRESS"
	EnvExecutorEnvironment = "EXECUTOR_ENVIRONMENT"
	EnvPersistentDir       = "PERSISTENT_DIR"
	EnvDockerImagePrefix   = "DOCKER_IMAGE_PREFIX"

	// File and directory permissions
	DefaultDirPermissions  = 0755
	DefaultFilePermissions = 0644

	// Docker
	DockerImagePrefix = "olakego/source"
)
