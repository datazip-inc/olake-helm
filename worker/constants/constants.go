package constants

const (
	DefaultDockerImagePrefix = "olakego/source"
	ContainerStopTimeout     = 5  // in second
	ContainerCleanupTimeout  = 30 // in second

	// Directory paths
	ContainerMountDir   = "/mnt/config"
	K8sPersistentDir    = "/data/olake-jobs"
	DockerPersistentDir = "/tmp/olake-config"

	// File and directory permissions
	DefaultDirPermissions  = 0755
	DefaultFilePermissions = 0644
)
