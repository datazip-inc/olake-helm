package constants

import "github.com/datazip-inc/olake-helm/worker/types"

const (
	DefaultDockerImagePrefix = "olakego/source"

	// Directory paths
	ContainerMountDir   = "/mnt/config"
	K8sPersistentDir    = "/data/olake-jobs"
	DockerPersistentDir = "/tmp/olake-config"

	// File and directory permissions
	DefaultDirPermissions  = 0755
	DefaultFilePermissions = 0644
)

var AsyncCommands = []types.Command{types.Sync, types.ClearDestination}
