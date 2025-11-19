package constants

import (
	"time"

	"github.com/datazip-inc/olake-helm/worker/types"
)

const (
	DefaultDockerImagePrefix = "olakego/source"
	ContainerStopTimeout     = 5  // in seconds
	ContainerCleanupTimeout  = 30 // in seconds
	DefaultSyncTimeout       = time.Hour * 24 * 30
	TaskQueue                = "OLAKE_DOCKER_TASK_QUEUE"
	OperationTypeKey         = "OperationType"

	// Directory paths
	ContainerMountDir   = "/mnt/config"
	K8sPersistentDir    = "/data/olake-jobs"
	DockerPersistentDir = "/tmp/olake-config"
	OutputFileName      = "output.json"

	// File and directory permissions
	DefaultDirPermissions  = 0755
	DefaultFilePermissions = 0644
)

var AsyncCommands = []types.Command{types.Sync, types.ClearDestination}
