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
	// UnschedulableGracePeriod is how long a pod may stay Pending for a reason
	// that cannot resolve itself before the run is failed. Generous enough to
	// absorb a slow volume detach from a dead node (~6 minutes worst case).
	UnschedulableGracePeriod = time.Minute * 10
	// VolumeAttachGracePeriod is how long a scheduled pod may keep reporting
	// volume attach or mount failures before the run is failed. Attaching a
	// block volume normally takes 10-60s; a volume left attached to a node that
	// died is force-detached after ~6 minutes.
	VolumeAttachGracePeriod  = time.Minute * 15
	TaskQueue                = "OLAKE_DOCKER_TASK_QUEUE"
	OperationTypeKey         = "OperationType"
	DefaultTemporalNamespace = "default"

	// Directory paths
	// TODO: make persistent path alias same for both docker and k8s.
	ContainerMountDir   = "/mnt/config"
	K8sPersistentDir    = "/data/olake-jobs"
	DockerPersistentDir = "/tmp/olake-config"
	OutputFileName      = "output.json"

	// IndexDirName is the persistence-root subdirectory that holds one Pebble
	// index directory per job. Docker counterpart of the per-job index PVC.
	IndexDirName = "index"
	// DefaultIndexMountPath is where the job's index volume is mounted inside the
	// connector container, in both the kubernetes and docker executors.
	DefaultIndexMountPath = "/var/lib/olake/index"
	// DefaultIndexCacheSizeMB is the Pebble block cache size, in megabytes.
	DefaultIndexCacheSizeMB = 512
	// DefaultIndexMaxOpenFiles caps the file descriptors Pebble keeps open.
	DefaultIndexMaxOpenFiles = 1000

	// File and directory permissions
	DefaultDirPermissions  = 0755
	DefaultFilePermissions = 0644

	StateFlag = "--state"
)

var AsyncCommands = []types.Command{types.Sync, types.ClearDestination}
