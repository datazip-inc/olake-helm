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
	// absorb a slow volume detach from a dead node.
	UnschedulableGracePeriod = time.Minute * 10
	TaskQueue                = "OLAKE_DOCKER_TASK_QUEUE"
	OperationTypeKey         = "OperationType"
	DefaultTemporalNamespace = "default"

	// Directory paths
	// TODO: make persistent path alias same for both docker and k8s.
	ContainerMountDir   = "/mnt/config"
	K8sPersistentDir    = "/data/olake-jobs"
	DockerPersistentDir = "/tmp/olake-config"
	OutputFileName      = "output.json"
	TelemetryUserIDPath = "telemetry/user_id"

	// IndexDirName is the persistence-root subdirectory that holds one Pebble
	// index directory per job. Docker counterpart of the per-job index PVC.
	IndexDirName = "index"
	// DefaultIndexSize is the requested size of a job's index volume.
	DefaultIndexSize = "20Gi"
	// DefaultIndexMountPath is where the job's index volume is mounted inside the
	// driver container, in both the kubernetes and docker executors.
	DefaultIndexMountPath = "/var/lib/olake/index"
	// DefaultIndexCacheSizeMB is the Pebble block cache size, in megabytes.
	DefaultIndexCacheSizeMB = 512
	// DefaultIndexMaxOpenFiles caps the file descriptors Pebble keeps open.
	DefaultIndexMaxOpenFiles = 1000
	// IndexResizeTimeout bounds the wait for the CSI driver to grow the backing
	// device. The filesystem half runs at pod mount and is never waited for.
	IndexResizeTimeout = time.Minute * 10
	// IndexResizePollInterval is how often the claim is re-read while it grows.
	IndexResizePollInterval = time.Second * 5

	// File and directory permissions
	DefaultDirPermissions  = 0755
	DefaultFilePermissions = 0644

	StateFlag = "--state"

	// Storage modes (OLAKE_STORAGE_MODE values)
	StorageModeNFS = "nfs"
	StorageModeS3  = "s3"

	// Kubernetes ConfigMap names
	GlobalEnvConfigMap  = "olake-global-env"
	WorkersConfigMap    = "olake-workers-config"
	WorkerContainerName = "olake-workers"

	// S3 log chunk collection
	PodLogChunkMaxBytes   = 1 << 20 // 1 MiB — steady-state and max single-line size
	ConnectorLogDirPrefix = "sync_"
	PodLogFilenamePref    = "connector-"
	WorkerLogFilenamePref = "worker-"
	WorkerLogFileName     = "worker.log"
	WorkerLogRelDir       = "logs/worker" // S3: chunk directory prefix (NFS uses logs/worker.log via InitWorkflowLoggerForNFS)
)

var (
	// PodLogChunkThresholds are the first S3 upload sizes; subsequent chunks use PodLogChunkMaxBytes.
	PodLogChunkThresholds = []int{
		1 << 10,   // 1 KiB
		2 << 10,   // 2 KiB
		5 << 10,   // 5 KiB
		10 << 10,  // 10 KiB
		20 << 10,  // 20 KiB
		50 << 10,  // 50 KiB
		100 << 10, // 100 KiB
		200 << 10, // 200 KiB
		500 << 10, // 500 KiB
		1 << 20,   // 1 MiB
	}
	AsyncCommands = []types.Command{types.Sync, types.ClearDestination}
)
