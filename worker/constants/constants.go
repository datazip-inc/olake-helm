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
	DefaultTemporalNamespace = "default"

	// Directory paths
	// TODO: make persistent path alias same for both docker and k8s.
	ContainerMountDir   = "/mnt/config"
	K8sPersistentDir    = "/data/olake-jobs"
	DockerPersistentDir = "/tmp/olake-config"
	OutputFileName      = "output.json"
	TelemetryUserIDPath = "telemetry/user_id"

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
