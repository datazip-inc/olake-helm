package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// olake_sync_status value encoding.
const (
	StatusRunning   = 0
	StatusSucceeded = 1
	StatusFailed    = 2
)

var labelNames = []string{"job_id", "job_name", "source_name", "destination_name"}

var (
	Registry = prometheus.NewRegistry()

	SyncStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "olake_sync_status",
		Help: "Status of the most recent sync run: 0=running, 1=succeeded, 2=failed.",
	}, labelNames)

	SyncStartTime = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "olake_sync_start_time_seconds",
		Help: "Unix timestamp when the current/last sync run started.",
	}, labelNames)

	SyncDuration = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "olake_sync_duration_seconds",
		Help: "Duration of the sync run in seconds. Live while running; frozen at completion.",
	}, labelNames)

	// Gauges, not Counters: the underlying pool counters roll BACK on
	// failed/retried chunks, so values are not monotonic.
	SyncRecordsIngested = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "olake_sync_records_ingested",
		Help: "Records ingested in the current/last sync run (rollback-corrected; committed-only at completion).",
	}, labelNames)

	SyncBytesCommitted = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "olake_sync_bytes_committed",
		Help: "Source data bytes processed and committed in the current/last sync run.",
	}, labelNames)

	// Pass-through gauges: values re-exported verbatim from stats.json.
	SyncCPURatio = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "olake_process_cpu_usage_ratio",
		Help: "CPU utilization ratio (0-1) self-reported by the sync process via stats.json (currently a system-wide sample, not process-scoped).",
	}, labelNames)

	SyncMemoryBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "olake_process_memory_usage_bytes",
		Help: "Memory usage in bytes self-reported by the sync process via stats.json (currently a system-wide sample, not process-scoped).",
	}, labelNames)
)

func init() {
	Registry.MustRegister(
		SyncStatus, SyncStartTime, SyncDuration,
		SyncRecordsIngested, SyncBytesCommitted, SyncCPURatio, SyncMemoryBytes,
		collectors.NewGoCollector(),                                       // worker runtime
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}), // worker process CPU/Memory
	)
}

// Handler returns the Prometheus text-exposition handler for the worker registry.
func Handler() http.Handler {
	return promhttp.HandlerFor(Registry, promhttp.HandlerOpts{})
}
