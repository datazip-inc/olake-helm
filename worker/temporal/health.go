package temporal

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
)

const healthPort = 8090

type Server struct {
	server    *http.Server
	worker    *Worker
	startTime time.Time
	db        *database.DB
}

type HealthResponse struct {
	Status    string            `json:"status"`
	Timestamp time.Time         `json:"timestamp"`
	Checks    map[string]string `json:"checks,omitempty"`
}

func NewHealthServer(worker *Worker, db *database.DB) *Server {
	mux := http.NewServeMux()

	hs := &Server{
		worker:    worker,
		startTime: time.Now(),
		db:        db,
		server: &http.Server{
			Addr:    fmt.Sprintf(":%d", healthPort),
			Handler: mux,
		},
	}

	// Endpoints: align with old worker
	mux.HandleFunc("/health", hs.healthHandler)
	mux.HandleFunc("/ready", hs.readinessHandler)
	mux.HandleFunc("/metrics", hs.metricsHandler)

	return hs
}

func (hs *Server) Start() error {
	logger.Infof("Starting health check server on port %d", healthPort)
	return hs.server.ListenAndServe()
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if status > 0 {
		w.WriteHeader(status)
	}
	_ = json.NewEncoder(w).Encode(v)
}

// Liveness: fail if Temporal client/worker are not present
func (hs *Server) healthHandler(w http.ResponseWriter, _ *http.Request) {
	response := HealthResponse{
		Status:    "healthy",
		Timestamp: time.Now(),
		Checks:    map[string]string{"worker": "running"},
	}

	if hs.worker.worker == nil || hs.worker.temporal.client == nil {
		response.Status = "unhealthy"
		response.Checks["worker"] = utils.Ternary(hs.worker.temporal.client == nil, "temporal_client_disconnected", "temporal_worker_failed").(string)
		writeJSON(w, http.StatusServiceUnavailable, response)
		return
	}

	writeJSON(w, http.StatusOK, response)
}

// Readiness: require Temporal client + worker + database initialised
func (hs *Server) readinessHandler(w http.ResponseWriter, req *http.Request) {
	response := HealthResponse{
		Status:    "ready",
		Timestamp: time.Now(),
		Checks: map[string]string{
			"temporal": "connected",
			"database": "unknown", // old worker reported DB; new worker has no DB
		},
	}

	// Check Temporal connection - verifies worker and client are both initialized.
	// Readiness requires both components to be available before accepting traffic:
	// - worker: Must be non-nil (initialization completed)
	// - temporalClient: Must be connected (can communicate with Temporal server)
	// This prevents routing requests to pods that can't process workflows/activities.
	if hs.worker == nil || hs.worker.temporal.client == nil {
		response.Status = "not_ready"
		response.Checks["temporal"] = "disconnected"
		logger.Debugf("Readiness check failed - Temporal not connected (worker: %v, client: %v)", hs.worker != nil, hs.worker != nil && hs.worker.temporal.client != nil)
		writeJSON(w, http.StatusServiceUnavailable, response)
		return
	}

	// Check database connectivity - ensures job metadata can be read/written.
	// Database access is required for:
	// - Fetching job configurations and state
	// - Updating job progress and results
	// - Temporal workflow coordination
	// Without database access, workflows will fail during execution.
	if hs.db.PingContext(req.Context()) == nil {
		response.Checks["database"] = "connected"
	} else {
		response.Status = "not_ready"
		response.Checks["database"] = "disconnected"
		logger.Debugf("Readiness check failed - Database ping failed")
	}

	// Set HTTP status code based on overall health
	if response.Status == "not_ready" {
		writeJSON(w, http.StatusServiceUnavailable, response)
		return
	}

	writeJSON(w, http.StatusOK, response)
}

// Metrics: align shape with old worker
func (hs *Server) metricsHandler(w http.ResponseWriter, _ *http.Request) {
	metrics := map[string]interface{}{
		"worker_status":  "running",
		"uptime_seconds": time.Since(hs.startTime).Seconds(),
		"timestamp":      time.Now(),
	}
	writeJSON(w, http.StatusOK, metrics)
}
