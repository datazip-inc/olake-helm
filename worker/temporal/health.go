package temporal

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/datazip-inc/olake-helm/worker/logger"
)

const healthPort = 8090

type Server struct {
	server    *http.Server
	worker    *Worker
	startTime time.Time
}

type HealthResponse struct {
	Status    string            `json:"status"`
	Timestamp time.Time         `json:"timestamp"`
	Checks    map[string]string `json:"checks,omitempty"`
}

func NewHealthServer(worker *Worker) *Server {
	mux := http.NewServeMux()

	hs := &Server{
		worker:    worker,
		startTime: time.Now(),
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

	if hs.worker.client == nil {
		response.Status = "unhealthy"
		response.Checks["worker"] = "temporal_client_disconnected"
		writeJSON(w, http.StatusServiceUnavailable, response)
		return
	}

	if hs.worker.worker == nil {
		response.Status = "unhealthy"
		response.Checks["worker"] = "temporal_worker_failed"
		writeJSON(w, http.StatusServiceUnavailable, response)
		return
	}

	writeJSON(w, http.StatusOK, response)
}

// Readiness: require Temporal client + worker initialized (no DB in new worker)
func (hs *Server) readinessHandler(w http.ResponseWriter, _ *http.Request) {
	response := HealthResponse{
		Status:    "ready",
		Timestamp: time.Now(),
		Checks: map[string]string{
			"temporal": "connected",
			"database": "unknown", // old worker reported DB; new worker has no DB
		},
	}

	if hs.worker == nil || hs.worker.client == nil {
		response.Status = "not_ready"
		response.Checks["temporal"] = "disconnected"
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
