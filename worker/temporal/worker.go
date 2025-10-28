package temporal

import (
	"github.com/datazip-inc/olake-helm/worker/executor"
	"go.temporal.io/sdk/worker"
)

const TaskQueue = "OLAKE_DOCKER_TASK_QUEUE"

// Worker handles Temporal worker functionality
type Worker struct {
	worker   worker.Worker
	temporal *Temporal
}

// NewWorker creates a new Temporal worker with the provided client
func NewWorker(t *Temporal, e executor.AbstractExecutor) *Worker {
	w := worker.New(t.GetClient(), TaskQueue, worker.Options{})

	// regsiter workflows
	w.RegisterWorkflow(RunSyncWorkflow)
	w.RegisterWorkflow(ExecuteWorkflow)
	w.RegisterWorkflow(ExecuteClearWorkflow)

	// regsiter activities
	activitiesInstance := NewActivity(e, t)
	w.RegisterActivity(activitiesInstance.ExecuteActivity)
	w.RegisterActivity(activitiesInstance.ExecuteSyncActivity)
	w.RegisterActivity(activitiesInstance.SyncCleanupActivity)
	w.RegisterActivity(activitiesInstance.ClearCleanupActivity)

	return &Worker{
		worker:   w,
		temporal: t,
	}
}

// Start starts the worker
func (w *Worker) Start() error {
	return w.worker.Start()
}

// Stop stops the worker and closes the client
func (w *Worker) Stop() {
	w.worker.Stop()
	w.temporal.Close()
}
