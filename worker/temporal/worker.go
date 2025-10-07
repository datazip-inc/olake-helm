package temporal

import (
	"github.com/datazip-inc/olake-helm/worker/executor"
	"go.temporal.io/sdk/worker"
)

// TaskQueue is the default task queue for Olake Docker workflows
const TaskQueue = "OLAKE_DOCKER_TASK_QUEUE"

// Worker handles Temporal worker functionality
type Worker struct {
	worker worker.Worker
	client *Client
}

// NewWorker creates a new Temporal worker with the provided client
func NewWorker(c *Client, e executor.Executor) *Worker {
	w := worker.New(c.GetClient(), TaskQueue, worker.Options{})

	// regsiter workflows
	w.RegisterWorkflow(ExecuteSyncWorkflow)
	w.RegisterWorkflow(ExecuteWorkflow)

	// regsiter activities
	activitiesInstance := NewActivity(e)
	w.RegisterActivity(activitiesInstance.ExecuteActivity)
	w.RegisterActivity(activitiesInstance.ExecuteSyncActivity)

	return &Worker{
		worker: w,
		client: c,
	}
}

// Start starts the worker
func (w *Worker) Start() error {
	return w.worker.Start()
}

// Stop stops the worker and closes the client
func (w *Worker) Stop() {
	w.worker.Stop()
	w.client.Close()
}
