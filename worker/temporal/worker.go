package temporal

import (
	"context"

	"github.com/datazip-inc/olake-helm/worker/executor"
	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/worker"
	"google.golang.org/grpc/codes"
)

const TaskQueue = "OLAKE_DOCKER_TASK_QUEUE"

// Worker handles Temporal worker functionality
type Worker struct {
	worker   worker.Worker
	temporal *Temporal
}

// NewWorker creates a new Temporal worker with the provided client
func NewWorker(t *Temporal, e executor.AbstractExecutor) (*Worker, error) {
	w := worker.New(t.GetClient(), TaskQueue, worker.Options{})

	// regsiter workflows
	w.RegisterWorkflow(RunSyncWorkflow)
	w.RegisterWorkflow(ExecuteWorkflow)
	// w.RegisterWorkflow(ExecuteClearWorkflow)

	// regsiter activities
	activitiesInstance := NewActivity(e, t)
	w.RegisterActivity(activitiesInstance.ExecuteActivity)
	w.RegisterActivity(activitiesInstance.ExecuteSyncActivity)
	w.RegisterActivity(activitiesInstance.SyncCleanupActivity)
	w.RegisterActivity(activitiesInstance.ClearCleanupActivity)

	// Register search attributes (silently ignore AlreadyExists)
	_, err := t.GetClient().OperatorService().AddSearchAttributes(context.Background(), &operatorservice.AddSearchAttributesRequest{
		SearchAttributes: map[string]enums.IndexedValueType{"OperationType": enums.INDEXED_VALUE_TYPE_KEYWORD},
	})
	if err != nil && serviceerror.ToStatus(err).Code() != codes.AlreadyExists {
		return nil, err
	}

	return &Worker{
		worker:   w,
		temporal: t,
	}, nil
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
