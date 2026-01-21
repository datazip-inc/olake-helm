package temporal

import (
	"context"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/database"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"

	"github.com/datazip-inc/olake-helm/worker/executor"
	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	"google.golang.org/grpc/codes"
)

// Worker handles Temporal worker functionality
type Worker struct {
	worker   worker.Worker
	temporal *Temporal
	db       *database.DB
}

// NewWorker creates a new Temporal worker with the provided client
func NewWorker(ctx context.Context, t *Temporal, e *executor.AbstractExecutor, db *database.DB) (*Worker, error) {
	workerOptions := worker.Options{
		Interceptors: []interceptor.WorkerInterceptor{
			NewLoggingInterceptor(),
		},
	}
	w := worker.New(t.GetClient(), constants.TaskQueue, workerOptions)

	// regsiter workflows
	w.RegisterWorkflow(RunSyncWorkflow)
	w.RegisterWorkflow(ExecuteWorkflow)
	// w.RegisterWorkflow(ExecuteClearWorkflow)

	// regsiter activities
	activitiesInstance := NewActivity(e, db, t)
	w.RegisterActivity(activitiesInstance.ExecuteActivity)
	w.RegisterActivity(activitiesInstance.SyncActivity)
	w.RegisterActivity(activitiesInstance.PostSyncActivity)
	w.RegisterActivity(activitiesInstance.PostClearActivity)
	w.RegisterActivity(activitiesInstance.SendWebhookNotificationActivity)

	// Register search attributes
	// Namespace is required for SQL/Postgres visibility store, optional for Elasticsearch
	_, err := t.GetClient().OperatorService().AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
		SearchAttributes: map[string]enums.IndexedValueType{constants.OperationTypeKey: enums.INDEXED_VALUE_TYPE_KEYWORD},
		Namespace:        "default",
	})
	if err != nil && serviceerror.ToStatus(err).Code() != codes.AlreadyExists {
		return nil, err
	}

	logger.Infof("worker client created successfully")	

	return &Worker{
		worker:   w,
		temporal: t,
		db:       db,
	}, nil
}

// Start starts the worker
func (w *Worker) Start() error {
	logger.Info("starting Temporal worker...")
	return w.worker.Start()
}

// Stop stops the worker and closes the client
func (w *Worker) Stop() {
	w.worker.Stop()
}
