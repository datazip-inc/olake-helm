package temporal

import (
	"context"

	"github.com/datazip-inc/olake-helm/worker/executor"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"go.temporal.io/sdk/interceptor"
)

// LoggingInterceptor automatically sets up workflow file logging for activities.
type LoggingInterceptor struct {
	interceptor.WorkerInterceptorBase
	exec *executor.AbstractExecutor
}

func NewLoggingInterceptor(exec *executor.AbstractExecutor) *LoggingInterceptor {
	return &LoggingInterceptor{exec: exec}
}

func (i *LoggingInterceptor) InterceptActivity(
	ctx context.Context,
	next interceptor.ActivityInboundInterceptor,
) interceptor.ActivityInboundInterceptor {
	return &loggingActivityInterceptor{
		ActivityInboundInterceptorBase: interceptor.ActivityInboundInterceptorBase{Next: next},
		exec:                           i.exec,
	}
}

type loggingActivityInterceptor struct {
	interceptor.ActivityInboundInterceptorBase
	exec *executor.AbstractExecutor
}

func (a *loggingActivityInterceptor) ExecuteActivity(
	ctx context.Context,
	in *interceptor.ExecuteActivityInput,
) (interface{}, error) {
	req := extractExecutionRequest(in.Args)
	if req == nil || req.WorkflowID == "" {
		return a.Next.ExecuteActivity(ctx, in)
	}

	ctxWithLogger, logFile, err := utils.PrepareWorkflowLogger(ctx, req.WorkflowID, req.Command, a.exec.NewWorkerLogCollector, a.exec.NewConnectorLogCollector)
	if err != nil {
		logger.Warnf("failed to prepare workflow logger for workflowID=%s: %s", req.WorkflowID, err)
		return a.Next.ExecuteActivity(ctx, in)
	}
	defer logFile.Close()

	return a.Next.ExecuteActivity(ctxWithLogger, in)
}

func extractExecutionRequest(args []interface{}) *types.ExecutionRequest {
	for _, arg := range args {
		if req, ok := arg.(*types.ExecutionRequest); ok {
			return req
		}
	}
	return nil
}
