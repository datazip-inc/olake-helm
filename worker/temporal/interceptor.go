package temporal

import (
	"context"
	"slices"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"go.temporal.io/sdk/interceptor"
)

// LoggingInterceptor automatically sets up workflow file logging for activities.
type LoggingInterceptor struct {
	interceptor.WorkerInterceptorBase
}

func NewLoggingInterceptor() *LoggingInterceptor {
	return &LoggingInterceptor{}
}

func (i *LoggingInterceptor) InterceptActivity(
	ctx context.Context,
	next interceptor.ActivityInboundInterceptor,
) interceptor.ActivityInboundInterceptor {
	return &loggingActivityInterceptor{
		ActivityInboundInterceptorBase: interceptor.ActivityInboundInterceptorBase{Next: next},
	}
}

type loggingActivityInterceptor struct {
	interceptor.ActivityInboundInterceptorBase
}

func (a *loggingActivityInterceptor) ExecuteActivity(
	ctx context.Context,
	in *interceptor.ExecuteActivityInput,
) (interface{}, error) {
	req := extractExecutionRequest(in.Args)
	if req == nil || req.WorkflowID == "" {
		return a.Next.ExecuteActivity(ctx, in)
	}

	ctxWithLogger, logFile, err := utils.PrepareWorkflowLogger(ctx, req.WorkflowID, req.Command)
	if err != nil {
		logger.Warnf("failed to prepare workflow logger for workflowID=%s: %s", req.WorkflowID, err)
		return a.Next.ExecuteActivity(ctx, in)
	}
	_, workDir := utils.GetWorkflowDirAndSubDir(req.WorkflowID, req.Command)
	defer func() {
		logFile.Close()
		if !slices.Contains(constants.AsyncCommands, req.Command) {
			utils.ReleaseWorkerLogWriter(workDir)
		}
	}()

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
