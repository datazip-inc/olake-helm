// GitOps: Temporal workflow that spawns/deletes failure-indicator pods/containers for ConfigMap/Secret errors.
package temporal

import (
	"time"

	"github.com/datazip-inc/olake-helm/worker/types"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

var indicatorRetryPolicy = &temporal.RetryPolicy{
	InitialInterval:    time.Second * 2,
	BackoffCoefficient: 2.0,
	MaximumInterval:    time.Second * 10,
	MaximumAttempts:    5, // a respawn retries until the previous indicator pod is gone
}

// IndicatorWorkflow creates or deletes a GitOps failure indicator pod/container.
func IndicatorWorkflow(ctx workflow.Context, req types.IndicatorRequest) error {
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         indicatorRetryPolicy,
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)
	return workflow.ExecuteActivity(ctx, IndicatorActivity, req).Get(ctx, nil)
}
