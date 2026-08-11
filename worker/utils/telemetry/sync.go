package telemetry

import (
	"context"

	"github.com/datazip-inc/olake-helm/worker/types"
	"go.temporal.io/sdk/client"
)

// TrackSyncStarted sends the "started" event, including the sync-run
// ordinal (computed inside SendEvent's goroutine, off the sync's critical
// path). attempt is activity.GetInfo(ctx).Attempt from the caller.
func TrackSyncStarted(tempClient client.Client, req *types.ExecutionRequest, environment string, attempt int) {
	SendEvent(func() EventPayload {
		return EventPayload{
			JobID:                req.JobID,
			ExecutionEnvironment: environment,
			WorkflowID:           req.WorkflowID,
			SyncRunCount:         GetOrIncrementSyncRunCount(context.Background(), tempClient, req, attempt),
			Event:                TelemetryEventStarted,
		}
	})
}

// TrackSyncFailed sends the "failed" event.
func TrackSyncFailed(req *types.ExecutionRequest, environment string) {
	SendEvent(func() EventPayload {
		return EventPayload{
			JobID:                req.JobID,
			ExecutionEnvironment: environment,
			WorkflowID:           req.WorkflowID,
			SyncRunCount:         ReadSyncRunCount(req.JobID),
			Event:                TelemetryEventFailed,
		}
	})
}

// TrackSyncCompleted sends the "completed" event.
func TrackSyncCompleted(req *types.ExecutionRequest, environment string) {
	SendEvent(func() EventPayload {
		return EventPayload{
			JobID:                req.JobID,
			ExecutionEnvironment: environment,
			WorkflowID:           req.WorkflowID,
			SyncRunCount:         ReadSyncRunCount(req.JobID),
			Event:                TelemetryEventCompleted,
		}
	})
}

// TrackSyncCancelled sends the "cancelled" event.
func TrackSyncCancelled(req *types.ExecutionRequest, environment string) {
	SendEvent(func() EventPayload {
		return EventPayload{
			JobID:                req.JobID,
			ExecutionEnvironment: environment,
			WorkflowID:           req.WorkflowID,
			SyncRunCount:         ReadSyncRunCount(req.JobID),
			Event:                TelemetryEventCancelled,
		}
	})
}
