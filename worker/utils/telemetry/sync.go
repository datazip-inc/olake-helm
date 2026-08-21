package telemetry

// TrackSyncEvent sends a sync telemetry event. reason is optional context on
// why the connector was killed (e.g. "oom_killed") - only meaningful when
// event is TelemetryEventFailed; pass "" otherwise.
func TrackSyncEvent(ctx TelemetryContext, event TelemetryEvent, reason string) {
	props := ctx.Properties()
	if reason != "" {
		props["failure_reason"] = reason
	}
	SendEvent(func() EventPayload {
		return EventPayload{
			JobID:                ctx.JobID,
			ExecutionEnvironment: ctx.Environment,
			WorkflowID:           ctx.WorkflowID,
			SyncRunCount:         ctx.SyncRunCount,
			Event:                event,
			Properties:           props,
		}
	})
}
