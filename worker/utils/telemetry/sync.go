package telemetry

// TrackSyncEvent sends a sync telemetry event. reason is optional context on
// why the connector was killed (e.g. "oom_killed") - only meaningful when
// event is TelemetryEventFailed; pass "" otherwise.
func TrackSyncEvent(payload TelemetryPayload, event TelemetryEvent, reason string) {
	props := payload.Properties()
	if reason != "" {
		props["failure_reason"] = reason
	}
	SendEvent(EventPayload{
		JobID:      payload.JobID,
		WorkflowID: payload.WorkflowID,
		Event:      event,
		Properties: props,
	})
}
