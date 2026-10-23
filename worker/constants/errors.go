package constants

import "errors"

// ErrExecutionFailed marks a connector container/pod that was started and then
// failed (non-zero exit, OOM kill, eviction, removal, never scheduled). The
// details travel in failure.ExecutionFailure; errors before the connector
// starts (image pull, container create) are not wrapped with it.
var ErrExecutionFailed = errors.New("execution failed")
