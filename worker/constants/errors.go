package constants

import "errors"

// ErrExecutionFailed is returned when a container/pod fails due to non-retryable application errors.
// Infrastructure failures (evictions, image pull errors, etc.) are NOT wrapped with this error.
var ErrExecutionFailed = errors.New("execution failed")
