package constants

import "errors"

// ErrPodFailed is returned when a pod fails due to non-retryable application errors.
// Infrastructure failures (evictions, image pull errors, etc.) are NOT wrapped with this error.
var ErrPodFailed = errors.New("pod execution failed")
