// Package failure describes why a connector container/pod stopped and turns
// that into a message users can act on.
package failure

import (
	"fmt"
	"strings"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
)

// ExecutionFailure carries what the executor saw when the connector stopped
// with a failure. It unwraps to constants.ErrExecutionFailed.
type ExecutionFailure struct {
	Kind        string // "pod" or "container"
	Name        string
	Node        string // node the pod ran on (kubernetes only)
	ExitCode    *int
	Reason      string // e.g. OOMKilled, Evicted, Unschedulable, Error
	Message     string // reason detail reported by kubernetes/docker
	MemoryLimit string // "" means no limit
	NotFound    bool   // pod/container disappeared before it finished
	LogTail     string // last lines of connector stdout/stderr
}

// Error keeps the connector's last error line: discover/check/spec failures
// reach the UI through this text, and it is the only place their cause shows.
func (f *ExecutionFailure) Error() string {
	var info []string
	if f.NotFound {
		info = append(info, "not found while waiting for completion")
	}
	if f.ExitCode != nil {
		info = append(info, fmt.Sprintf("exit code: %d", *f.ExitCode))
	}
	if f.Reason != "" {
		info = append(info, "reason: "+f.Reason)
	}
	if f.Message != "" {
		info = append(info, "message: "+f.Message)
	}

	msg := fmt.Sprintf("%s: %s %s failed (%s)", constants.ErrExecutionFailed, f.Kind, f.Name, strings.Join(info, ", "))
	if line := lastErrorLine(f.LogTail); line != "" {
		msg += ": " + line
	}
	return msg
}

func (f *ExecutionFailure) Unwrap() error {
	return constants.ErrExecutionFailed
}

// Reasons the classification acts on. Kubernetes reports them on the pod or
// container status; the docker executor maps State.OOMKilled to ReasonOOMKilled.
const (
	ReasonOOMKilled        = "OOMKilled"
	reasonEvicted          = "Evicted"
	reasonDeadlineExceeded = "DeadlineExceeded"
)

// Category groups failures that share a user-facing explanation.
type Category string

const (
	CategoryNone    Category = ""
	CategoryOOM     Category = "oom"
	CategoryEvicted Category = "evicted"
)

const exitCodeSIGKILL = 137

// oomMarkers are log lines the connector prints when it runs out of memory
// without being killed (Go runtime, Iceberg Java writer).
var oomMarkers = []string{
	"runtime: out of memory",
	"java.lang.OutOfMemoryError",
}

// errorMarkers identify the connector output line that explains a failure:
// console log levels and Go crash output.
var errorMarkers = []string{" FATAL ", " ERROR ", "fatal error:", "panic:"}

// maxErrorLineLength bounds the connector line copied into errors.
const maxErrorLineLength = 1000

// classify maps a failure to a category. As the PRD asks, a forced termination
// (exit 137 without the OOMKilled reason, or a force-deleted pod) is treated as
// OOM: a manual kill looks the same, and OOM is far more likely.
func classify(f *ExecutionFailure) Category {
	switch {
	case f == nil:
		return CategoryNone
	case f.Reason == reasonEvicted:
		return CategoryEvicted
	case f.Reason == ReasonOOMKilled, f.NotFound, f.ExitCode != nil && *f.ExitCode == exitCodeSIGKILL, oomMarker(f.LogTail) != "":
		return CategoryOOM
	default:
		return CategoryNone
	}
}

// TelemetryReason reports whether the connector was killed externally, so it
// never sent its own exit telemetry. Returns "" when it exited on its own.
// Stricter than classify on purpose: a bare exit 137 is shown to users as OOM,
// but is not counted as oom_killed, so the usage stats stay exact.
func TelemetryReason(f *ExecutionFailure) string {
	switch {
	case f.NotFound:
		return "pod_removed"
	case f.Reason == reasonEvicted:
		return "evicted"
	case f.Reason == ReasonOOMKilled:
		return "oom_killed"
	case f.Reason == reasonDeadlineExceeded:
		return "deadline_exceeded"
	default:
		return ""
	}
}

// UserMessage is what the worker writes to worker.log for the user.
type UserMessage struct {
	Category Category
	Headline string
	Details  string
	Tip      string
}

const (
	tipIncreaseMemory = "Recommendation: Increase the memory/resources allocated to the OLake sync pod and retry the sync."
	tipEvictedMemory  = "Recommendation: The sync ran out of available memory. Try increasing the memory/resources allocated to the OLake sync pod and retry the sync."
)

// Describe builds the user-facing message for a failure. Failures without a
// known category get a single line.
func Describe(f *ExecutionFailure) UserMessage {
	category := classify(f)
	msg := UserMessage{Category: category}

	switch category {
	case CategoryOOM:
		msg.Headline = "Sync failed due to insufficient memory. The OLake sync pod ran out of available memory while processing this sync."
		msg.Details = oomDetails(f)
		msg.Tip = tipIncreaseMemory
	case CategoryEvicted:
		msg.Headline = "Sync failed because Kubernetes evicted the sync pod (resource limit or node pressure)."
		msg.Details = fmt.Sprintf("Details: %s %s evicted%s: %q", f.Kind, f.Name, onNode(f), strings.TrimSpace(f.Message))
		msg.Tip = tipEvictedMemory
	default:
		if f.ExitCode != nil {
			msg.Headline = fmt.Sprintf("Sync failed: connector exited with code %d. See Sync logs for the error.", *f.ExitCode)
		} else {
			msg.Headline = fmt.Sprintf("Sync failed: %s %s did not run to completion (reason: %s): %s", f.Kind, f.Name, f.Reason, f.Message)
		}
	}
	return msg
}

func oomDetails(f *ExecutionFailure) string {
	if marker := oomMarker(f.LogTail); marker != "" {
		return fmt.Sprintf("Details: connector crashed with %q%s", marker, exitCodeSuffix(f))
	}

	limit := "none (uses all available node/host memory)"
	if f.MemoryLimit != "" {
		limit = f.MemoryLimit
	}

	if f.NotFound {
		return fmt.Sprintf("Details: %s %s was force-terminated and removed before it finished (not found while waiting for completion)",
			f.Kind, f.Name)
	}
	if f.Reason == ReasonOOMKilled {
		return fmt.Sprintf("Details: %s %s was OOMKilled%s%s; memory limit: %s",
			f.Kind, f.Name, onNode(f), exitCodeSuffix(f), limit)
	}
	return fmt.Sprintf("Details: %s %s was force-killed%s (exit code %d, SIGKILL), which usually means it ran out of memory; memory limit: %s",
		f.Kind, f.Name, onNode(f), exitCodeSIGKILL, limit)
}

// oomMarker returns the log line that shows an out-of-memory crash, if any.
func oomMarker(logTail string) string {
	for _, line := range strings.Split(logTail, "\n") {
		if containsAny(line, oomMarkers) {
			return strings.TrimSpace(logger.StripANSI(line))
		}
	}
	return ""
}

// lastErrorLine returns the connector output line that best explains a failure:
// the last error/fatal/crash line, else the last non-empty line.
func lastErrorLine(logTail string) string {
	lines := strings.Split(logger.StripANSI(logTail), "\n")
	fallback := ""
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			continue
		}
		if fallback == "" {
			fallback = line
		}
		if containsAny(line, errorMarkers) || containsAny(line, oomMarkers) {
			return truncate(line)
		}
	}
	return truncate(fallback)
}

func containsAny(line string, markers []string) bool {
	for _, marker := range markers {
		if strings.Contains(line, marker) {
			return true
		}
	}
	return false
}

func truncate(line string) string {
	if len(line) > maxErrorLineLength {
		return line[:maxErrorLineLength] + "..."
	}
	return line
}

func onNode(f *ExecutionFailure) string {
	if f.Node == "" {
		return ""
	}
	return " on node " + f.Node
}

func exitCodeSuffix(f *ExecutionFailure) string {
	if f.ExitCode == nil {
		return ""
	}
	return fmt.Sprintf(" (exit code %d)", *f.ExitCode)
}
