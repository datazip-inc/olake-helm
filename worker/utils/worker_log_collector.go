package utils

import (
	"context"
	"encoding/json"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
)

type workflowLogKey struct {
	workflowID string
	command    types.Command
}

type workerLogLine struct {
	podTimestamp time.Time
	line         []byte
}

// NewWorkerLogCollector tails worker container logs via the runtime API and uploads chunks to S3.
func NewWorkerLogCollector(ctx context.Context, workflowID, workDir string, streamLogs StreamFunc) (*RuntimeLogCollector, error) {
	buffer, lastLogTimestamp, err := newWorkerPodLogBufferForWorkDir(ctx, workDir)
	if err != nil {
		return nil, err
	}

	// First collection for this workflow: tail from activity start, not entire container history.
	activityStart := time.Now().UTC()
	if lastLogTimestamp.IsZero() {
		lastLogTimestamp = activityStart.Add(-2 * time.Second)
	}

	runtimeLogCollector := &RuntimeLogCollector{
		buffer:           buffer,
		lastLogTimestamp: lastLogTimestamp,
		streamLogs:       streamLogs,
		done:             make(chan struct{}),
	}

	runtimeLogCollector.processLine = func(ctx context.Context, line string) error {
		normalized, lastLogTimestamp, ok := NormalizePodLogLine(line)
		if !ok {
			return nil
		}

		var entry struct {
			WorkflowID string `json:"workflowID"`
		}
		if err := json.Unmarshal([]byte(strings.TrimSpace(normalized)), &entry); err != nil {
			return nil
		}
		// Only lines tagged for this activity; skip startup/root-logger noise without workflowID.
		if entry.WorkflowID != workflowID {
			return nil
		}

		runtimeLogCollector.lastLogTimestampMu.Lock()
		runtimeLogCollector.lastLogTimestamp = lastLogTimestamp
		runtimeLogCollector.lastLogTimestampMu.Unlock()

		return runtimeLogCollector.buffer.WriteLine(ctx, []byte(normalized), lastLogTimestamp)
	}

	return runtimeLogCollector, nil
}

// RecoverWorkerLogs reads a one-shot runtime log stream and uploads missing lines per workflow.
func RecoverWorkerLogs(ctx context.Context, streamLogs func(ctx context.Context) (io.Reader, error)) error {
	reader, err := streamLogs(ctx)
	if err != nil {
		return err
	}
	if reader == nil {
		return nil
	}
	if closer, ok := reader.(io.Closer); ok {
		defer closer.Close()
	}

	groupedLogLines, err := groupWorkerLogLines(reader)
	if err != nil {
		return err
	}
	for key, lines := range groupedLogLines {
		if err := appendWorkerLogLines(ctx, key.workflowID, key.command, lines); err != nil {
			logger.Warnf("failed to recover worker logs for workflowID=%s: %s", key.workflowID, err)
		}
	}
	return nil
}

// groupWorkerLogLines groups worker log lines by workflowID and command.
func groupWorkerLogLines(reader io.Reader) (map[workflowLogKey][]workerLogLine, error) {
	groupedLogLines := make(map[workflowLogKey][]workerLogLine)
	var workflowID string
	var command types.Command

	err := readPodLogStream(reader, func(line string) error {
		normalized, podTS, ok := NormalizePodLogLine(line)
		if !ok {
			return nil
		}

		var entry struct {
			WorkflowID string `json:"workflowID"`
			Command    string `json:"command"`
		}
		if err := json.Unmarshal([]byte(strings.TrimSpace(normalized)), &entry); err != nil {
			return nil
		}
		if entry.WorkflowID != "" {
			workflowID = entry.WorkflowID
		}
		if entry.Command != "" {
			command = types.Command(entry.Command)
		}
		if workflowID == "" {
			return nil
		}
		if command == "" {
			command = types.Sync
		}
		key := workflowLogKey{workflowID: workflowID, command: command}
		groupedLogLines[key] = append(groupedLogLines[key], workerLogLine{podTimestamp: podTS, line: []byte(normalized)})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return groupedLogLines, nil
}

// appendWorkerLogLines appends worker log lines to the buffer and flushes them to S3.
func appendWorkerLogLines(ctx context.Context, workflowID string, command types.Command, lines []workerLogLine) error {
	sort.Slice(lines, func(i, j int) bool {
		return lines[i].podTimestamp.Before(lines[j].podTimestamp)
	})

	_, workDir := GetWorkflowDirAndSubDir(workflowID, command)
	buffer, lastTS, err := newWorkerPodLogBufferForWorkDir(ctx, workDir)
	if err != nil {
		return err
	}

	for _, line := range lines {
		if !lastTS.IsZero() && !line.podTimestamp.IsZero() && !line.podTimestamp.After(lastTS) {
			continue
		}
		if err := buffer.WriteLine(ctx, line.line, line.podTimestamp); err != nil {
			return err
		}
	}
	return buffer.Flush(ctx)
}
