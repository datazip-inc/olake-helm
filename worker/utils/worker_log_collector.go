package utils

import (
	"context"
	"io"
	"time"

	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
)

type workflowLogKey struct {
	workflowID string
	command    types.Command
}

// NewWorkerLogCollector tails worker container logs via the runtime API and uploads chunks to S3.
func NewWorkerLogCollector(ctx context.Context, workflowID, workDir string, streamLogs StreamFunc) (*RuntimeLogCollector, error) {
	buffer, lastLogTimestamp, lastLogSeq, err := newWorkerPodLogBufferForWorkDir(ctx, workDir)
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

	runtimeLogCollector.processLine = func(ctx context.Context, rawLogLine string) error {
		normalizedLogLine, ok := parsePodLogLine(rawLogLine)
		if !ok {
			return nil
		}
		// Only lines tagged for this activity; skip startup/root-logger noise without workflowID.
		if normalizedLogLine.WorkflowID != workflowID {
			return nil
		}
		if normalizedLogLine.Seq > 0 {
			if normalizedLogLine.Seq <= lastLogSeq {
				return nil
			}
			lastLogSeq = normalizedLogLine.Seq
		}

		runtimeLogCollector.lastLogTimestampMu.Lock()
		runtimeLogCollector.lastLogTimestamp = normalizedLogLine.PodLogTimestamp
		runtimeLogCollector.lastLogTimestampMu.Unlock()

		return runtimeLogCollector.buffer.WriteLine(ctx, normalizedLogLine)
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

	groupedNormalizedLogLines, err := groupWorkerLogLines(reader)
	if err != nil {
		return err
	}
	for key, normalizedLogLines := range groupedNormalizedLogLines {
		if err := appendWorkerLogLines(ctx, key.workflowID, key.command, normalizedLogLines); err != nil {
			logger.Warnf("failed to recover worker logs for workflowID=%s: %s", key.workflowID, err)
		}
	}
	return nil
}

// groupWorkerLogLines groups worker log lines by workflowID and command.
func groupWorkerLogLines(reader io.Reader) (map[workflowLogKey][]podLogLineEntry, error) {
	groupedNormalizedLogLines := make(map[workflowLogKey][]podLogLineEntry)
	var workflowID string
	var command types.Command

	err := readPodLogStream(reader, func(rawLogLine string) error {
		normalizedLogLine, ok := parsePodLogLine(rawLogLine)
		if !ok {
			return nil
		}

		if normalizedLogLine.WorkflowID != "" {
			workflowID = normalizedLogLine.WorkflowID
		}
		if workflowID == "" {
			return nil
		}
		if normalizedLogLine.Command != "" {
			command = types.Command(normalizedLogLine.Command)
		}
		if command == "" {
			command = types.Sync
		}
		key := workflowLogKey{workflowID: workflowID, command: command}
		groupedNormalizedLogLines[key] = append(groupedNormalizedLogLines[key], normalizedLogLine)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return groupedNormalizedLogLines, nil
}

// appendWorkerLogLines appends worker log lines to the buffer and flushes them to S3.
func appendWorkerLogLines(ctx context.Context, workflowID string, command types.Command, normalizedLogLines []podLogLineEntry) error {
	_, workDir := GetWorkflowDirAndSubDir(workflowID, command)
	buffer, _, lastLogSeq, err := newWorkerPodLogBufferForWorkDir(ctx, workDir)
	if err != nil {
		return err
	}

	for _, normalizedLogLine := range normalizedLogLines {
		if normalizedLogLine.Seq > 0 {
			if normalizedLogLine.Seq <= lastLogSeq {
				continue
			}
			lastLogSeq = normalizedLogLine.Seq
		}
		if err := buffer.WriteLine(ctx, normalizedLogLine); err != nil {
			return err
		}
	}
	return buffer.Flush(ctx)
}
