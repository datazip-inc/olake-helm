package utils

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"sync/atomic"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
)

type workflowLogKey struct {
	workflowID string
	command    types.Command
}

// workerLogWriter writes live worker logs into the same S3 chunk buffer connector logs use.
type workerLogWriter struct {
	buffer *PodLogBuffer
	seq    atomic.Uint64
	mu     sync.Mutex
}

func newWorkerLogWriter(ctx context.Context, workDir string) (*workerLogWriter, error) {
	resume, err := loadResumePoint(ctx, workDir, constants.WorkerLogRelDir, constants.WorkerLogFilenamePref)
	if err != nil {
		return nil, err
	}
	buffer, err := NewPodLogBuffer(workDir, resume.logDir, constants.WorkerLogFilenamePref, resume.chunkCounter)
	if err != nil {
		return nil, err
	}

	writer := &workerLogWriter{buffer: buffer}
	writer.seq.Store(resume.lastPodLogSeq)
	return writer, nil
}

func (w *workerLogWriter) nextSeq() uint64 {
	return w.seq.Add(1)
}

func (w *workerLogWriter) Write(logLine []byte) (int, error) {
	if len(logLine) == 0 {
		return 0, nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	var parsed podLogLineEntry
	_ = json.Unmarshal(logLine, &parsed)
	parsed.normalizedLogLine = string(logLine)

	writeCtx, cancel := context.WithTimeout(context.Background(), logFinishTimeout)
	defer cancel()
	if err := w.buffer.WriteLine(writeCtx, parsed); err != nil {
		logger.Warnf("failed to persist worker log line: %s", err)
		return 0, err
	}
	return len(logLine), nil
}

func (w *workerLogWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	flushCtx, cancel := context.WithTimeout(context.Background(), logFinishTimeout)
	defer cancel()
	return w.buffer.Flush(flushCtx)
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
// Only lines that carry workflowID are recovered; inheriting it from a previous
// line would pull in unrelated stdout (startup logs, debug prints) and write
// seq-less chunks that reset resume seq to 0.
func groupWorkerLogLines(reader io.Reader) (map[workflowLogKey][]podLogLineEntry, error) {
	groupedNormalizedLogLines := make(map[workflowLogKey][]podLogLineEntry)

	err := readPodLogStream(reader, func(rawLogLine string) error {
		normalizedLogLine, ok := parsePodLogLine(rawLogLine)
		if !ok || normalizedLogLine.WorkflowID == "" {
			return nil
		}

		command := Ternary(normalizedLogLine.Command == "", types.Sync, types.Command(normalizedLogLine.Command)).(types.Command)
		key := workflowLogKey{workflowID: normalizedLogLine.WorkflowID, command: command}
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
	resume, err := loadResumePoint(ctx, workDir, constants.WorkerLogRelDir, constants.WorkerLogFilenamePref)
	if err != nil {
		return err
	}
	buffer, err := NewPodLogBuffer(workDir, resume.logDir, constants.WorkerLogFilenamePref, resume.chunkCounter)
	if err != nil {
		return err
	}
	lastLogSeq := resume.lastPodLogSeq

	for _, normalizedLogLine := range normalizedLogLines {
		if normalizedLogLine.Seq > 0 && normalizedLogLine.Seq <= lastLogSeq {
			continue
		}
		if err := buffer.WriteLine(ctx, normalizedLogLine); err != nil {
			return err
		}
		if normalizedLogLine.Seq > 0 {
			lastLogSeq = normalizedLogLine.Seq
		}
	}
	return buffer.Flush(ctx)
}
