package logger

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/rs/zerolog"
)

// ctxKey is the key type for the logger in the context.
type ctxKey struct{}

// WorkflowLogFile holds the log sink for a workflow and must be closed when the workflow finishes.
type WorkflowLogFile struct {
	file    *os.File
	onClose func() error
}

// Close must be called when the workflow finishes.
func (wf *WorkflowLogFile) Close() error {
	if wf == nil {
		return nil
	}
	var err error
	switch {
	case wf.file != nil:
		err = wf.file.Close()
	case wf.onClose != nil:
		err = wf.onClose()
	}
	return err
}

// InitWorkflowLoggerForS3 creates a zerolog.Logger that writes to stdout and the given writer.
// workflowID and command are attached to every log line for S3 worker log routing.
func InitWorkflowLoggerForS3(ctx context.Context, workflowID, command string, fileWriter io.Writer, onClose func() error) (context.Context, *WorkflowLogFile, error) {
	stdoutWriter := createStdoutWriter()
	multiWriter := zerolog.MultiLevelWriter(stdoutWriter, fileWriter)
	var seq uint64
	log := zerolog.New(multiWriter).Hook(zerolog.HookFunc(func(e *zerolog.Event, _ zerolog.Level, _ string) {
		e.Uint64("seq", atomic.AddUint64(&seq, 1))
	})).With().Timestamp().Logger()
	if workflowID != "" {
		log = log.With().Str("workflowID", workflowID).Logger()
	}
	if command != "" {
		log = log.With().Str("command", command).Logger()
	}

	return CtxWithLogger(ctx, log), &WorkflowLogFile{onClose: onClose}, nil
}

// Note: workflowDir must already exist before calling this function.
func InitWorkflowLoggerForNFS(ctx context.Context, workflowLogsDir string) (context.Context, *WorkflowLogFile, error) {
	logFilePath := filepath.Join(workflowLogsDir, "worker.log")
	file, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, constants.DefaultFilePermissions)
	if err != nil {
		return ctx, nil, fmt.Errorf("failed to open worker.log: %w", err)
	}

	stdoutWriter := createStdoutWriter()
	multiWriter := zerolog.MultiLevelWriter(stdoutWriter, file)
	log := zerolog.New(multiWriter).With().Timestamp().Logger()
	logFile := &WorkflowLogFile{file: file}

	return CtxWithLogger(ctx, log), logFile, nil
}

// CtxWithLogger attaches a zerolog.Logger instance to the context.
func CtxWithLogger(ctx context.Context, log zerolog.Logger) context.Context {
	return context.WithValue(ctx, ctxKey{}, log)
}

// FromContext retrieves the logger instance from context, or returns the global logger.
func FromContext(ctx context.Context) zerolog.Logger {
	if ctx == nil {
		return rootLogger
	}
	if log, ok := ctx.Value(ctxKey{}).(zerolog.Logger); ok {
		return log
	}
	return rootLogger
}
