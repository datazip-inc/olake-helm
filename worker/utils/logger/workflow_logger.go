package logger

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/rs/zerolog"
)

// ctxKey is the key type for the logger in the context.
type ctxKey struct{}

// WorkflowLogFile holds the NFS worker.log handle. S3 mode has no file to close.
type WorkflowLogFile struct {
	file *os.File
}

// Close must be called when the activity finishes. Nil-safe for S3 mode.
func (wf *WorkflowLogFile) Close() error {
	if wf == nil || wf.file == nil {
		return nil
	}
	return wf.file.Close()
}

// InitWorkflowLoggerForS3 logs to stdout and fileWriter with workflowID, command, and nextSeq.
// There is no file handle to close; the writer is released by the interceptor for
// discover/check, and by PostSync/PostClear for sync/clear-destination.
func InitWorkflowLoggerForS3(ctx context.Context, workflowID, command string, fileWriter io.Writer, nextSeq func() uint64) (context.Context, error) {
	stdoutWriter := createStdoutWriter()
	multiWriter := zerolog.MultiLevelWriter(stdoutWriter, fileWriter)
	log := zerolog.New(multiWriter).Hook(zerolog.HookFunc(func(e *zerolog.Event, _ zerolog.Level, _ string) {
		e.Uint64("seq", nextSeq())
	})).With().Timestamp().Logger()
	if workflowID != "" {
		log = log.With().Str("workflowID", workflowID).Logger()
	}
	if command != "" {
		log = log.With().Str("command", command).Logger()
	}

	return CtxWithLogger(ctx, log), nil
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
