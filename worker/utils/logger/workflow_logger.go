package logger

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/rs/zerolog"
)

// ctxKey is the key type for the logger in the context.
type ctxKey struct{}

// WorkflowLogFile holds the file handle for a workflow's log file.
type WorkflowLogFile struct {
	file *os.File
}

// Close must be called when the workflow finishes.
func (wf *WorkflowLogFile) Close() error {
	if wf == nil || wf.file == nil {
		return nil
	}
	return wf.file.Close()
}

// InitWorkflowLogger creates a zerolog.Logger instance that writes to both stdout and <workflowDir>/worker.log.
// Returns the logger instance and a file handle that must be closed when the workflow finishes.
// Note: workflowDir must already exist before calling this function.
func InitWorkflowLogger(ctx context.Context, workflowLogsDir string) (context.Context, *WorkflowLogFile, error) {
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
