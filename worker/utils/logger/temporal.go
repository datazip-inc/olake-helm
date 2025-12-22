package logger

import (
	"context"

	"go.temporal.io/sdk/log"
)

// contextAwareLogger implements Temporal's log.Logger but writes to workflow file if available.
type contextAwareLogger struct {
	ctx context.Context
}

func (l contextAwareLogger) Debug(msg string, keyvals ...interface{}) {
	logger := FromContext(l.ctx)
	if len(keyvals) > 0 {
		logger.Debug().Fields(keyvals).Msg(msg)
	} else {
		logger.Debug().Msg(msg)
	}
}

func (l contextAwareLogger) Info(msg string, keyvals ...interface{}) {
	logger := FromContext(l.ctx)
	if len(keyvals) > 0 {
		logger.Info().Fields(keyvals).Msg(msg)
	} else {
		logger.Info().Msg(msg)
	}
}

func (l contextAwareLogger) Warn(msg string, keyvals ...interface{}) {
	logger := FromContext(l.ctx)
	if len(keyvals) > 0 {
		logger.Warn().Fields(keyvals).Msg(msg)
	} else {
		logger.Warn().Msg(msg)
	}
}

func (l contextAwareLogger) Error(msg string, keyvals ...interface{}) {
	logger := FromContext(l.ctx)
	if len(keyvals) > 0 {
		logger.Error().Fields(keyvals).Msg(msg)
	} else {
		logger.Error().Msg(msg)
	}
}

// Log returns a Temporal-compatible logger that writes to workflow file if available.
// Usage: logger.Log(ctx).Info("message", "key", value)
func Log(ctx context.Context) log.Logger {
	return contextAwareLogger{ctx: ctx}
}
