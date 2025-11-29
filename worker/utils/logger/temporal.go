package logger

import (
	"go.temporal.io/sdk/log"
)

// temporalLogger wraps our existing logger functions to implement Temporal's interface
type temporalLogger struct{}

func (l temporalLogger) Debug(msg string, keyvals ...interface{}) {
	if len(keyvals) > 0 {
		rootLogger.Debug().Fields(keyvals).Msg(msg)
	} else {
		rootLogger.Debug().Msg(msg)
	}
}

func (l temporalLogger) Info(msg string, keyvals ...interface{}) {
	if len(keyvals) > 0 {
		rootLogger.Info().Fields(keyvals).Msg(msg)
	} else {
		rootLogger.Info().Msg(msg)
	}
}

func (l temporalLogger) Warn(msg string, keyvals ...interface{}) {
	if len(keyvals) > 0 {
		rootLogger.Warn().Fields(keyvals).Msg(msg)
	} else {
		rootLogger.Warn().Msg(msg)
	}
}

func (l temporalLogger) Error(msg string, keyvals ...interface{}) {
	if len(keyvals) > 0 {
		rootLogger.Error().Fields(keyvals).Msg(msg)
	} else {
		rootLogger.Error().Msg(msg)
	}
}

// NewTemporalLogger creates a Temporal-compatible logger using our existing zerolog logger
func NewTemporalLogger() log.Logger {
	return temporalLogger{}
}
