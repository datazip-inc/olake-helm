package logger

import (
	"io"
	"os"
	"strings"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/rs/zerolog"
	"github.com/spf13/viper"
)

var rootLogger zerolog.Logger // global logger instance

func Init() {
	format := viper.GetString(constants.EnvLogFormat)
	level := viper.GetString(constants.EnvLogLevel)

	zerolog.TimestampFunc = func() time.Time { return time.Now().UTC() }

	var writer io.Writer
	if strings.EqualFold(format, "console") {
		writer = zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	} else {
		writer = os.Stdout
	}

	rootLogger = zerolog.New(writer).With().Timestamp().Logger()
	zerolog.SetGlobalLevel(parseLogLevel(level))
}

func parseLogLevel(levelStr string) zerolog.Level {
	switch strings.ToLower(levelStr) {
	case "debug":
		return zerolog.DebugLevel
	case "info":
		return zerolog.InfoLevel
	case "warn":
		return zerolog.WarnLevel
	case "error":
		return zerolog.ErrorLevel
	case "fatal":
		return zerolog.FatalLevel
	default:
		return zerolog.InfoLevel
	}
}

func Info(v ...interface{}) {
	logArgs(rootLogger.Info(), v...)
}

func Infof(format string, v ...interface{}) {
	rootLogger.Info().Msgf(format, v...)
}

func Warn(v ...interface{}) {
	logArgs(rootLogger.Warn(), v...)
}

func Warnf(format string, v ...interface{}) {
	rootLogger.Warn().Msgf(format, v...)
}

func Error(v ...interface{}) {
	logArgs(rootLogger.Error(), v...)
}

func Errorf(format string, v ...interface{}) {
	rootLogger.Error().Msgf(format, v...)
}

func Debug(v ...interface{}) {
	logArgs(rootLogger.Debug(), v...)
}

func Debugf(format string, v ...interface{}) {
	rootLogger.Debug().Msgf(format, v...)
}

func Fatal(v ...interface{}) {
	logArgs(rootLogger.Fatal(), v...)
	os.Exit(1)
}

func Fatalf(format string, v ...interface{}) {
	rootLogger.Fatal().Msgf(format, v...)
	os.Exit(1)
}
