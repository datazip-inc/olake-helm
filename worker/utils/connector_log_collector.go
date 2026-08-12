package utils

import (
	"context"

	"github.com/datazip-inc/olake-helm/worker/constants"
)

// NewConnectorLogCollector tails connector logs, buffers locally, and uploads chunks to S3.
func NewConnectorLogCollector(ctx context.Context, workDir string, streamLogs StreamFunc, stillRunning StillRunningFunc) (*RuntimeLogCollector, error) {
	buffer, lastLogTimestamp, err := newConnectorPodLogBufferForWorkDir(ctx, workDir, constants.PodLogFilenamePref)
	if err != nil {
		return nil, err
	}

	collector := &RuntimeLogCollector{
		buffer:           buffer,
		lastLogTimestamp: lastLogTimestamp,
		streamLogs:       streamLogs,
		stillRunning:     stillRunning,
		done:             make(chan struct{}),
	}

	collector.processLine = func(ctx context.Context, line string) error {
		normalized, lastLogTimestamp, ok := NormalizePodLogLine(line)
		if !ok {
			return nil
		}

		collector.lastLogTimestampMu.Lock()
		collector.lastLogTimestamp = lastLogTimestamp
		collector.lastLogTimestampMu.Unlock()

		return collector.buffer.WriteLine(ctx, []byte(normalized), lastLogTimestamp)
	}

	return collector, nil
}
