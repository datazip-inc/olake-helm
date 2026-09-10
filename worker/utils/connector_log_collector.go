package utils

import (
	"context"

	"github.com/datazip-inc/olake-helm/worker/constants"
)

// NewConnectorLogCollector tails connector logs, buffers locally, and uploads chunks to S3.
func NewConnectorLogCollector(ctx context.Context, workDir string, streamLogs StreamFunc, stillRunning StillRunningFunc) (*RuntimeLogCollector, error) {
	buffer, lastLogTimestamp, lastLogSeq, err := newConnectorPodLogBufferForWorkDir(ctx, workDir, constants.PodLogFilenamePref)
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

	collector.processLine = func(ctx context.Context, rawLogLine string) error {
		normalizedLogLine, ok := parsePodLogLine(rawLogLine)
		if !ok {
			return nil
		}
		if normalizedLogLine.Seq > 0 {
			if normalizedLogLine.Seq <= lastLogSeq {
				return nil
			}
			lastLogSeq = normalizedLogLine.Seq
		}

		collector.lastLogTimestampMu.Lock()
		collector.lastLogTimestamp = normalizedLogLine.PodLogTimestamp
		collector.lastLogTimestampMu.Unlock()

		return collector.buffer.WriteLine(ctx, normalizedLogLine)
	}

	return collector, nil
}
