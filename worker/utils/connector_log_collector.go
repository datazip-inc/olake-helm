package utils

import (
	"context"
	"path"

	"github.com/datazip-inc/olake-helm/worker/constants"
)

// NewConnectorLogCollector tails connector logs, buffers locally, and uploads chunks to S3.
func NewConnectorLogCollector(ctx context.Context, workDir string, streamLogs StreamFunc, stillRunning StillRunningFunc) (*RuntimeLogCollector, error) {
	currentLogDir, err := resolveCurrentLogDir(ctx, workDir)
	if err != nil {
		return nil, err
	}
	resume, err := loadResumePoint(ctx, workDir, path.Join("logs", currentLogDir), constants.PodLogFilenamePref)
	if err != nil {
		return nil, err
	}
	buffer, err := NewPodLogBuffer(workDir, resume.logDir, constants.PodLogFilenamePref, resume.chunkCounter)
	if err != nil {
		return nil, err
	}
	collector := &RuntimeLogCollector{
		buffer:              buffer,
		lastPodLogTimestamp: resume.lastPodLogTimestamp,
		streamLogs:          streamLogs,
		stillRunning:        stillRunning,
		done:                make(chan struct{}),
	}
	collector.lastPodLogSeq.Store(resume.lastPodLogSeq)
	return collector, nil
}
