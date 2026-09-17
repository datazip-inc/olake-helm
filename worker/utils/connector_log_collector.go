package utils

import (
	"context"
	"fmt"
	"io"
	"path"
	"sync/atomic"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
)

const (
	logReconnectInitial = time.Second
	logReconnectMax     = 30 * time.Second
	logFinishTimeout    = 2 * time.Minute
)

// ConnectorStreamFunc opens a connector log stream from the given resume timestamp.
type ConnectorStreamFunc func(ctx context.Context, lastLogTimestamp time.Time, follow bool) (io.Reader, error)

// ConnectorStillRunningFunc reports whether the connector workload is still active and logs may resume.
type ConnectorStillRunningFunc func(ctx context.Context) bool

// ConnectorLogCollector tails connector logs, buffers locally, and uploads chunks to S3.
// One owner goroutine runs the stream, a final non-follow catch-up, and flush.
// Stop only signals and joins.
type ConnectorLogCollector struct {
	buffer              *PodLogBuffer
	lastPodLogTimestamp time.Time
	lastPodLogSeq       atomic.Uint64
	streamLogs          ConnectorStreamFunc
	stillRunning        ConnectorStillRunningFunc
	streamCtx           context.Context
	cancel              context.CancelFunc
	done                chan struct{}
}

// NewConnectorLogCollector tails connector logs, buffers locally, and uploads chunks to S3.
func NewConnectorLogCollector(ctx context.Context, workDir string, streamLogs ConnectorStreamFunc, stillRunning ConnectorStillRunningFunc) (*ConnectorLogCollector, error) {
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
	collector := &ConnectorLogCollector{
		buffer:              buffer,
		lastPodLogTimestamp: resume.lastPodLogTimestamp,
		streamLogs:          streamLogs,
		stillRunning:        stillRunning,
		done:                make(chan struct{}),
	}
	collector.lastPodLogSeq.Store(resume.lastPodLogSeq)
	return collector, nil
}

// processLogLine parses a raw stream line, skips old seq, and writes the buffer.
func (c *ConnectorLogCollector) processLogLine(ctx context.Context, rawLogLine string) error {
	normalizedLogLine, ok := parsePodLogLine(rawLogLine)
	if !ok {
		return nil
	}
	if normalizedLogLine.Seq > 0 && normalizedLogLine.Seq <= c.lastPodLogSeq.Load() {
		return nil
	}
	if err := c.buffer.WriteLine(ctx, normalizedLogLine); err != nil {
		return err
	}
	if normalizedLogLine.Seq > 0 {
		c.lastPodLogSeq.Store(normalizedLogLine.Seq)
	}
	c.lastPodLogTimestamp = normalizedLogLine.PodLogTimestamp
	return nil
}

func (c *ConnectorLogCollector) Start(ctx context.Context) {
	c.streamCtx, c.cancel = context.WithCancel(ctx)
	go func() {
		defer close(c.done)
		defer func() {
			if err := c.catchUpAndFlush(); err != nil {
				logger.Warnf("failed to flush remaining logs: %s", err)
			}
		}()
		c.follow()
	}()
}

func (c *ConnectorLogCollector) follow() {
	backoff := logReconnectInitial

	for {
		err := c.runStream(c.streamCtx, true)
		if c.streamCtx.Err() != nil {
			return
		}

		if c.stillRunning != nil && !c.stillRunning(c.streamCtx) {
			return
		}

		if err == nil {
			backoff = logReconnectInitial
		}

		select {
		case <-c.streamCtx.Done():
			return
		case <-time.After(backoff):
		}

		if err != nil {
			backoff = min(backoff*2, logReconnectMax)
		}
	}
}

func (c *ConnectorLogCollector) runStream(ctx context.Context, follow bool) error {
	reader, err := c.streamLogs(ctx, c.lastPodLogTimestamp, follow)
	if err != nil {
		return err
	}
	if closer, ok := reader.(io.Closer); ok {
		defer closer.Close()
	}

	return readPodLogStream(reader, func(rawLogLine string) error {
		return c.processLogLine(ctx, rawLogLine)
	})
}

func (c *ConnectorLogCollector) Stop() {
	if c.cancel == nil {
		return
	}
	c.cancel()
	<-c.done
}

// Drain uploads leftover logs without starting a follow loop.
// If follow is already running, Stop closes it and Start's catch-up is the drain.
// If follow never started, this is a one-shot catch-up+flush.
func (c *ConnectorLogCollector) Drain() error {
	if c.cancel != nil {
		c.Stop()
		return nil
	}
	return c.catchUpAndFlush()
}

func (c *ConnectorLogCollector) catchUpAndFlush() error {
	finishCtx, cancel := context.WithTimeout(context.Background(), logFinishTimeout)
	defer cancel()
	if err := c.runStream(finishCtx, false); err != nil {
		logger.Warnf("failed to catch up logs: %s", err)
	}
	if err := c.buffer.Flush(finishCtx); err != nil {
		return fmt.Errorf("failed to flush remaining logs: %s", err)
	}
	return nil
}
