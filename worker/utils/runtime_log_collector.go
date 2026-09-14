package utils

import (
	"context"
	"io"
	"sync/atomic"
	"time"

	"github.com/datazip-inc/olake-helm/worker/utils/logger"
)

const (
	logReconnectInitial = time.Second
	logReconnectMax     = 30 * time.Second
	logFinishTimeout    = 30 * time.Second
)

// StreamFunc opens a log stream from the given resume timestamp.
type StreamFunc func(ctx context.Context, lastLogTimestamp time.Time, follow bool) (io.Reader, error)

// StillRunningFunc reports whether the workload is still active and logs may resume.
type StillRunningFunc func(ctx context.Context) bool

// RuntimeLogCollector tails runtime logs, buffers locally, and uploads chunks to S3.
// One owner goroutine runs follow, catch-up, and flush. Stop only signals and joins.
type RuntimeLogCollector struct {
	buffer              *PodLogBuffer
	lastPodLogTimestamp time.Time
	lastPodLogSeq       atomic.Uint64
	streamLogs          StreamFunc
	stillRunning        StillRunningFunc
	streamCtx           context.Context
	cancel              context.CancelFunc
	done                chan struct{}
}

// processLogLine parses a raw stream line, skips old seq, and writes the buffer.
func (c *RuntimeLogCollector) processLogLine(ctx context.Context, rawLogLine string) error {
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

func (c *RuntimeLogCollector) Start(ctx context.Context) {
	c.streamCtx, c.cancel = context.WithCancel(ctx)
	go func() {
		defer close(c.done)
		defer func() {
			finishCtx, cancel := context.WithTimeout(context.Background(), logFinishTimeout)
			defer cancel()
			c.catchUp(finishCtx)
			if err := c.buffer.Flush(finishCtx); err != nil {
				logger.Warnf("failed to flush remaining logs: %s", err)
			}
		}()
		c.follow()
	}()
}

func (c *RuntimeLogCollector) follow() {
	backoff := logReconnectInitial

	for {
		err := c.runStream()
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

func (c *RuntimeLogCollector) runStream() error {
	reader, err := c.streamLogs(c.streamCtx, c.lastPodLogTimestamp, true)
	if err != nil {
		return err
	}
	if closer, ok := reader.(io.Closer); ok {
		defer closer.Close()
	}

	return readPodLogStream(reader, func(rawLogLine string) error {
		return c.processLogLine(c.streamCtx, rawLogLine)
	})
}

func (c *RuntimeLogCollector) Stop() {
	if c.cancel == nil {
		return
	}
	c.cancel()
	<-c.done
}

func (c *RuntimeLogCollector) catchUp(ctx context.Context) {
	reader, err := c.streamLogs(ctx, c.lastPodLogTimestamp, false)
	if err != nil {
		logger.Warnf("failed to catch up logs: %s", err)
		return
	}
	if closer, ok := reader.(io.Closer); ok {
		defer closer.Close()
	}

	if err := readPodLogStream(reader, func(rawLogLine string) error {
		return c.processLogLine(ctx, rawLogLine)
	}); err != nil {
		logger.Warnf("failed to read catch-up logs: %s", err)
	}
}
