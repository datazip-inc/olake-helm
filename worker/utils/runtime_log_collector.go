package utils

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/datazip-inc/olake-helm/worker/utils/logger"
)

const (
	logReconnectInitial = time.Second
	logReconnectMax     = 30 * time.Second
)

// StreamFunc opens a log stream from the given resume timestamp.
type StreamFunc func(ctx context.Context, lastLogTimestamp time.Time, follow bool) (io.Reader, error)

// StillRunningFunc reports whether the workload is still active and logs may resume.
type StillRunningFunc func(ctx context.Context) bool

// RuntimeLogCollector tails runtime logs, buffers locally, and uploads chunks to S3.
type RuntimeLogCollector struct {
	buffer *PodLogBuffer

	lastLogTimestamp   time.Time
	lastLogTimestampMu sync.Mutex

	streamCancel   context.CancelFunc
	streamCancelMu sync.Mutex

	streamLogs   StreamFunc
	stillRunning StillRunningFunc

	processLine func(ctx context.Context, rawLogLine string) error

	done chan struct{}
	wg   sync.WaitGroup
}

func (c *RuntimeLogCollector) Start(ctx context.Context) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.follow(ctx)
	}()
}

func (c *RuntimeLogCollector) follow(ctx context.Context) {
	backoff := logReconnectInitial

	for {
		err := c.runStream(ctx)
		if c.shouldStop(ctx) {
			return
		}

		if c.stillRunning != nil && !c.stillRunning(ctx) {
			return
		}

		if err == nil {
			backoff = logReconnectInitial
		}

		select {
		case <-c.done:
			return
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		if err != nil && backoff < logReconnectMax {
			backoff *= 2
			if backoff > logReconnectMax {
				backoff = logReconnectMax
			}
		}
	}
}

func (c *RuntimeLogCollector) runStream(ctx context.Context) error {
	c.lastLogTimestampMu.Lock()
	lastLogTimestamp := c.lastLogTimestamp
	c.lastLogTimestampMu.Unlock()

	streamCtx, cancel := context.WithCancel(ctx)
	c.streamCancelMu.Lock()
	c.streamCancel = cancel
	c.streamCancelMu.Unlock()
	defer func() {
		cancel()
		c.streamCancelMu.Lock()
		c.streamCancel = nil
		c.streamCancelMu.Unlock()
	}()

	reader, err := c.streamLogs(streamCtx, lastLogTimestamp, true)
	if err != nil {
		return err
	}
	if closer, ok := reader.(io.Closer); ok {
		defer closer.Close()
	}

	return readPodLogStream(reader, func(rawLogLine string) error {
		return c.processLine(ctx, rawLogLine)
	})
}

func (c *RuntimeLogCollector) shouldStop(ctx context.Context) bool {
	select {
	case <-c.done:
		return true
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func (c *RuntimeLogCollector) Stop(ctx context.Context) {
	c.cancelStream()
	close(c.done)
	c.wg.Wait()
	c.catchUp(ctx)

	if err := c.buffer.Flush(ctx); err != nil {
		logger.Warnf("failed to flush remaining logs: %s", err)
	}
}

func (c *RuntimeLogCollector) catchUp(ctx context.Context) {
	c.lastLogTimestampMu.Lock()
	lastLogTimestamp := c.lastLogTimestamp
	c.lastLogTimestampMu.Unlock()
	if lastLogTimestamp.IsZero() {
		return
	}

	catchUpCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	reader, err := c.streamLogs(catchUpCtx, lastLogTimestamp, false)
	if err != nil {
		logger.Warnf("failed to catch up logs: %s", err)
		return
	}
	if closer, ok := reader.(io.Closer); ok {
		defer closer.Close()
	}

	if err := readPodLogStream(reader, func(rawLogLine string) error {
		return c.processLine(ctx, rawLogLine)
	}); err != nil {
		logger.Warnf("failed to read catch-up logs: %s", err)
	}
}

func (c *RuntimeLogCollector) cancelStream() {
	c.streamCancelMu.Lock()
	if c.streamCancel != nil {
		c.streamCancel()
	}
	c.streamCancelMu.Unlock()
}
