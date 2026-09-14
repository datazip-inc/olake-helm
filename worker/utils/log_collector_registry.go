package utils

import (
	"context"
	"sync"
)

type workerLogWriters struct {
	workerLogWriter *workerLogWriter
	attempts        int
}

type workerLogWriterRegistry struct {
	mu      sync.Mutex
	entries map[string]*workerLogWriters
}

var globalWorkerLogWriterRegistry = &workerLogWriterRegistry{
	entries: make(map[string]*workerLogWriters),
}

type connectorLogCollectors struct {
	collector *RuntimeLogCollector
	attempts  int
}

type connectorLogCollectorRegistry struct {
	mu      sync.Mutex
	entries map[string]*connectorLogCollectors
}

var globalConnectorLogCollectorRegistry = &connectorLogCollectorRegistry{
	entries: make(map[string]*connectorLogCollectors),
}

// acquireWorkerLogWriter returns a release function that must run when the activity attempt ends.
// The worker writer is shared across Temporal activity retries for the same workDir.
func acquireWorkerLogWriter(
	ctx context.Context,
	workDir string,
) (release func() error, workerWriter *workerLogWriter, err error) {
	globalWorkerLogWriterRegistry.mu.Lock()
	defer globalWorkerLogWriterRegistry.mu.Unlock()

	if logWriters := globalWorkerLogWriterRegistry.entries[workDir]; logWriters != nil {
		logWriters.attempts++
		return releaseWorkerLogWriter(workDir), logWriters.workerLogWriter, nil
	}

	workerWriter, err = newWorkerLogWriter(ctx, workDir)
	if err != nil {
		return nil, nil, err
	}

	globalWorkerLogWriterRegistry.entries[workDir] = &workerLogWriters{
		workerLogWriter: workerWriter,
		attempts:        1,
	}
	return releaseWorkerLogWriter(workDir), workerWriter, nil
}

func releaseWorkerLogWriter(workDir string) func() error {
	return func() error {
		globalWorkerLogWriterRegistry.mu.Lock()
		defer globalWorkerLogWriterRegistry.mu.Unlock()

		logWriters := globalWorkerLogWriterRegistry.entries[workDir]
		if logWriters == nil {
			return nil
		}
		logWriters.attempts--
		if logWriters.attempts > 0 {
			return nil
		}

		var err error
		if logWriters.workerLogWriter != nil {
			err = logWriters.workerLogWriter.Close()
		}
		delete(globalWorkerLogWriterRegistry.entries, workDir)
		return err
	}
}

// AcquireConnectorLogCollector returns a shared connector collector for workDir.
// Overlapping Execute calls (Temporal retries) reuse one buffer and chunk counter.
// Release from Execute so ownership stays out of the interceptor.
func AcquireConnectorLogCollector(ctx context.Context, workDir string, newCollector func() (*RuntimeLogCollector, error)) (release func(), err error) {
	globalConnectorLogCollectorRegistry.mu.Lock()
	defer globalConnectorLogCollectorRegistry.mu.Unlock()

	if logCollectors := globalConnectorLogCollectorRegistry.entries[workDir]; logCollectors != nil {
		logCollectors.attempts++
		return releaseConnectorLogCollector(workDir), nil
	}

	collector, err := newCollector()
	if err != nil {
		return nil, err
	}
	collector.Start(context.WithoutCancel(ctx))

	globalConnectorLogCollectorRegistry.entries[workDir] = &connectorLogCollectors{
		collector: collector,
		attempts:  1,
	}
	return releaseConnectorLogCollector(workDir), nil
}

func releaseConnectorLogCollector(workDir string) func() {
	return func() {
		globalConnectorLogCollectorRegistry.mu.Lock()
		defer globalConnectorLogCollectorRegistry.mu.Unlock()

		logCollectors := globalConnectorLogCollectorRegistry.entries[workDir]
		if logCollectors == nil {
			return
		}
		logCollectors.attempts--
		if logCollectors.attempts > 0 {
			return
		}

		if logCollectors.collector != nil {
			logCollectors.collector.Stop()
		}
		delete(globalConnectorLogCollectorRegistry.entries, workDir)
	}
}
