package utils

import (
	"context"
	"sync"

	"github.com/datazip-inc/olake-helm/worker/utils/logger"
)

type workerLogWriterRegistry struct {
	mu      sync.Mutex
	entries map[string]*workerLogWriter
}

var globalWorkerLogWriterRegistry = &workerLogWriterRegistry{
	entries: make(map[string]*workerLogWriter),
}

type connectorLogCollectorRegistry struct {
	mu      sync.Mutex
	entries map[string]*ConnectorLogCollector
}

var globalConnectorLogCollectorRegistry = &connectorLogCollectorRegistry{
	entries: make(map[string]*ConnectorLogCollector),
}

// acquireWorkerLogWriter returns the worker log writer for workDir, creating it only if missing.
// A Temporal retry that finds an existing writer reuses it; logging follows the
// container/pod.
func acquireWorkerLogWriter(ctx context.Context, workDir string) (*workerLogWriter, error) {
	globalWorkerLogWriterRegistry.mu.Lock()
	defer globalWorkerLogWriterRegistry.mu.Unlock()

	if writer := globalWorkerLogWriterRegistry.entries[workDir]; writer != nil {
		return writer, nil
	}

	workerWriter, err := newWorkerLogWriter(ctx, workDir)
	if err != nil {
		return nil, err
	}

	globalWorkerLogWriterRegistry.entries[workDir] = workerWriter
	return workerWriter, nil
}

// ReleaseWorkerLogWriter flushes and drops the worker log writer for workDir.
// Call when the container/pod is gone.
func ReleaseWorkerLogWriter(workDir string) {
	globalWorkerLogWriterRegistry.mu.Lock()
	workerWriter := globalWorkerLogWriterRegistry.entries[workDir]

	delete(globalWorkerLogWriterRegistry.entries, workDir)
	globalWorkerLogWriterRegistry.mu.Unlock()

	if workerWriter != nil {
		if err := workerWriter.Close(); err != nil {
			logger.Warnf("failed to close worker log writer: %s", err)
		}
	}
}

// AcquireConnectorLogCollector binds a connector log collector for workDir.
// If follow is true (Execute): start follow when none is running; a Temporal retry
// that finds an existing collector reuses it.
// If follow is false (flush leftovers): close an existing follow collector, otherwise
// one-shot drain without Start.
func AcquireConnectorLogCollector(ctx context.Context, workDir string, newCollector func() (*ConnectorLogCollector, error), follow bool) error {
	globalConnectorLogCollectorRegistry.mu.Lock()
	defer globalConnectorLogCollectorRegistry.mu.Unlock()

	if existing := globalConnectorLogCollectorRegistry.entries[workDir]; existing != nil {
		if !follow {
			delete(globalConnectorLogCollectorRegistry.entries, workDir)
			return existing.Drain()
		}
		return nil
	}

	collector, err := newCollector()
	if err != nil {
		return err
	}

	if !follow {
		return collector.Drain()
	}

	collector.Start(context.WithoutCancel(ctx))
	globalConnectorLogCollectorRegistry.entries[workDir] = collector
	return nil
}

// ReleaseConnectorLogCollector stops and drops the collector for workDir.
// Call when the container/pod is gone.
func ReleaseConnectorLogCollector(workDir string) {
	globalConnectorLogCollectorRegistry.mu.Lock()
	connectorCollector := globalConnectorLogCollectorRegistry.entries[workDir]

	delete(globalConnectorLogCollectorRegistry.entries, workDir)
	globalConnectorLogCollectorRegistry.mu.Unlock()

	if connectorCollector != nil {
		connectorCollector.Stop()
	}
}
