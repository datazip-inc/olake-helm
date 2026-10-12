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
// The registry lock is held until Close's final flush finishes, so a concurrent acquire for
// the same workDir cannot create a new writer whose buffer setup clears this writer's local
// buffer mid-flush.
func ReleaseWorkerLogWriter(workDir string) {
	//Comment: This function is used to release the worker log writer for a given workDir.
	//Check if we can avoid using lock here on close function.
	globalWorkerLogWriterRegistry.mu.Lock()
	defer globalWorkerLogWriterRegistry.mu.Unlock()

	workerWriter := globalWorkerLogWriterRegistry.entries[workDir]
	delete(globalWorkerLogWriterRegistry.entries, workDir)

	if workerWriter != nil {
		if err := workerWriter.Close(); err != nil {
			logger.Warnf("failed to close worker log writer: %s", err)
		}
	}
}

// AcquireConnectorLogCollector starts a follow collector for workDir when none is running.
// A Temporal retry that finds an existing collector reuses it.
func AcquireConnectorLogCollector(ctx context.Context, workDir string, newCollector func() (*ConnectorLogCollector, error)) error {
	globalConnectorLogCollectorRegistry.mu.Lock()
	defer globalConnectorLogCollectorRegistry.mu.Unlock()

	if globalConnectorLogCollectorRegistry.entries[workDir] != nil {
		return nil
	}

	collector, err := newCollector()
	if err != nil {
		return err
	}

	collector.Start(context.WithoutCancel(ctx))
	globalConnectorLogCollectorRegistry.entries[workDir] = collector
	return nil
}

// ReleaseConnectorLogCollector stops and drops the collector for workDir.
// The registry lock is held until Stop's final catch-up and flush finish, so a concurrent
// Acquire cannot start a second collector that clears this workDir's local buffer mid-flush.
func ReleaseConnectorLogCollector(workDir string) {
	//Comment: This function is used to release the connector log collector for a given workDir.
	//Check if we can avoid using lock here on stop function.
	globalConnectorLogCollectorRegistry.mu.Lock()
	defer globalConnectorLogCollectorRegistry.mu.Unlock()

	connectorCollector := globalConnectorLogCollectorRegistry.entries[workDir]

	delete(globalConnectorLogCollectorRegistry.entries, workDir)

	if connectorCollector != nil {
		connectorCollector.Stop()
	}
}
