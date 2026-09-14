package utils

import (
	"context"
	"sync"
)

type workflowLogCollectors struct {
	worker   *workerLogWriter
	attempts int
}

type workflowLogCollectorRegistry struct {
	mu      sync.Mutex
	entries map[string]*workflowLogCollectors
}

var globalWorkflowLogCollectorRegistry = &workflowLogCollectorRegistry{
	entries: make(map[string]*workflowLogCollectors),
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
	globalWorkflowLogCollectorRegistry.mu.Lock()
	defer globalWorkflowLogCollectorRegistry.mu.Unlock()

	if logCollectors := globalWorkflowLogCollectorRegistry.entries[workDir]; logCollectors != nil {
		logCollectors.attempts++
		return releaseWorkerLogWriter(workDir), logCollectors.worker, nil
	}

	workerWriter, err = newWorkerLogWriter(ctx, workDir)
	if err != nil {
		return nil, nil, err
	}

	globalWorkflowLogCollectorRegistry.entries[workDir] = &workflowLogCollectors{
		worker:   workerWriter,
		attempts: 1,
	}
	return releaseWorkerLogWriter(workDir), workerWriter, nil
}

func releaseWorkerLogWriter(workDir string) func() error {
	return func() error {
		globalWorkflowLogCollectorRegistry.mu.Lock()
		defer globalWorkflowLogCollectorRegistry.mu.Unlock()

		logCollectors := globalWorkflowLogCollectorRegistry.entries[workDir]
		if logCollectors == nil {
			return nil
		}
		logCollectors.attempts--
		if logCollectors.attempts > 0 {
			return nil
		}

		var err error
		if logCollectors.worker != nil {
			err = logCollectors.worker.Close()
		}
		delete(globalWorkflowLogCollectorRegistry.entries, workDir)
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
