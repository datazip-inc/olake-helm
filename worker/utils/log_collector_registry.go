package utils

import (
	"context"
	"sync"

	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
)

type workflowLogCollectors struct {
	worker    *RuntimeLogCollector
	connector *RuntimeLogCollector
	attempts  int
}

type workflowLogCollectorRegistry struct {
	mu      sync.Mutex
	entries map[string]*workflowLogCollectors
}

var globalWorkflowLogCollectorRegistry = &workflowLogCollectorRegistry{
	entries: make(map[string]*workflowLogCollectors),
}

// acquireWorkflowLogCollectors returns a release function that must run when the activity attempt ends.
// Collectors are shared across Temporal activity retries for the same workDir (attempt-counted).
func acquireWorkflowLogCollectors(
	ctx context.Context,
	workflowID string,
	workDir string,
	command types.Command,
	newWorkerLogCollector func(ctx context.Context, workflowID, workDir string) (*RuntimeLogCollector, error),
	newConnectorLogCollector func(ctx context.Context, workflowID, workDir string, command types.Command) (*RuntimeLogCollector, error),
) (func(context.Context) error, error) {
	globalWorkflowLogCollectorRegistry.mu.Lock()
	defer globalWorkflowLogCollectorRegistry.mu.Unlock()

	if logCollectors := globalWorkflowLogCollectorRegistry.entries[workDir]; logCollectors != nil {
		logCollectors.attempts++
		return releaseWorkflowLogCollectors(workDir), nil
	}

	workerCollector, err := newWorkerLogCollector(ctx, workflowID, workDir)
	if err != nil {
		return nil, err
	}
	workerCollector.Start(ctx)

	var connectorCollector *RuntimeLogCollector
	if newConnectorLogCollector != nil {
		connectorCollector, err = newConnectorLogCollector(ctx, workflowID, workDir, command)
		if err != nil {
			logger.Warnf("failed to start connector log collector for workflowID=%s: %s", workflowID, err)
		} else {
			connectorCollector.Start(ctx)
		}
	}

	globalWorkflowLogCollectorRegistry.entries[workDir] = &workflowLogCollectors{
		worker:    workerCollector,
		connector: connectorCollector,
		attempts:  1,
	}
	return releaseWorkflowLogCollectors(workDir), nil
}

func releaseWorkflowLogCollectors(workDir string) func(context.Context) error {
	return func(ctx context.Context) error {
		globalWorkflowLogCollectorRegistry.mu.Lock()
		defer globalWorkflowLogCollectorRegistry.mu.Unlock()

		logCollectors := globalWorkflowLogCollectorRegistry.entries[workDir]

		logCollectors.attempts--
		if logCollectors.attempts > 0 {
			return nil
		}

		delete(globalWorkflowLogCollectorRegistry.entries, workDir)
		if logCollectors.worker != nil {
			logCollectors.worker.Stop(ctx)
		}
		if logCollectors.connector != nil {
			logCollectors.connector.Stop(ctx)
		}
		return nil
	}
}
