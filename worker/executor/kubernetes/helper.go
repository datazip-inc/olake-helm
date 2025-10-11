package kubernetes

import (
	"strings"

	"github.com/datazip-inc/olake-helm/worker/logger"
	"github.com/datazip-inc/olake-helm/worker/types"
	"k8s.io/apimachinery/pkg/api/resource"
)

// getNodeSelectorForJob returns node selector configuration for the given jobID
// Returns empty map if no mapping is found (graceful fallback)
// Only applies node mapping for sync operations
func (k *KubernetesExecutor) GetNodeSelectorForJob(jobID int, operation types.Command) map[string]string {
	// Only apply node mapping for sync operations
	if operation != types.Sync {
		return make(map[string]string)
	}

	// Use live mapping from ConfigMapWatcher
	if mapping, exists := k.configWatcher.GetJobMapping(jobID); exists {
		logger.Infof("found node mapping for JobID %d: %v", jobID, mapping)
		return mapping
	}
	logger.Debugf("no node mapping found for JobID %d, using default scheduling", jobID)
	return make(map[string]string)
}

func (k *KubernetesExecutor) sanitizeName(name string) string {
	name = strings.ToLower(name)

	// Replace invalid characters with hyphens
	name = strings.ReplaceAll(name, "_", "-")
	name = strings.ReplaceAll(name, ".", "-")
	name = strings.ReplaceAll(name, ":", "-")

	name = strings.Trim(name, "-")

	// Truncate if too long (max 63 characters for Kubernetes)
	if len(name) > 63 {
		name = name[:63]
		name = strings.TrimSuffix(name, "-")
	}

	return name
}

func (k *KubernetesExecutor) parseQuantity(s string) resource.Quantity {
	q, _ := resource.ParseQuantity(s)
	return q
}
