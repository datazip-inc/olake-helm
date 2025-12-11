package kubernetes

import (
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
)

// getNodeSelectorForJob returns node selector configuration for the given jobID
// Returns empty map if no mapping is found (graceful fallback)
// Only applies node mapping for async operations (sync, clear destination)
func (k *KubernetesExecutor) GetNodeSelectorForJob(jobID int, operation types.Command) map[string]string {
	// 1. Try specific mapping (Preferred)
	// Only apply specific mapping for async operations (sync)
	if slices.Contains(constants.AsyncCommands, operation) {
		if mapping, exists := k.configWatcher.GetJobMapping(jobID); exists {
			logger.Infof("found node mapping for JobID %d: %v", jobID, mapping)
			return mapping
		}
	}

	// 2. Try default mapping (JobID 0)
	// Apply default mapping for ALL operations (sync, check, discover)
	if mapping, exists := k.configWatcher.GetJobMapping(0); exists {
		logger.Debugf("using default node mapping")
		return mapping
	}

	logger.Debugf("no specific or default mapping found for JobID %d, using standard scheduling", jobID)
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

// BuildAffinityForJob returns NodeAffinity rules to prevent unmapped jobs from scheduling on nodes reserved for mapped jobs.
// Uses NotIn operator to exclude nodes with label key-value pairs used by any mapped job.
// Reference: https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#node-affinity
func (k *KubernetesExecutor) BuildAffinityForJob(jobID int, operation types.Command) *corev1.Affinity {
	if !slices.Contains(constants.AsyncCommands, operation) {
		return nil
	}

	if _, exists := k.configWatcher.GetJobMapping(jobID); exists {
		return nil
	}

	// If default mapping exists (JobID 0), trust it for placement.
	// Do not auto-generate anti-affinity rules which might conflict with the default selector.
	// Example: If Default=gpu and Job1=gpu, Anti-Affinity (NotIn gpu) would make unmapped jobs unschedulable on Default nodes.
	if _, exists := k.configWatcher.GetJobMapping(0); exists {
		return nil
	}

	// Get all job mappings and transform to unique label key-value pairs
	allJobMappings := k.configWatcher.GetAllJobMapping()
	if len(allJobMappings) == 0 {
		return nil
	}

	// Transform map[int]map[string]string to map[string][]string
	// Collect all unique values for each label key across all jobs
	uniq := map[string]sets.Set[string]{}
	for _, labels := range allJobMappings {
		for k, v := range labels {
			if _, ok := uniq[k]; !ok {
				uniq[k] = sets.New[string]()
			}
			uniq[k].Insert(v)
		}
	}

	// Build NodeSelectorRequirements from unique label mappings
	expressions := make([]corev1.NodeSelectorRequirement, 0, len(uniq))
	for labelKey, labelValuesSet := range uniq {
		expressions = append(expressions, corev1.NodeSelectorRequirement{
			Key:      labelKey,
			Operator: corev1.NodeSelectorOpNotIn,
			Values:   labelValuesSet.UnsortedList(),
		})
	}

	return &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{
						MatchExpressions: expressions,
					},
				},
			},
		},
	}
}
