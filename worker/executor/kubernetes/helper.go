package kubernetes

import (
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
)

// GetNodeSelectorForJob returns node selector configuration for the given jobID
func (k *KubernetesExecutor) GetNodeSelectorForJob(jobID int, operation types.Command) map[string]string {
	// 1. Default mapping (JobID 0) applies to all operations
	var defaultSelector map[string]string
	if config, exists := k.configWatcher.GetJobMapping(0); exists {
		if config.NodeSelector != nil {
			defaultSelector = config.NodeSelector
		}
	}

	// 2. Job-specific mapping applies only to async operations (sync/clear-destination)
	if slices.Contains(constants.AsyncCommands, operation) {
		if config, exists := k.configWatcher.GetJobMapping(jobID); exists {
			// NodeSelector nil => not specified => inherit default
			if config.NodeSelector != nil {
				logger.Infof("found node selector for JobID %d: %v", jobID, config.NodeSelector)
				return config.NodeSelector
			}
			if defaultSelector != nil {
				logger.Debugf("inheriting default node selector for JobID %d", jobID)
				return defaultSelector
			}
			return map[string]string{}
		}
	}

	if defaultSelector != nil {
		logger.Debugf("using default node selector for JobID %d: %v", jobID, defaultSelector)
		return defaultSelector
	}

	logger.Debugf("no job-specific or default node selector for JobID %d", jobID)
	return map[string]string{}
}

// GetTolerationsForJob returns tolerations for the given jobID
func (k *KubernetesExecutor) GetTolerationsForJob(jobID int, operation types.Command) []corev1.Toleration {
	// 1. Default tolerations (JobID 0) apply to all operations
	var defaultTolerations []corev1.Toleration
	if config, exists := k.configWatcher.GetJobMapping(0); exists {
		// nil slice => not set; empty slice => explicitly set empty (clear)
		if config.Tolerations != nil {
			defaultTolerations = config.Tolerations
		}
	}

	// 2. Job-specific tolerations apply only to async operations (sync/clear-destination)
	if slices.Contains(constants.AsyncCommands, operation) {
		if config, exists := k.configWatcher.GetJobMapping(jobID); exists {
			// nil slice => not specified => inherit default
			if config.Tolerations != nil {
				return config.Tolerations
			}
			if defaultTolerations != nil {
				return defaultTolerations
			}
			return []corev1.Toleration{}
		}
	}

	if defaultTolerations != nil {
		return defaultTolerations
	}

	return []corev1.Toleration{}
}

// BuildAffinityForJob returns affinity rules for the given jobID
func (k *KubernetesExecutor) BuildAffinityForJob(jobID int, operation types.Command) *corev1.Affinity {
	// 1. Explicit Config (Preferred)
	// Preserve legacy behavior: only apply job-specific overrides for async commands (sync/clear-destination).
	// Default (JobID 0) applies to all operations, including short-lived jobs.
	if slices.Contains(constants.AsyncCommands, operation) {
		if config, exists := k.configWatcher.GetJobMapping(jobID); exists {
			if config.Affinity != nil {
				logger.Infof("using explicit affinity for JobID %d", jobID)
				return config.Affinity
			}
			// Affinity not specified => inherit default affinity (if any).
			// Note: auto-generated rules (legacy safety-net) are still suppressed below when a job config exists.
		}
	}

	// 2. Default Config (JobID 0) applies to all operations
	if config, exists := k.configWatcher.GetJobMapping(0); exists {
		if config.Affinity != nil {
			logger.Debugf("using default affinity (JobID 0) for JobID %d", jobID)
			return config.Affinity
		}
	}

	// 3. Legacy safety-net (migration support):
	// If there are other job mappings/profiles using NodeSelectors and this job is unmapped,
	// generate a NotIn nodeAffinity so "unmapped" async jobs don't accidentally land on reserved nodes.
	// This preserves behavior from the previous implementation when users only configured jobMapping without a default (0).
	if !slices.Contains(constants.AsyncCommands, operation) {
		return nil
	}

	// If job-specific config exists but doesn't specify affinity, assume the user is managing scheduling
	// and do not auto-generate rules.
	if _, exists := k.configWatcher.GetJobMapping(jobID); exists {
		return nil
	}

	all := k.configWatcher.GetAllJobMapping()
	if len(all) == 0 {
		return nil
	}

	// Collect unique label values per key across all configured NodeSelectors.
	uniq := map[string]map[string]struct{}{}
	for _, cfg := range all {
		for k, v := range cfg.NodeSelector {
			if k == "" || v == "" {
				continue
			}
			if _, ok := uniq[k]; !ok {
				uniq[k] = map[string]struct{}{}
			}
			uniq[k][v] = struct{}{}
		}
	}

	if len(uniq) == 0 {
		return nil
	}

	expressions := make([]corev1.NodeSelectorRequirement, 0, len(uniq))
	for labelKey, labelValues := range uniq {
		values := make([]string, 0, len(labelValues))
		for v := range labelValues {
			values = append(values, v)
		}
		slices.Sort(values)

		expressions = append(expressions, corev1.NodeSelectorRequirement{
			Key:      labelKey,
			Operator: corev1.NodeSelectorOpNotIn,
			Values:   values,
		})
	}

	logger.Debugf("using legacy auto anti-affinity for unmapped JobID %d (reserved label pairs: %d keys)", jobID, len(uniq))
	return &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{MatchExpressions: expressions},
				},
			},
		},
	}
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
