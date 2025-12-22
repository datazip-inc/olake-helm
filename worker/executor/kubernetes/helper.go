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
	// 1. Try specific mapping (Preferred)
	if slices.Contains(constants.AsyncCommands, operation) {
		if config, exists := k.configWatcher.GetJobMapping(jobID); exists {
			if config.NodeSelector != nil {
				logger.Infof("found node selector for JobID %d: %v", jobID, config.NodeSelector)
				return config.NodeSelector
			}
		}
	}

	// 2. Try default mapping (JobID 0)
	if config, exists := k.configWatcher.GetJobMapping(0); exists {
		if config.NodeSelector != nil {
			logger.Debugf("using default node selector: %v", config.NodeSelector)
			return config.NodeSelector
		}
	}

	logger.Debugf("no specific or default node selector for JobID %d", jobID)
	return make(map[string]string)
}

// GetTolerationsForJob returns tolerations for the given jobID
func (k *KubernetesExecutor) GetTolerationsForJob(jobID int, operation types.Command) []corev1.Toleration {
	// 1. Try specific mapping (Preferred)
	if slices.Contains(constants.AsyncCommands, operation) {
		if config, exists := k.configWatcher.GetJobMapping(jobID); exists {
			if len(config.Tolerations) > 0 {
				return config.Tolerations
			}
		}
	}

	// 2. Try default mapping (JobID 0)
	if config, exists := k.configWatcher.GetJobMapping(0); exists {
		if len(config.Tolerations) > 0 {
			return config.Tolerations
		}
	}

	return []corev1.Toleration{}
}

// BuildAffinityForJob returns affinity rules for the given jobID
func (k *KubernetesExecutor) BuildAffinityForJob(jobID int, operation types.Command) *corev1.Affinity {
	if !slices.Contains(constants.AsyncCommands, operation) {
		return nil
	}

	// 1. Explicit Config (Preferred)
	if config, exists := k.configWatcher.GetJobMapping(jobID); exists {
		if config.Affinity != nil {
			logger.Infof("using explicit affinity for JobID %d", jobID)
			return config.Affinity
		}
		// If explicit profile exists (even without affinity), we DO NOT generate auto-rules.
		// The user manages this job explicitly.
		return nil
	}

	// 2. Default Config (JobID 0)
	if config, exists := k.configWatcher.GetJobMapping(0); exists {
		if config.Affinity != nil {
			logger.Debugf("using default affinity for JobID %d", jobID)
			return config.Affinity
		}
		return nil
	}

	return nil
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
