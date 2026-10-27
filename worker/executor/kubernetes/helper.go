package kubernetes

import (
	"encoding/json"
	"fmt"
	"maps"
	"path"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
)

// getNodeSelectorForJob returns node selector configuration for the given jobID
// Returns empty map if no mapping is found (graceful fallback)
// Only applies node mapping for async operations (sync, clear destination)
func (k *KubernetesExecutor) GetNodeSelectorForJob(jobID int, operation types.Command) map[string]string {
	// Check profiles for async operations
	if utils.IsAsyncCommand(operation) {
		if profile, exists := k.configWatcher.GetJobProfile(jobID); exists {
			if profile.NodeSelector != nil {
				return profile.NodeSelector
			}
			return map[string]string{}
		}
	}

	// Check default profile
	if profile, exists := k.configWatcher.GetJobProfile(0); exists {
		if profile.NodeSelector != nil {
			return profile.NodeSelector
		}
		return map[string]string{}
	}

	// [TO BE DEPRECATED]
	// Try specific mapping (Preferred)
	if utils.IsAsyncCommand(operation) {
		if mapping, exists := k.configWatcher.GetJobMapping(jobID); exists {
			logger.Infof("found node mapping for JobID %d: %v", jobID, mapping)
			return mapping
		}
	}

	// [TO BE DEPRECATED]
	// Try default mapping (JobID 0)
	if mapping, exists := k.configWatcher.GetJobMapping(0); exists {
		logger.Debugf("using default node mapping: %v", mapping)
		return mapping
	}

	logger.Debugf("no specific or default mapping found for JobID %d, using standard scheduling", jobID)
	return make(map[string]string)
}

// GetTolerationsForJob returns tolerations for the given jobID
func (k *KubernetesExecutor) GetTolerationsForJob(jobID int, operation types.Command) []corev1.Toleration {
	// 1. Check specific profile
	if utils.IsAsyncCommand(operation) {
		if profile, exists := k.configWatcher.GetJobProfile(jobID); exists {
			if len(profile.Tolerations) > 0 {
				return profile.Tolerations
			}
			return []corev1.Toleration{}
		}
	}

	// 2. Check default profile
	if profile, exists := k.configWatcher.GetJobProfile(0); exists {
		if len(profile.Tolerations) > 0 {
			logger.Debugf("using default profile tolerations")
			return profile.Tolerations
		}
		logger.Debugf("default profile exists but tolerations empty")
		return []corev1.Toleration{}
	}

	return []corev1.Toleration{}
}

// defaultJobResources applies when no job profile sets resources: a small request
// so the pod schedules, and no limits for flexibility.
func defaultJobResources() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("256Mi"),
			corev1.ResourceCPU:    resource.MustParse("100m"),
		},
	}
}

// hasResources treats `resources: {}` as unset so it cannot strip the pod's requests
func hasResources(r *corev1.ResourceRequirements) bool {
	return r != nil && (len(r.Requests) > 0 || len(r.Limits) > 0)
}

// GetResourcesForJob returns container resources for the given jobID.
// A job's own profile applies only to async operations (sync, clear destination).
// A profile without resources falls back to the default profile, then to the built-in default.
func (k *KubernetesExecutor) GetResourcesForJob(jobID int, operation types.Command) corev1.ResourceRequirements {
	// 1. Check specific profile
	if utils.IsAsyncCommand(operation) {
		if profile, exists := k.configWatcher.GetJobProfile(jobID); exists && hasResources(profile.Resources) {
			return *profile.Resources.DeepCopy()
		}
	}

	// 2. Check default profile
	if profile, exists := k.configWatcher.GetJobProfile(0); exists && hasResources(profile.Resources) {
		return *profile.Resources.DeepCopy()
	}

	return defaultJobResources()
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

// buildPodAnnotations merges global job pod annotations with olake-internal ones.
// Global annotations are applied first so internal olake.io/* keys always win on conflict.
func (k *KubernetesExecutor) buildPodAnnotations(internal map[string]string) map[string]string {
	return mergeStringMaps(k.config.JobPodAnnotations, internal)
}

// buildPodLabels merges global job pod labels with internal ones; internal keys win.
func (k *KubernetesExecutor) buildPodLabels(internal map[string]string) map[string]string {
	return mergeStringMaps(k.config.JobPodLabels, internal)
}

// mergeStringMaps returns base with override applied on top, per key. Returns nil
// when both are empty so the object carries no empty map.
func mergeStringMaps(base, override map[string]string) map[string]string {
	if len(base) == 0 && len(override) == 0 {
		return nil
	}
	merged := make(map[string]string, len(base)+len(override))
	maps.Copy(merged, base)
	maps.Copy(merged, override)
	return merged
}

// BuildAffinityForJob returns NodeAffinity rules to prevent unmapped jobs from scheduling on nodes reserved for mapped jobs.
// Uses NotIn operator to exclude nodes with label key-value pairs used by any mapped job.
// Reference: https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#node-affinity
func (k *KubernetesExecutor) BuildAffinityForJob(jobID int, operation types.Command) *corev1.Affinity {
	// Check if profile has explicit affinity
	if profile, exists := k.configWatcher.GetJobProfile(jobID); exists {
		if profile.Affinity != nil {
			return profile.Affinity
		}
		return nil
	}

	// Check default profile
	if profile, exists := k.configWatcher.GetJobProfile(0); exists {
		if profile.Affinity != nil {
			return profile.Affinity
		}
		return nil
	}

	// Check if job has explicit mapping
	if _, exists := k.configWatcher.GetJobMapping(jobID); exists {
		return nil
	}

	// [TO BE DEPRECATED]
	// If default mapping exists (JobID 0), trust it for placement.
	// Do not auto-generate anti-affinity rules which might conflict with the default selector.
	// Example: If Default=gpu and Job1=gpu, Anti-Affinity (NotIn gpu) would make unmapped jobs unschedulable on Default nodes.
	if _, exists := k.configWatcher.GetJobMapping(0); exists {
		return nil
	}

	// For non-async operations, don't auto-generate anti-affinity
	// They should only use explicit configs (profiles or mappings)
	if !utils.IsAsyncCommand(operation) {
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

// reservedVolumeNames are the volumes the executor adds itself; a user-supplied
// volume with the same name would make the API server reject the pod.
var reservedVolumeNames = map[string]struct{}{
	"job-storage":   {},
	"index-storage": {},
}

// parseJobVolumes decodes and validates the user-supplied connector pod volumes and
// mounts (global.extraVolumes / global.extraVolumeMounts). Empty inputs yield nothing.
func parseJobVolumes(volumesJSON, mountsJSON string) ([]corev1.Volume, []corev1.VolumeMount, error) {
	var (
		volumes []corev1.Volume
		mounts  []corev1.VolumeMount
	)
	if volumesJSON != "" {
		if err := json.Unmarshal([]byte(volumesJSON), &volumes); err != nil {
			return nil, nil, fmt.Errorf("failed to unmarshal job volumes: %w", err)
		}
	}
	if mountsJSON != "" {
		if err := json.Unmarshal([]byte(mountsJSON), &mounts); err != nil {
			return nil, nil, fmt.Errorf("failed to unmarshal job volume mounts: %w", err)
		}
	}

	names := make(map[string]struct{}, len(volumes))
	for _, v := range volumes {
		if v.Name == "" {
			return nil, nil, fmt.Errorf("extraVolumes entry is missing a name")
		}
		if _, reserved := reservedVolumeNames[v.Name]; reserved {
			return nil, nil, fmt.Errorf("extraVolumes name %q is reserved for the executor's own volume", v.Name)
		}
		if _, dup := names[v.Name]; dup {
			return nil, nil, fmt.Errorf("extraVolumes name %q is used more than once", v.Name)
		}
		names[v.Name] = struct{}{}
	}

	configDir := path.Clean(constants.ContainerMountDir)
	paths := make(map[string]struct{}, len(mounts))
	for _, m := range mounts {
		if _, ok := names[m.Name]; !ok {
			return nil, nil, fmt.Errorf("extraVolumeMounts name %q does not match any extraVolumes entry", m.Name)
		}
		// Container paths are POSIX regardless of where the worker runs.
		clean := path.Clean(m.MountPath)
		if m.MountPath == "" || clean == "/" {
			return nil, nil, fmt.Errorf("extraVolumeMounts %q needs a mountPath other than the container root", m.Name)
		}
		if clean == configDir || strings.HasPrefix(clean, configDir+"/") {
			return nil, nil, fmt.Errorf("extraVolumeMounts %q mountPath %q would shadow the shared config directory %s",
				m.Name, m.MountPath, configDir)
		}
		if _, dup := paths[clean]; dup {
			return nil, nil, fmt.Errorf("extraVolumeMounts mountPath %q is used more than once", m.MountPath)
		}
		paths[clean] = struct{}{}
	}

	return volumes, mounts, nil
}
