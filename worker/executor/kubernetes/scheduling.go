package kubernetes

import (
	"encoding/json"
	"fmt"
	"maps"
	"strings"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Package-level variable to store last known good mapping for fallback
var lastValidMapping map[int]map[string]string

// Package-level variable to store the last successfully parsed job profiles.
// Without it, a malformed OLAKE_JOB_PROFILES would drop every profile - including
// the default profile 0 - and jobs would silently run with no scheduling
// constraints and no index volume configuration.
var lastValidProfiles map[int]JobSchedulingConfig

// JobMappingStats contains statistics about job mapping loading
type JobMappingStats struct {
	TotalEntries    int
	ValidEntries    int
	InvalidMappings []string // TODO:  remove invalidMappings from struct{}
}

// JobSchedulingConfig defines the scheduling constraints for a job
type JobSchedulingConfig struct {
	NodeSelector map[string]string   `json:"nodeSelector,omitempty"`
	Tolerations  []corev1.Toleration `json:"tolerations,omitempty"`
	Affinity     *corev1.Affinity    `json:"affinity,omitempty"`
	// IndexStorage overrides the chart-wide index volume settings for this job.
	// Co-located with scheduling because the volume's zone and the profile's
	// scheduling constraints mutually constrain each other.
	IndexStorage *IndexStorageConfig `json:"indexStorage,omitempty"`
}

func validateLabelPair(jobID int, key, value string, stats *JobMappingStats) error {
	if key == "" {
		err := fmt.Errorf("empty label key")
		stats.InvalidMappings = append(stats.InvalidMappings, fmt.Sprintf("JobID %d: %s", jobID, err))
		return err
	}
	if value == "" {
		err := fmt.Errorf("empty label value for key '%s'", key)
		stats.InvalidMappings = append(stats.InvalidMappings, fmt.Sprintf("JobID %d: %s", jobID, err))
		return err
	}
	if errs := validation.IsQualifiedName(key); len(errs) > 0 {
		err := fmt.Errorf("invalid label key '%s': %s", key, errs)
		stats.InvalidMappings = append(stats.InvalidMappings, fmt.Sprintf("JobID %d: %s", jobID, err))
		return err
	}
	if errs := validation.IsValidLabelValue(value); len(errs) > 0 {
		err := fmt.Errorf("invalid label value '%s' for key '%s': %s", value, key, errs)
		stats.InvalidMappings = append(stats.InvalidMappings, fmt.Sprintf("JobID %d: %s", jobID, err))
		return err
	}
	return nil
}

// validateJobMapping validates a single job mapping entry
func validateJobMapping(jobID int, nodeLabels map[string]string, stats *JobMappingStats) (map[string]string, bool) {
	if jobID < 0 {
		stats.InvalidMappings = append(stats.InvalidMappings, fmt.Sprintf("Invalid JobID: %d", jobID))
		return nil, false
	}
	if nodeLabels == nil {
		stats.InvalidMappings = append(stats.InvalidMappings, fmt.Sprintf("JobID %d: null mapping", jobID))
		return nil, false
	}
	if len(nodeLabels) == 0 {
		return make(map[string]string), true
	}

	validMapping := make(map[string]string)
	for k, v := range nodeLabels {
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		if err := validateLabelPair(jobID, k, v, stats); err != nil {
			return nil, false
		}
		validMapping[k] = v
	}
	return validMapping, true
}

// LoadJobMapping parses and validates OLAKE_JOB_MAPPING JSON string
func LoadJobMapping(rawMapping string) map[int]map[string]string {
	if strings.TrimSpace(rawMapping) == "" {
		logger.Info("no JobID to Node mapping found, using empty mapping")
		return map[int]map[string]string{}
	}

	stats := JobMappingStats{InvalidMappings: make([]string, 0)}
	result := make(map[int]map[string]string)

	if err := json.Unmarshal([]byte(rawMapping), &result); err != nil {
		logger.Errorf("failed to parse OLAKE_JOB_MAPPING as json: %s", err)
		return map[int]map[string]string{}
	}

	for jobID, nodeLabels := range result {
		stats.TotalEntries++
		if valid, ok := validateJobMapping(jobID, nodeLabels, &stats); ok {
			result[jobID] = valid
			stats.ValidEntries++
		} else {
			delete(result, jobID)
		}
	}

	// Log comprehensive statistics
	logger.Infof("job mapping loaded: %d valid entries out of %d total",
		stats.ValidEntries, stats.TotalEntries)

	// Print the valid job mapping configuration as JSON
	if len(result) > 0 {
		if jsonBytes, err := json.Marshal(result); err == nil {
			logger.Debugf("job mapping configuration: %s", string(jsonBytes))
		}
	}

	if len(stats.InvalidMappings) > 0 {
		logger.Warnf("found %d invalid mappings: %s", len(stats.InvalidMappings), stats.InvalidMappings)
	}

	// Warn if no valid mappings were loaded
	if stats.ValidEntries == 0 && stats.TotalEntries > 0 {
		logger.Warnf("no valid job mappings loaded despite %d entries in configuration", stats.TotalEntries)
	}

	// Fallback to last valid mapping if available
	if stats.ValidEntries == 0 && lastValidMapping != nil {
		logger.Debugf("falling back to previous valid mapping with %d entries", len(lastValidMapping))
		return lastValidMapping
	}

	// Store successful result as fallback for future failures
	if len(result) > 0 || stats.ValidEntries > 0 {
		lastValidMapping = result
		logger.Debugf("cached valid mapping with %d entries for future fallback", len(result))
		logger.Infof("valid Job mappings:")
		for jobID, mapping := range result {
			var labels []string
			for k, v := range mapping {
				labels = append(labels, fmt.Sprintf("%s:%s", k, v))
			}
			logger.Infof("JobID %d: %s", jobID, strings.Join(labels, " "))
		}
	}
	return result
}

// LoadJobProfiles parses OLAKE_JOB_PROFILES JSON string
// Does NOT validate NodeSelector labels - trusts user input for new format
func LoadJobProfiles(profiles string) map[int]JobSchedulingConfig {
	if strings.TrimSpace(profiles) == "" {
		logger.Info("no Job Profiles found")
		return map[int]JobSchedulingConfig{}
	}

	result := make(map[int]JobSchedulingConfig)

	if err := json.Unmarshal([]byte(profiles), &result); err != nil {
		if lastValidProfiles != nil {
			logger.Errorf("failed to parse OLAKE_JOB_PROFILES as json: %s. keeping the last valid profiles (%d entries)", err, len(lastValidProfiles))
			return lastValidProfiles
		}
		logger.Errorf("failed to parse OLAKE_JOB_PROFILES as json: %s. no previous profiles to fall back on, scheduling and index storage will use chart defaults", err)
		return map[int]JobSchedulingConfig{}
	}

	// An explicit empty object is a valid instruction to clear every profile, so
	// it is cached like any other successful parse.
	lastValidProfiles = result

	logger.Infof("job profiles loaded: %d entries", len(result))

	if len(result) > 0 {
		if jsonBytes, err := json.Marshal(result); err == nil {
			logger.Debugf("job profiles configuration: %s", string(jsonBytes))
		}
	}

	return result
}

// Default values applied when a field is left unset at every configuration level.
const (
	IndexStorageModePVC           = "pvc"
	IndexStorageModeExistingClaim = "existingClaim"
	IndexStorageModeNone          = "none"

	DefaultIndexStorageMode = IndexStorageModePVC
	DefaultIndexStorageSize = "50Gi"
)

// IndexStorageConfig describes the per-job block volume that holds the Pebble
// index used by the direct positional-delete / deletion-vector write path.
// The same volume is mounted by every async operation of a job (sync and
// clear-destination), so both see the same index.
type IndexStorageConfig struct {
	// Mode selects the volume source: "pvc" (worker provisions one PVC per job),
	// "existingClaim" (mount a user supplied PVC) or "none" (no index volume).
	Mode string `json:"mode,omitempty"`
	// ExistingClaim is the PVC name used when Mode is "existingClaim".
	ExistingClaim string `json:"existingClaim,omitempty"`
	// Size is the requested volume size. Growing it is applied on the next run;
	// Kubernetes rejects shrinking.
	Size string `json:"size,omitempty"`
	// StorageClass is a passthrough to the PVC. Empty uses the cluster default.
	StorageClass string `json:"storageClass,omitempty"`
	// AccessModes defaults to ReadWriteOnce, which block storage requires.
	AccessModes []string `json:"accessModes,omitempty"`
	// MountPath is where the volume is mounted inside the connector container.
	MountPath string `json:"mountPath,omitempty"`
	// SubPath mounts a subdirectory of the volume, for laying out several jobs
	// inside one existing claim.
	SubPath string `json:"subPath,omitempty"`
	// CacheSizeMB is the Pebble block cache size in megabytes. It is resident
	// memory in the connector pod, so it must fit inside the pod's memory budget.
	CacheSizeMB int `json:"cacheSizeMB,omitempty"`
	// MaxOpenFiles caps the file descriptors Pebble keeps open for its SSTables.
	MaxOpenFiles int `json:"maxOpenFiles,omitempty"`

	Annotations map[string]string `json:"annotations,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
}

// LoadIndexStorage parses the chart-wide OLAKE_INDEX_STORAGE JSON string.
// An empty or malformed value yields the built-in defaults rather than
// disabling the index volume, so a bad edit cannot silently start running
// syncs without their index.
func LoadIndexStorage(raw string) IndexStorageConfig {
	defaults := IndexStorageConfig{
		Mode:         DefaultIndexStorageMode,
		Size:         DefaultIndexStorageSize,
		MountPath:    constants.DefaultIndexMountPath,
		AccessModes:  []string{string(corev1.ReadWriteOnce)},
		CacheSizeMB:  constants.DefaultIndexCacheSizeMB,
		MaxOpenFiles: constants.DefaultIndexMaxOpenFiles,
	}

	if strings.TrimSpace(raw) == "" {
		logger.Debugf("no OLAKE_INDEX_STORAGE found, using built-in index storage defaults")
		return defaults
	}

	var parsed IndexStorageConfig
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		logger.Errorf("failed to parse OLAKE_INDEX_STORAGE as json: %s. using built-in defaults", err)
		return defaults
	}

	return MergeIndexStorage(defaults, &parsed)
}

// MergeIndexStorage deep-merges override onto base per key, so a job that only
// overrides `size` keeps the inherited storageClass, mountPath and the rest.
func MergeIndexStorage(base IndexStorageConfig, override *IndexStorageConfig) IndexStorageConfig {
	if override == nil {
		return base
	}

	merged := base
	if override.Mode != "" {
		merged.Mode = override.Mode
	}
	if override.ExistingClaim != "" {
		merged.ExistingClaim = override.ExistingClaim
	}
	if override.Size != "" {
		merged.Size = override.Size
	}
	if override.StorageClass != "" {
		merged.StorageClass = override.StorageClass
	}
	if len(override.AccessModes) > 0 {
		merged.AccessModes = override.AccessModes
	}
	if override.MountPath != "" {
		merged.MountPath = override.MountPath
	}
	if override.SubPath != "" {
		merged.SubPath = override.SubPath
	}
	if override.CacheSizeMB != 0 {
		merged.CacheSizeMB = override.CacheSizeMB
	}
	if override.MaxOpenFiles != 0 {
		merged.MaxOpenFiles = override.MaxOpenFiles
	}
	if len(override.Annotations) > 0 {
		merged.Annotations = mergeStringMap(base.Annotations, override.Annotations)
	}
	if len(override.Labels) > 0 {
		merged.Labels = mergeStringMap(base.Labels, override.Labels)
	}
	return merged
}

func mergeStringMap(base, override map[string]string) map[string]string {
	merged := make(map[string]string, len(base)+len(override))
	for k, v := range base {
		merged[k] = v
	}
	maps.Copy(merged, override)
	return merged
}
