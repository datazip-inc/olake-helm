package kubernetes

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Package-level variable to store last known good mapping for fallback
var lastValidMapping map[int]map[string]string

// JobMappingStats contains statistics about job mapping loading
type JobMappingStats struct {
	TotalEntries    int
	ValidEntries    int
	InvalidMappings []string // TODO:  remove invalidMappings from struct{}
}

// JobSchedulingConfig defines the scheduling constraints and compute resources for a job
type JobSchedulingConfig struct {
	NodeSelector map[string]string            `json:"nodeSelector,omitempty"`
	Tolerations  []corev1.Toleration          `json:"tolerations,omitempty"`
	Affinity     *corev1.Affinity             `json:"affinity,omitempty"`
	Resources    *corev1.ResourceRequirements `json:"resources,omitempty"`
}

// validateResources rejects negative quantities and a request that exceeds its limit;
// the API server would otherwise refuse every pod built from the profile.
// Runs on every load, so it also covers a ConfigMap edited directly, where the Helm schema does not run.
func validateResources(r *corev1.ResourceRequirements) error {
	if r == nil {
		return nil
	}
	for kind, list := range map[string]corev1.ResourceList{"request": r.Requests, "limit": r.Limits} {
		for name, quantity := range list {
			if quantity.Sign() < 0 {
				return fmt.Errorf("%s %s %s is negative", name, kind, quantity.String())
			}
		}
	}
	for name, limit := range r.Limits {
		if request, ok := r.Requests[name]; ok && request.Cmp(limit) > 0 {
			return fmt.Errorf("%s request %s exceeds limit %s", name, request.String(), limit.String())
		}
	}
	return nil
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

// parseJobProfile parses one profile. When only its resources fail to parse
// (for example "2GB"), the profile is returned without resources, along with
// the parse error as resourcesErr, so its scheduling fields still apply.
func parseJobProfile(raw json.RawMessage) (profile JobSchedulingConfig, resourcesErr error, err error) {
	if err = json.Unmarshal(raw, &profile); err == nil {
		return profile, nil, nil
	}

	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return JobSchedulingConfig{}, nil, err
	}
	if _, hasResources := fields["resources"]; !hasResources {
		return JobSchedulingConfig{}, nil, err
	}
	delete(fields, "resources")

	stripped, marshalErr := json.Marshal(fields)
	if marshalErr != nil {
		return JobSchedulingConfig{}, nil, err
	}
	profile = JobSchedulingConfig{}
	if json.Unmarshal(stripped, &profile) != nil {
		return JobSchedulingConfig{}, nil, err
	}
	return profile, err, nil
}

// LoadJobProfiles parses OLAKE_JOB_PROFILES JSON string
// Does NOT validate NodeSelector labels - trusts user input for new format
func LoadJobProfiles(profiles string) map[int]JobSchedulingConfig {
	if strings.TrimSpace(profiles) == "" {
		logger.Info("no Job Profiles found")
		return map[int]JobSchedulingConfig{}
	}

	// Parse each profile separately so one bad entry cannot drop every other profile
	rawProfiles := make(map[int]json.RawMessage)
	if err := json.Unmarshal([]byte(profiles), &rawProfiles); err != nil {
		logger.Errorf("failed to parse OLAKE_JOB_PROFILES as json: %s", err)
		return map[int]JobSchedulingConfig{}
	}

	result := make(map[int]JobSchedulingConfig, len(rawProfiles))
	for jobID, raw := range rawProfiles {
		profile, resourcesErr, err := parseJobProfile(raw)
		if err != nil {
			logger.Errorf("job profile %d: skipping invalid profile: %s", jobID, err)
			continue
		}
		if resourcesErr != nil {
			logger.Warnf("job profile %d: ignoring unparsable resources, falling back to default: %s", jobID, resourcesErr)
		}

		// Drop only the invalid resources so the profile's scheduling fields still apply
		if err := validateResources(profile.Resources); err != nil {
			logger.Warnf("job profile %d: ignoring resources, falling back to default: %s", jobID, err)
			profile.Resources = nil
		}
		result[jobID] = profile
	}

	logger.Infof("job profiles loaded: %d entries", len(result))

	if len(result) > 0 {
		if jsonBytes, err := json.Marshal(result); err == nil {
			logger.Debugf("job profiles configuration: %s", string(jsonBytes))
		}
	}

	return result
}
