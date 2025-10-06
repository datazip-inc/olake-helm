package kubernetes

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/datazip-inc/olake-helm/worker/logger"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Package-level variable to store last known good mapping for fallback
var lastValidMapping map[int]map[string]string

// JobMappingStats contains statistics about job mapping loading
type JobMappingStats struct {
	TotalEntries    int
	ValidEntries    int
	InvalidMappings []string
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
		err := fmt.Errorf("invalid label key '%s': %v", key, errs)
		stats.InvalidMappings = append(stats.InvalidMappings, fmt.Sprintf("JobID %d: %s", jobID, err))
		return err
	}
	if errs := validation.IsValidLabelValue(value); len(errs) > 0 {
		err := fmt.Errorf("invalid label value '%s' for key '%s': %v", value, key, errs)
		stats.InvalidMappings = append(stats.InvalidMappings, fmt.Sprintf("JobID %d: %s", jobID, err))
		return err
	}
	return nil
}

// validateJobMapping validates a single job mapping entry
func validateJobMapping(jobID int, nodeLabels map[string]string, stats *JobMappingStats) (map[string]string, bool) {
	if jobID <= 0 {
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
		logger.Info("No JobID to Node mapping found, using empty mapping")
		return map[int]map[string]string{}
	}

	stats := JobMappingStats{InvalidMappings: make([]string, 0)}
	result := make(map[int]map[string]string)

	if err := json.Unmarshal([]byte(rawMapping), &result); err != nil {
		logger.Errorf("Failed to parse OLAKE_JOB_MAPPING as JSON: %v", err)
		return map[int]map[string]string{}
	}

	for jobID, nodeLabels := range result {
		stats.TotalEntries++
		if valid, ok := validateJobMapping(jobID, nodeLabels, &stats); ok {
			result[jobID] = valid
			stats.ValidEntries++
		}
	}

	// Log comprehensive statistics
	logger.Infof("Job mapping loaded: %d valid entries out of %d total",
		stats.ValidEntries, stats.TotalEntries)

	// Print the valid job mapping configuration as JSON
	if len(result) > 0 {
		if jsonBytes, err := json.Marshal(result); err == nil {
			logger.Debugf("Job mapping configuration: %s", string(jsonBytes))
		}
	}

	if len(stats.InvalidMappings) > 0 {
		logger.Errorf("Found %d invalid mappings: %v", len(stats.InvalidMappings), stats.InvalidMappings)
	}

	// Warn if no valid mappings were loaded
	if stats.ValidEntries == 0 && stats.TotalEntries > 0 {
		logger.Errorf("No valid job mappings loaded despite %d entries in configuration", stats.TotalEntries)
	}

	// Fallback to last valid mapping if available
	if stats.ValidEntries == 0 && lastValidMapping != nil {
		logger.Debugf("Falling back to previous valid mapping with %d entries", len(lastValidMapping))
		return lastValidMapping
	}

	// Store successful result as fallback for future failures
	if len(result) > 0 || stats.ValidEntries > 0 {
		lastValidMapping = result
		logger.Debugf("Cached valid mapping with %d entries for future fallback", len(result))
		logger.Infof("Valid Job mappings:")
		for jobID, mapping := range result {
			var labels []string
			for k, v := range mapping {
				labels = append(labels, fmt.Sprintf("%s:%s", k, v))
			}
			logger.Infof("  JobID %d: %s", jobID, strings.Join(labels, " "))
		}
	}
	return result
}
