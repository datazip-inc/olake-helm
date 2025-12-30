package kubernetes

import (
	"encoding/json"
	"strings"

	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	corev1 "k8s.io/api/core/v1"
)

// JobSchedulingConfig defines the scheduling constraints for a job
type JobSchedulingConfig struct {
	NodeSelector map[string]string   `json:"nodeSelector,omitempty"`
	Tolerations  []corev1.Toleration `json:"tolerations,omitempty"`
	Affinity     *corev1.Affinity    `json:"affinity,omitempty"`
}

// LoadJobMapping parses legacy OLAKE_JOB_MAPPING JSON string
// Returns a map of JobSchedulingConfig to maintain interface consistency
// TODO: Remove this legacy function when OLAKE_JOB_MAPPING is fully removed.
// It parses the old nested map format which is now deprecated.
func LoadJobMapping(mapping string) map[int]JobSchedulingConfig {
	if strings.TrimSpace(mapping) == "" {
		logger.Info("no legacy JobID to Node mapping found")
		return map[int]JobSchedulingConfig{}
	}

	legacyResult := make(map[int]map[string]string)
	result := make(map[int]JobSchedulingConfig)

	if err := json.Unmarshal([]byte(mapping), &legacyResult); err != nil {
		logger.Errorf("failed to parse OLAKE_JOB_MAPPING as json: %s", err)
		return map[int]JobSchedulingConfig{}
	}

	for jobID, nodeLabels := range legacyResult {
		result[jobID] = JobSchedulingConfig{
			NodeSelector: nodeLabels,
		}
	}

	logger.Infof("legacy mapping loaded: %d entries", len(result))

	if len(result) > 0 {
		if jsonBytes, err := json.Marshal(result); err == nil {
			logger.Debugf("legacy mapping configuration: %s", string(jsonBytes))
		}
	}

	return result
}

// LoadJobProfiles parses OLAKE_JOB_PROFILES JSON string
func LoadJobProfiles(profiles string) map[int]JobSchedulingConfig {
	if strings.TrimSpace(profiles) == "" {
		logger.Info("no Job Profiles found")
		return map[int]JobSchedulingConfig{}
	}

	result := make(map[int]JobSchedulingConfig)

	if err := json.Unmarshal([]byte(profiles), &result); err != nil {
		logger.Errorf("failed to parse OLAKE_JOB_PROFILES as json: %s", err)
		return map[int]JobSchedulingConfig{}
	}

	logger.Infof("job profiles loaded: %d entries", len(result))

	if len(result) > 0 {
		if jsonBytes, err := json.Marshal(result); err == nil {
			logger.Debugf("job profiles configuration: %s", string(jsonBytes))
		}
	}

	return result
}
