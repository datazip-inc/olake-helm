package kubernetes

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestLoadJobProfilesResources(t *testing.T) {
	raw := `{
		"0":   {"resources": {"requests": {"cpu": "500m", "memory": "1Gi"}, "limits": {"memory": "2Gi"}}},
		"123": {"nodeSelector": {"node-type": "big"}, "resources": {"requests": {"cpu": 2}}},
		"456": {"nodeSelector": {"node-type": "big"}, "resources": {"requests": {"memory": "4Gi"}, "limits": {"memory": "1Gi"}}}
	}`

	profiles := LoadJobProfiles(raw)
	if len(profiles) != 3 {
		t.Fatalf("expected 3 profiles, got %d", len(profiles))
	}

	def := profiles[0].Resources
	if def == nil {
		t.Fatal("default profile resources not parsed")
	}
	if mem := def.Limits[corev1.ResourceMemory]; mem.String() != "2Gi" {
		t.Errorf("default memory limit = %s, want 2Gi", mem.String())
	}
	if cpu := def.Requests[corev1.ResourceCPU]; cpu.String() != "500m" {
		t.Errorf("default cpu request = %s, want 500m", cpu.String())
	}

	// A plain JSON number is a valid quantity (YAML `cpu: 2`)
	if cpu := profiles[123].Resources.Requests[corev1.ResourceCPU]; cpu.String() != "2" {
		t.Errorf("job 123 cpu request = %s, want 2", cpu.String())
	}

	// Request above limit: resources dropped, scheduling kept
	if profiles[456].Resources != nil {
		t.Errorf("job 456 resources should be dropped when request exceeds limit")
	}
	if profiles[456].NodeSelector["node-type"] != "big" {
		t.Errorf("job 456 nodeSelector should be kept when resources are dropped")
	}
}

func TestLoadJobProfilesRejectsNegativeResources(t *testing.T) {
	raw := `{
		"1": {"nodeSelector": {"node-type": "big"}, "resources": {"requests": {"cpu": -1}}},
		"2": {"resources": {"limits": {"memory": "-1Gi"}}}
	}`

	profiles := LoadJobProfiles(raw)
	for jobID, profile := range profiles {
		if profile.Resources != nil {
			t.Errorf("job %d: negative resources should be dropped", jobID)
		}
	}
	if profiles[1].NodeSelector["node-type"] != "big" {
		t.Errorf("job 1 nodeSelector should be kept when resources are dropped")
	}
}

func TestLoadJobProfilesIsolatesBadProfiles(t *testing.T) {
	raw := `{
		"0":   {"nodeSelector": {"node-type": "standard"}, "resources": {"requests": {"memory": "1Gi"}}},
		"123": {"nodeSelector": {"node-type": "big"}, "resources": {"limits": {"memory": "2GB"}}},
		"456": {"nodeSelector": "not-a-map"}
	}`

	profiles := LoadJobProfiles(raw)

	// A good profile survives a bad neighbour
	if mem := profiles[0].Resources.Requests[corev1.ResourceMemory]; mem.String() != "1Gi" {
		t.Errorf("job 0 memory request = %s, want 1Gi", mem.String())
	}

	// An unparsable quantity drops only that profile's resources
	if profiles[123].Resources != nil {
		t.Errorf("job 123 resources should be dropped when a quantity cannot be parsed")
	}
	if profiles[123].NodeSelector["node-type"] != "big" {
		t.Errorf("job 123 nodeSelector should be kept when only resources are invalid")
	}

	// A profile broken outside resources is skipped entirely
	if _, exists := profiles[456]; exists {
		t.Errorf("job 456 should be skipped when its scheduling fields are invalid")
	}
}

func TestLoadJobProfilesWithoutResources(t *testing.T) {
	profiles := LoadJobProfiles(`{"0": {"nodeSelector": {"node-type": "standard"}}}`)
	if profiles[0].Resources != nil {
		t.Errorf("resources should stay nil when not configured")
	}
}
