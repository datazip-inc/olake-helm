package kubernetes

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestMemoryLimitKey(t *testing.T) {
	tests := []struct {
		name      string
		operation string
		jobID     string
		want      string
	}{
		{"sync points at the job profile", "sync", "123", "global.jobProfiles.123.resources.limits.memory"},
		{"clear destination points at the job profile", "clear-destination", "123", "global.jobProfiles.123.resources.limits.memory"},
		{"discover points at the default profile", "discover", "123", "global.jobProfiles.0.resources.limits.memory"},
		{"check points at the default profile", "check", "0", "global.jobProfiles.0.resources.limits.memory"},
		{"missing job id points at the default profile", "sync", "", "global.jobProfiles.0.resources.limits.memory"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				"olake.io/operation-type": tt.operation,
				"olake.io/job-id":         tt.jobID,
			}}}
			if got := memoryLimitKey(pod); got != tt.want {
				t.Errorf("memoryLimitKey() = %s, want %s", got, tt.want)
			}
		})
	}
}
