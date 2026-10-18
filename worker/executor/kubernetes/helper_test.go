package kubernetes

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/datazip-inc/olake-helm/worker/types"
)

func newTestExecutor(profiles map[int]JobSchedulingConfig) *KubernetesExecutor {
	if profiles == nil {
		profiles = map[int]JobSchedulingConfig{}
	}
	return &KubernetesExecutor{configWatcher: &ConfigMapWatcher{jobProfiles: profiles}}
}

func TestGetResourcesForJob(t *testing.T) {
	heavy := &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("8Gi")},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("8Gi")},
	}
	base := &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi"), corev1.ResourceCPU: resource.MustParse("500m")},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi")},
	}

	tests := []struct {
		name     string
		profiles map[int]JobSchedulingConfig
		jobID    int
		op       types.Command
		want     corev1.ResourceRequirements
	}{
		{
			name:  "no profiles uses built-in default",
			jobID: 1,
			op:    types.Sync,
			want:  defaultJobResources(),
		},
		{
			name:     "profiles without resources use built-in default",
			profiles: map[int]JobSchedulingConfig{0: {NodeSelector: map[string]string{"a": "b"}}},
			jobID:    1,
			op:       types.Sync,
			want:     defaultJobResources(),
		},
		{
			name:     "default profile applies to unmapped job",
			profiles: map[int]JobSchedulingConfig{0: {Resources: base}},
			jobID:    1,
			op:       types.Sync,
			want:     *base,
		},
		{
			name:     "job profile wins for sync",
			profiles: map[int]JobSchedulingConfig{0: {Resources: base}, 123: {Resources: heavy}},
			jobID:    123,
			op:       types.Sync,
			want:     *heavy,
		},
		{
			name:     "job profile wins for clear destination",
			profiles: map[int]JobSchedulingConfig{0: {Resources: base}, 123: {Resources: heavy}},
			jobID:    123,
			op:       types.ClearDestination,
			want:     *heavy,
		},
		{
			name:     "job profile ignored for discover",
			profiles: map[int]JobSchedulingConfig{0: {Resources: base}, 123: {Resources: heavy}},
			jobID:    123,
			op:       types.Discover,
			want:     *base,
		},
		{
			name:     "job profile ignored for discover without default profile",
			profiles: map[int]JobSchedulingConfig{123: {Resources: heavy}},
			jobID:    123,
			op:       types.Discover,
			want:     defaultJobResources(),
		},
		{
			name:     "job profile without resources falls back to default profile",
			profiles: map[int]JobSchedulingConfig{0: {Resources: base}, 123: {NodeSelector: map[string]string{"a": "b"}}},
			jobID:    123,
			op:       types.Sync,
			want:     *base,
		},
		{
			name:     "empty resources treated as unset",
			profiles: map[int]JobSchedulingConfig{123: {Resources: &corev1.ResourceRequirements{}}},
			jobID:    123,
			op:       types.Sync,
			want:     defaultJobResources(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newTestExecutor(tt.profiles).GetResourcesForJob(tt.jobID, tt.op)
			if !apiequality.Semantic.DeepEqual(got, tt.want) {
				t.Errorf("GetResourcesForJob() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestGetResourcesForJobReturnsCopy(t *testing.T) {
	base := &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")},
	}
	k := newTestExecutor(map[int]JobSchedulingConfig{0: {Resources: base}})

	got := k.GetResourcesForJob(1, types.Sync)
	got.Requests[corev1.ResourceMemory] = resource.MustParse("64Gi")

	if mem := base.Requests[corev1.ResourceMemory]; mem.String() != "1Gi" {
		t.Errorf("mutating returned resources changed the shared profile: got %s", mem.String())
	}
}
