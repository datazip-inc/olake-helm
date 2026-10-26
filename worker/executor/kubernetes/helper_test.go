package kubernetes

import (
	"strings"
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

func TestParseJobVolumes(t *testing.T) {
	const (
		configMapVol = `[{"name":"ca","configMap":{"name":"my-ca"}}]`
		csiVol       = `[{"name":"kc","csi":{"driver":"secrets-store.csi.k8s.io","readOnly":true,"volumeAttributes":{"secretProviderClass":"kafka"}}}]`
		caMount      = `[{"name":"ca","mountPath":"/etc/ssl/custom-certs"}]`
	)

	tests := []struct {
		name        string
		volumes     string
		mounts      string
		wantVolumes int
		wantMounts  int
		wantErr     string
	}{
		{
			name: "nothing set",
		},
		{
			name:        "configMap volume and mount",
			volumes:     configMapVol,
			mounts:      caMount,
			wantVolumes: 1,
			wantMounts:  1,
		},
		{
			name:        "csi volume and mount",
			volumes:     csiVol,
			mounts:      `[{"name":"kc","mountPath":"/etc/ssl/kafka"}]`,
			wantVolumes: 1,
			wantMounts:  1,
		},
		{
			name:        "volume without mount",
			volumes:     configMapVol,
			wantVolumes: 1,
		},
		{
			name:        "path next to the config dir",
			volumes:     configMapVol,
			mounts:      `[{"name":"ca","mountPath":"/mnt/configs"}]`,
			wantVolumes: 1,
			wantMounts:  1,
		},
		{
			name:        "multiple volumes and mounts",
			volumes:     `[{"name":"a"},{"name":"b"}]`,
			mounts:      `[{"name":"a","mountPath":"/x"},{"name":"b","mountPath":"/y"}]`,
			wantVolumes: 2,
			wantMounts:  2,
		},
		{
			name:        "one volume at multiple paths",
			volumes:     configMapVol,
			mounts:      `[{"name":"ca","mountPath":"/x"},{"name":"ca","mountPath":"/y","subPath":"certs"}]`,
			wantVolumes: 1,
			wantMounts:  2,
		},
		{
			name:    "reserved index volume name",
			volumes: `[{"name":"index-storage","configMap":{"name":"x"}}]`,
			wantErr: "reserved",
		},
		{
			name:    "invalid volumes json",
			volumes: `{`,
			wantErr: "unmarshal job volumes",
		},
		{
			name:    "invalid mounts json",
			volumes: configMapVol,
			mounts:  `{`,
			wantErr: "unmarshal job volume mounts",
		},
		{
			name:    "volume without name",
			volumes: `[{"configMap":{"name":"x"}}]`,
			wantErr: "missing a name",
		},
		{
			name:    "reserved volume name",
			volumes: `[{"name":"job-storage","configMap":{"name":"x"}}]`,
			wantErr: "reserved",
		},
		{
			name:    "duplicate volume name",
			volumes: `[{"name":"a"},{"name":"a"}]`,
			wantErr: "more than once",
		},
		{
			name:    "mount without any volume",
			mounts:  caMount,
			wantErr: "does not match",
		},
		{
			name:    "mount for unknown volume",
			volumes: configMapVol,
			mounts:  `[{"name":"nope","mountPath":"/x"}]`,
			wantErr: "does not match",
		},
		{
			name:    "mount at container root",
			volumes: configMapVol,
			mounts:  `[{"name":"ca","mountPath":"/"}]`,
			wantErr: "container root",
		},
		{
			name:    "mount without path",
			volumes: configMapVol,
			mounts:  `[{"name":"ca"}]`,
			wantErr: "container root",
		},
		{
			name:    "mount at config dir",
			volumes: configMapVol,
			mounts:  `[{"name":"ca","mountPath":"/mnt/config"}]`,
			wantErr: "shadow",
		},
		{
			name:    "mount inside config dir",
			volumes: configMapVol,
			mounts:  `[{"name":"ca","mountPath":"/mnt/config/certs"}]`,
			wantErr: "shadow",
		},
		{
			name:    "duplicate mount path",
			volumes: `[{"name":"a"},{"name":"b"}]`,
			mounts:  `[{"name":"a","mountPath":"/x"},{"name":"b","mountPath":"/x/"}]`,
			wantErr: "more than once",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			volumes, mounts, err := parseJobVolumes(tt.volumes, tt.mounts)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("parseJobVolumes() error = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseJobVolumes() unexpected error: %v", err)
			}
			if len(volumes) != tt.wantVolumes || len(mounts) != tt.wantMounts {
				t.Errorf("parseJobVolumes() returned %d volumes, %d mounts, want %d, %d",
					len(volumes), len(mounts), tt.wantVolumes, tt.wantMounts)
			}
		})
	}
}
