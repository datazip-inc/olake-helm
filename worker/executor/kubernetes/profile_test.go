package kubernetes

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/datazip-inc/olake-helm/worker/types"
)

// newProfileExecutor builds an executor backed by a watcher holding the given
// profiles, which is all the profile resolution paths read.
func newProfileExecutor(profiles map[int]JobSchedulingConfig) *KubernetesExecutor {
	return &KubernetesExecutor{
		configWatcher: &ConfigMapWatcher{jobProfiles: profiles},
	}
}

// The chart ships its index defaults as profile 0, so a job profile that
// overrides one field has to inherit the rest of that profile rather than
// starting from an empty config.
func TestResolveIndexStorageInheritsDefaultProfile(t *testing.T) {
	chartDefaults := &IndexStorageConfig{
		Mode:         "pvc",
		Size:         "50Gi",
		StorageClass: "gp3",
		AccessModes:  []string{"ReadWriteOnce"},
		MountPath:    "/var/lib/olake/index",
		CacheSizeMB:  512,
		MaxOpenFiles: 1000,
	}

	tests := []struct {
		name     string
		profiles map[int]JobSchedulingConfig
		jobID    int
		want     IndexStorageConfig
	}{
		{
			name:     "no profiles at all falls back to the built-in defaults",
			profiles: map[int]JobSchedulingConfig{},
			jobID:    7,
			want:     defaultIndexStorage(),
		},
		{
			name:     "profile 0 alone applies to every job",
			profiles: map[int]JobSchedulingConfig{0: {IndexStorage: chartDefaults}},
			jobID:    7,
			want:     *chartDefaults,
		},
		{
			name: "a job overriding only size keeps the rest of profile 0",
			profiles: map[int]JobSchedulingConfig{
				0: {IndexStorage: chartDefaults},
				2: {IndexStorage: &IndexStorageConfig{Size: "100Gi"}},
			},
			jobID: 2,
			want: IndexStorageConfig{
				Mode:         "pvc",
				Size:         "100Gi",
				StorageClass: "gp3",
				AccessModes:  []string{"ReadWriteOnce"},
				MountPath:    "/var/lib/olake/index",
				CacheSizeMB:  512,
				MaxOpenFiles: 1000,
			},
		},
		{
			name: "a job profile without indexStorage keeps profile 0 whole",
			profiles: map[int]JobSchedulingConfig{
				0: {IndexStorage: chartDefaults},
				2: {NodeSelector: map[string]string{"disk": "fast"}},
			},
			jobID: 2,
			want:  *chartDefaults,
		},
		{
			name: "an unrelated job profile does not leak into this job",
			profiles: map[int]JobSchedulingConfig{
				0: {IndexStorage: chartDefaults},
				2: {IndexStorage: &IndexStorageConfig{Size: "100Gi"}},
			},
			jobID: 3,
			want:  *chartDefaults,
		},
		{
			name: "profile 0 may disable the index for every job",
			profiles: map[int]JobSchedulingConfig{
				0: {IndexStorage: &IndexStorageConfig{Mode: "none"}},
			},
			jobID: 2,
			want: IndexStorageConfig{
				Mode:         "none",
				Size:         defaultIndexStorageSize,
				AccessModes:  []string{"ReadWriteOnce"},
				MountPath:    "/var/lib/olake/index",
				CacheSizeMB:  512,
				MaxOpenFiles: 1000,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newProfileExecutor(tt.profiles).resolveIndexStorage(tt.jobID)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("resolveIndexStorage(%d) = %+v, want %+v", tt.jobID, got, tt.want)
			}
		})
	}
}

// Scheduling inherits from profile 0 the same way index storage does. Without
// this a profile that sets only indexStorage would drop the default placement
// and send the job to an arbitrary node - and, with a zone-pinned index volume,
// leave its pod Pending on a node affinity conflict.
func TestResolveSchedulingProfileInheritsDefaultProfile(t *testing.T) {
	defaultAffinity := &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{}}
	jobAffinity := &corev1.Affinity{PodAffinity: &corev1.PodAffinity{}}
	spotToleration := []corev1.Toleration{{Key: "spot", Operator: corev1.TolerationOpExists}}

	profile0 := JobSchedulingConfig{
		NodeSelector: map[string]string{"node-type": "standard"},
		Tolerations:  spotToleration,
		Affinity:     defaultAffinity,
	}

	tests := []struct {
		name             string
		profiles         map[int]JobSchedulingConfig
		jobID            int
		operation        types.Command
		wantFound        bool
		wantNodeSelector map[string]string
		wantTolerations  []corev1.Toleration
		wantAffinity     *corev1.Affinity
	}{
		{
			name:      "no profiles leaves the caller on the legacy mapping path",
			profiles:  map[int]JobSchedulingConfig{},
			jobID:     2,
			operation: types.Sync,
			wantFound: false,
		},
		{
			// The chart always emits profile 0 to carry the index defaults. That
			// alone must not count as profile-managed scheduling, or it would
			// silently retire the deprecated jobMapping for existing users.
			name: "an index-only profile 0 leaves the legacy mapping path intact",
			profiles: map[int]JobSchedulingConfig{
				0: {IndexStorage: &IndexStorageConfig{Size: "50Gi"}},
			},
			jobID:     2,
			operation: types.Sync,
			wantFound: false,
		},
		{
			name: "a profile overriding only indexStorage keeps profile 0 scheduling",
			profiles: map[int]JobSchedulingConfig{
				0: profile0,
				2: {IndexStorage: &IndexStorageConfig{Size: "100Gi"}},
			},
			jobID:            2,
			operation:        types.Sync,
			wantFound:        true,
			wantNodeSelector: map[string]string{"node-type": "standard"},
			wantTolerations:  spotToleration,
			wantAffinity:     defaultAffinity,
		},
		{
			name: "a job overrides one field and inherits the others",
			profiles: map[int]JobSchedulingConfig{
				0: profile0,
				2: {Affinity: jobAffinity},
			},
			jobID:            2,
			operation:        types.Sync,
			wantFound:        true,
			wantNodeSelector: map[string]string{"node-type": "standard"},
			wantTolerations:  spotToleration,
			wantAffinity:     jobAffinity,
		},
		{
			name: "an explicitly empty value clears what profile 0 provides",
			profiles: map[int]JobSchedulingConfig{
				0: profile0,
				2: {NodeSelector: map[string]string{}, Tolerations: []corev1.Toleration{}},
			},
			jobID:            2,
			operation:        types.Sync,
			wantFound:        true,
			wantNodeSelector: map[string]string{},
			wantTolerations:  []corev1.Toleration{},
			wantAffinity:     defaultAffinity,
		},
		{
			name: "short-lived operations run on profile 0, never on the job profile",
			profiles: map[int]JobSchedulingConfig{
				0: profile0,
				2: {NodeSelector: map[string]string{"gpu": "true"}},
			},
			jobID:            2,
			operation:        types.Discover,
			wantFound:        true,
			wantNodeSelector: map[string]string{"node-type": "standard"},
			wantTolerations:  spotToleration,
			wantAffinity:     defaultAffinity,
		},
		{
			name: "a job profile applies with no profile 0 present",
			profiles: map[int]JobSchedulingConfig{
				2: {NodeSelector: map[string]string{"gpu": "true"}},
			},
			jobID:            2,
			operation:        types.Sync,
			wantFound:        true,
			wantNodeSelector: map[string]string{"gpu": "true"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := newProfileExecutor(tt.profiles)

			got, found := k.resolveSchedulingProfile(tt.jobID, tt.operation)
			if found != tt.wantFound {
				t.Fatalf("resolveSchedulingProfile(%d, %s) found = %v, want %v", tt.jobID, tt.operation, found, tt.wantFound)
			}
			if !tt.wantFound {
				return
			}
			if !reflect.DeepEqual(got.NodeSelector, tt.wantNodeSelector) {
				t.Errorf("nodeSelector = %+v, want %+v", got.NodeSelector, tt.wantNodeSelector)
			}
			if !reflect.DeepEqual(got.Tolerations, tt.wantTolerations) {
				t.Errorf("tolerations = %+v, want %+v", got.Tolerations, tt.wantTolerations)
			}
			if got.Affinity != tt.wantAffinity {
				t.Errorf("affinity = %+v, want %+v", got.Affinity, tt.wantAffinity)
			}

			// The exported helpers must agree with the resolution above.
			if selector := k.GetNodeSelectorForJob(tt.jobID, tt.operation); !reflect.DeepEqual(selector, tt.wantNodeSelector) {
				t.Errorf("GetNodeSelectorForJob = %+v, want %+v", selector, tt.wantNodeSelector)
			}
			if affinity := k.BuildAffinityForJob(tt.jobID, tt.operation); affinity != tt.wantAffinity {
				t.Errorf("BuildAffinityForJob = %+v, want %+v", affinity, tt.wantAffinity)
			}
		})
	}
}
