package kubernetes

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/spf13/viper"
)

const (
	workerPod   = "olake-workers-abc"
	workerImage = "registry.local/olakego/ui-worker:v1.2.3"
)

func newIndicatorExecutor(client *fake.Clientset) *KubernetesExecutor {
	return &KubernetesExecutor{
		client:        client,
		namespace:     "olake",
		config:        &KubernetesConfig{},
		configWatcher: &ConfigMapWatcher{},
	}
}

// newWorkerClient returns a fake clientset holding the worker's own pod with the given containers,
// or none when containers is nil.
func newWorkerClient(t *testing.T, containers []corev1.Container) *fake.Clientset {
	viper.Set(constants.EnvPodName, workerPod)
	t.Cleanup(func() { viper.Set(constants.EnvPodName, "") })
	if containers == nil {
		return fake.NewClientset()
	}
	return fake.NewClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: workerPod, Namespace: "olake"},
		Spec:       corev1.PodSpec{Containers: containers},
	})
}

func spawnRequest() *types.IndicatorRequest {
	return &types.IndicatorRequest{
		Action:       types.IndicatorActionSpawn,
		Name:         "olake-failed-source-pg-cm",
		Kind:         "source",
		ResourceName: "pg-cm",
		Message:      "password authentication failed",
	}
}

func TestIndicatorImage(t *testing.T) {
	worker := corev1.Container{Name: constants.WorkerContainerName, Image: workerImage}
	sidecar := corev1.Container{Name: "istio-proxy", Image: "istio/proxyv2:1.20"}

	tests := []struct {
		name       string
		containers []corev1.Container
		want       string
		wantErr    bool
	}{
		{"worker container", []corev1.Container{worker}, workerImage, false},
		{"worker container after a sidecar", []corev1.Container{sidecar, worker}, workerImage, false},
		{"no worker container", []corev1.Container{{Name: "app", Image: workerImage}, sidecar}, "", true},
		{"unreadable worker pod", nil, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newIndicatorExecutor(newWorkerClient(t, tt.containers)).indicatorImage(context.Background())
			if (err != nil) != tt.wantErr {
				t.Fatalf("indicatorImage() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("indicatorImage() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIndicator(t *testing.T) {
	worker := []corev1.Container{{Name: constants.WorkerContainerName, Image: workerImage}}
	forbidEvents := func(client *fake.Clientset) {
		client.PrependReactor("create", "events", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "events"}, "", nil)
		})
	}
	oldPodTerminating := func(client *fake.Clientset) {
		client.PrependReactor("create", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewAlreadyExists(schema.GroupResource{Resource: "pods"}, "olake-failed-source-pg-cm")
		})
	}

	tests := []struct {
		name       string
		containers []corev1.Container
		setup      func(*fake.Clientset)
		wantErr    bool
		wantPod    bool
		wantEvents int
	}{
		{name: "spawn records a pod event", containers: worker, wantPod: true, wantEvents: 1},
		// the activity retries; the pod is already up and shows the error meanwhile
		{name: "spawn with a failed event", containers: worker, setup: forbidEvents, wantErr: true, wantPod: true},
		// the activity retries; no pod is created without the worker image
		{name: "spawn with unreadable worker pod", wantErr: true},
		// the activity retries once the old pod is gone, instead of reporting an indicator that vanishes
		{name: "spawn while the old pod is terminating", containers: worker, setup: oldPodTerminating, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			client := newWorkerClient(t, tt.containers)
			client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
				pod := action.(k8stesting.CreateAction).GetObject().(*corev1.Pod)
				pod.UID = "pod-uid" // the API server sets it; the fake does not
				return false, nil, nil
			})
			if tt.setup != nil {
				tt.setup(client)
			}

			err := newIndicatorExecutor(client).Indicator(ctx, spawnRequest())
			if (err != nil) != tt.wantErr {
				t.Fatalf("Indicator() error = %v, wantErr %v", err, tt.wantErr)
			}

			pod, err := client.CoreV1().Pods("olake").Get(ctx, "olake-failed-source-pg-cm", metav1.GetOptions{})
			if !tt.wantPod {
				if !apierrors.IsNotFound(err) {
					t.Errorf("indicator pod created: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("indicator pod not created: %v", err)
			}
			if got := pod.Spec.Containers[0].Image; got != workerImage {
				t.Errorf("indicator image %q, want %q", got, workerImage)
			}
			if token := pod.Spec.AutomountServiceAccountToken; token == nil || *token {
				t.Errorf("indicator pod mounts a ServiceAccount token")
			}

			events, err := client.CoreV1().Events("olake").List(ctx, metav1.ListOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if len(events.Items) != tt.wantEvents {
				t.Fatalf("want %d events, got %d", tt.wantEvents, len(events.Items))
			}
			if tt.wantEvents == 0 {
				return
			}
			got := events.Items[0]
			want := corev1.ObjectReference{APIVersion: "v1", Kind: "Pod", Namespace: "olake", Name: "olake-failed-source-pg-cm", UID: "pod-uid"}
			if got.InvolvedObject != want {
				t.Errorf("event involves %+v, want %+v (with the UID, so kubectl describe pod lists it)", got.InvolvedObject, want)
			}
			if got.Type != corev1.EventTypeWarning || got.Reason != "SyncFailed" {
				t.Errorf("event is %s/%s, want Warning/SyncFailed", got.Type, got.Reason)
			}
			if wantMsg := `GitOps source "pg-cm" failed to sync: password authentication failed`; got.Message != wantMsg {
				t.Errorf("event message %q, want %q", got.Message, wantMsg)
			}
		})
	}
}
