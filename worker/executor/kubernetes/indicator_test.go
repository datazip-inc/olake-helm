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

	"github.com/datazip-inc/olake-helm/worker/types"
)

func newIndicatorExecutor(client *fake.Clientset) *KubernetesExecutor {
	return &KubernetesExecutor{
		client:        client,
		namespace:     "olake",
		config:        &KubernetesConfig{},
		configWatcher: &ConfigMapWatcher{},
	}
}

func spawnRequest() *types.IndicatorRequest {
	return &types.IndicatorRequest{
		Action:       types.IndicatorActionSpawn,
		Name:         "olake-failed-source-pg-cm",
		Namespace:    "olake",
		Kind:         "source",
		ResourceName: "pg-cm",
		Message:      "password authentication failed",
	}
}

func TestIndicatorSpawnRecordsPodEvent(t *testing.T) {
	ctx := context.Background()
	client := fake.NewClientset()
	client.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		pod := action.(k8stesting.CreateAction).GetObject().(*corev1.Pod)
		pod.UID = "pod-uid" // the API server sets it; the fake does not
		return false, nil, nil
	})

	if err := newIndicatorExecutor(client).Indicator(ctx, spawnRequest()); err != nil {
		t.Fatalf("spawn: %v", err)
	}
	if _, err := client.CoreV1().Pods("olake").Get(ctx, "olake-failed-source-pg-cm", metav1.GetOptions{}); err != nil {
		t.Fatalf("indicator pod not created: %v", err)
	}

	events, err := client.CoreV1().Events("olake").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(events.Items) != 1 {
		t.Fatalf("want 1 event, got %d", len(events.Items))
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
}

func TestIndicatorSpawnWithoutEventPermission(t *testing.T) {
	// a Role from before events were granted: the pod still reports the error
	ctx := context.Background()
	client := fake.NewClientset()
	client.PrependReactor("create", "events", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "events"}, "", nil)
	})

	if err := newIndicatorExecutor(client).Indicator(ctx, spawnRequest()); err != nil {
		t.Fatalf("spawn failed because the event was forbidden: %v", err)
	}
	if _, err := client.CoreV1().Pods("olake").Get(ctx, "olake-failed-source-pg-cm", metav1.GetOptions{}); err != nil {
		t.Fatalf("indicator pod not created: %v", err)
	}
}
