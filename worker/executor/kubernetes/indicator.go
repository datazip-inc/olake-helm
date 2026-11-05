// GitOps: failure-indicator pods for ConfigMap/Secret validation errors (spawn/delete via IndicatorWorkflow).
package kubernetes

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
)

func (k *KubernetesExecutor) Indicator(ctx context.Context, req *types.IndicatorRequest) error {
	switch req.Action {
	case types.IndicatorActionDelete:
		return k.cleanupPod(ctx, req.Name)
	case types.IndicatorActionSpawn:
		return k.spawnIndicatorPod(ctx, req)
	default:
		return fmt.Errorf("unknown indicator action %q", req.Action)
	}
}

func (k *KubernetesExecutor) spawnIndicatorPod(ctx context.Context, req *types.IndicatorRequest) error {
	log := logger.Log(ctx)
	name := req.Name
	// read the image first, so a failed read keeps the previous indicator in place
	image, err := k.indicatorImage(ctx)
	if err != nil {
		log.Error("read worker image failed", "podName", workerPodName(), "namespace", k.namespace, "error", err)
		return fmt.Errorf("read worker image: %w", err)
	}

	// delete without a grace period, unlike cleanupPod, so the create below rarely finds the old pod
	zero := int64(0)
	_ = k.client.CoreV1().Pods(k.namespace).Delete(ctx, name, metav1.DeleteOptions{GracePeriodSeconds: &zero})

	pod, err := k.client.CoreV1().Pods(k.namespace).Create(ctx, k.IndicatorPodSpec(image, req), metav1.CreateOptions{})
	if err != nil {
		// AlreadyExists included: the old pod may still be terminating, so report it and let
		// the activity retry instead of losing the indicator once the old pod is gone
		log.Error("spawn indicator pod failed", "podName", name, "namespace", k.namespace, "error", err)
		return fmt.Errorf("spawn indicator pod: %w", err)
	}
	// best effort: the pod already shows the error, and a retry would recreate it for an event that
	// fails the same way, e.g. on a worker Role without events create (useStandardResources: false)
	if err := k.emitIndicatorEvent(ctx, pod, req); err != nil {
		log.Warn("record indicator pod event failed", "podName", name, "namespace", k.namespace, "error", err)
	}
	return nil
}

// emitIndicatorEvent records a Warning event on the indicator pod with the error, so event-based
// alerting and `kubectl describe pod` show why the pod exists.
func (k *KubernetesExecutor) emitIndicatorEvent(ctx context.Context, pod *corev1.Pod, req *types.IndicatorRequest) error {
	now := metav1.Now()
	event := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			// the name client-go's event recorder uses: the object name and a unique suffix
			Name:      fmt.Sprintf("%s.%x", pod.Name, now.UnixNano()),
			Namespace: pod.Namespace,
		},
		InvolvedObject: corev1.ObjectReference{
			APIVersion: "v1",
			Kind:       "Pod",
			Namespace:  pod.Namespace,
			Name:       pod.Name,
			UID:        pod.UID,
		},
		Type:           corev1.EventTypeWarning,
		Reason:         constants.IndicatorEventReason,
		Message:        utils.Truncate(fmt.Sprintf("GitOps %s %q failed to sync: %s", req.Kind, req.ResourceName, req.Message), constants.IndicatorAnnotationMax),
		Source:         corev1.EventSource{Component: constants.IndicatorEventComponent},
		FirstTimestamp: now,
		LastTimestamp:  now,
		Count:          1,
	}
	_, err := k.client.CoreV1().Events(pod.Namespace).Create(ctx, event, metav1.CreateOptions{})
	return err
}

// indicatorImage returns the image of the worker's own container. It is always pullable in this
// cluster, so the indicator needs no image beyond the ones OLake already runs.
func (k *KubernetesExecutor) indicatorImage(ctx context.Context) (string, error) {
	pod, err := k.client.CoreV1().Pods(k.namespace).Get(ctx, workerPodName(), metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	for _, c := range pod.Spec.Containers {
		if c.Name == constants.WorkerContainerName {
			return c.Image, nil
		}
	}
	return "", fmt.Errorf("worker pod %s has no %s container", pod.Name, constants.WorkerContainerName)
}

// IndicatorPodSpec builds the failure-indicator pod.
func (k *KubernetesExecutor) IndicatorPodSpec(image string, req *types.IndicatorRequest) *corev1.Pod {
	automountToken := false
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      req.Name,
			Namespace: k.namespace,
			Labels: map[string]string{
				constants.LabelIndicator:         "true",
				constants.LabelIndicatorKind:     req.Kind,
				constants.LabelIndicatorResource: req.ResourceName,
				constants.LabelIndicatorPhase:    "Failed",
			},
			Annotations: k.buildPodAnnotations(map[string]string{
				constants.AnnotationIndicatorError: utils.Truncate(strings.ReplaceAll(req.Message, "\n", " "), constants.IndicatorAnnotationMax),
			}),
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			// the indicator only prints an error, so it carries no ServiceAccount token
			AutomountServiceAccountToken: &automountToken,
			NodeSelector:                 k.GetNodeSelectorForJob(0, types.Check), // job 0 + non-sync command = default profile
			Tolerations:                  k.GetTolerationsForJob(0, types.Check),
			Affinity:                     k.BuildAffinityForJob(0, types.Check),
			SecurityContext:              k.config.SecurityContext,
			Containers: []corev1.Container{{
				Name:            "indicator",
				Image:           image,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Env: []corev1.EnvVar{{
					Name:  "OLAKE_ERROR",
					Value: utils.Truncate(req.Message, constants.IndicatorMessageMax),
				}},
				Command:                  []string{"sh", "-c", `printf '%s\n' "$OLAKE_ERROR" | tee /dev/termination-log >&2; exit 1`},
				TerminationMessagePath:   "/dev/termination-log",
				TerminationMessagePolicy: corev1.TerminationMessageReadFile,
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("1m"),
						corev1.ResourceMemory: resource.MustParse("4Mi"),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("10m"),
						corev1.ResourceMemory: resource.MustParse("16Mi"),
					},
				},
			}},
		},
	}

	// Set ServiceAccountName only if configured (non-empty)
	// If empty, Kubernetes will use the namespace's default service account
	if k.config.JobServiceAccount != "" && k.config.JobServiceAccount != "default" {
		pod.Spec.ServiceAccountName = k.config.JobServiceAccount
	}

	return pod
}
