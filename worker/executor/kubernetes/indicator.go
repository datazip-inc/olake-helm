// GitOps: failure-indicator pods for ConfigMap/Secret validation errors (spawn/delete via IndicatorWorkflow).
package kubernetes

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
)

func (k *KubernetesExecutor) Indicator(ctx context.Context, req *types.IndicatorRequest) error {
	ns := k.indicatorNamespace(req)
	switch req.Action {
	case types.IndicatorActionDelete:
		return k.deleteIndicatorPod(ctx, ns, req.Name)
	case types.IndicatorActionSpawn:
		return k.spawnIndicatorPod(ctx, ns, req)
	default:
		return fmt.Errorf("unknown indicator action %q", req.Action)
	}
}

func (k *KubernetesExecutor) indicatorNamespace(req *types.IndicatorRequest) string {
	if req.Namespace != "" {
		return req.Namespace
	}
	return k.namespace
}

func (k *KubernetesExecutor) deleteIndicatorPod(ctx context.Context, ns, name string) error {
	log := logger.Log(ctx)
	err := k.client.CoreV1().Pods(ns).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		log.Error("delete indicator pod failed", "podName", name, "namespace", ns, "error", err)
		return err
	}
	return nil
}

func (k *KubernetesExecutor) spawnIndicatorPod(ctx context.Context, ns string, req *types.IndicatorRequest) error {
	log := logger.Log(ctx)
	name := req.Name
	zero := int64(0)
	_ = k.client.CoreV1().Pods(ns).Delete(ctx, name, metav1.DeleteOptions{GracePeriodSeconds: &zero})

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels: map[string]string{
				constants.LabelIndicator:         "true",
				constants.LabelIndicatorKind:     req.Kind,
				constants.LabelIndicatorResource: req.ResourceName,
				constants.LabelIndicatorPhase:    "Failed",
			},
			Annotations: map[string]string{
				constants.AnnotationIndicatorError: utils.Truncate(strings.ReplaceAll(req.Message, "\n", " "), constants.IndicatorAnnotationMax),
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:            "indicator",
				Image:           utils.GetRegistryImage(constants.IndicatorImage),
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

	_, err := k.client.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		log.Error("spawn indicator pod failed", "podName", name, "namespace", ns, "error", err)
		return fmt.Errorf("spawn indicator pod: %w", err)
	}
	return nil
}
