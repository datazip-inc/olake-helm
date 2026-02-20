package kubernetes

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
)

func (k *KubernetesExecutor) waitForPodCompletion(ctx context.Context, podName string, timeout time.Duration, heartbeatFunc func(context.Context, ...interface{})) error {
	log := logger.Log(ctx)
	log.Debug("waiting for pod to complete", "podName", podName, "timeout", timeout)
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		// Record heartbeat to enable cancellation detection if heartbeat function is provided
		if heartbeatFunc != nil {
			heartbeatFunc(ctx, fmt.Sprintf("Waiting for pod %s (status check)", podName))
		}

		pod, err := k.client.CoreV1().Pods(k.namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			log.Error("failed to get pod status", "podName", podName, "error", err)
			return fmt.Errorf("failed to get pod status: %s", err)
		}

		// Check if pod completed successfully
		if pod.Status.Phase == corev1.PodSucceeded {
			log.Info("pod completed successfully", "podName", podName)
			return nil
		}

		// Check if pod failed
		if pod.Status.Phase == corev1.PodFailed {
			// Check if this is a retryable infrastructure failure
			retryableReasons := []string{"ImagePullBackOff", "ErrImagePull"}
			if slices.Contains(retryableReasons, pod.Status.Reason) {
				log.Warn("pod not running, continuing to poll", "podName", podName, "reason", pod.Status.Reason, "message", pod.Status.Message)
				continue
			}

			// Common exit codes:
			// - Exit 0: Success
			// - Exit 1: General application error
			// - Exit 2: Misuse of shell command or manual termination
			// - Exit 137: SIGKILL (OOMKilled or manual kill)
			// - Exit 143: SIGTERM (graceful termination)
			var containerInfo string
			if len(pod.Status.ContainerStatuses) > 0 {
				status := pod.Status.ContainerStatuses[0]
				if status.State.Terminated != nil {
					term := status.State.Terminated
					containerInfo = fmt.Sprintf("exit code: %d, reason: %s", term.ExitCode, term.Reason)
				}
			}
			log.Error("pod failed", "podName", podName, "containerInfo", containerInfo)
			return fmt.Errorf("%w: pod %s failed (%s)", constants.ErrExecutionFailed, podName, containerInfo)
		}

		// Wait before checking again, with responsive cancellation
		select {
		case <-time.After(5 * time.Second):
			// Continue to next iteration
		case <-ctx.Done():
			log.Warn("context cancelled while waiting for pod", "podName", podName)
			return ctx.Err()
		}
	}

	log.Error("pod timed out", "podName", podName, "timeout", timeout)
	return fmt.Errorf("pod timed out after %v", timeout)
}

func (k *KubernetesExecutor) getPodLogs(ctx context.Context, podName string) (string, error) {
	log := logger.Log(ctx)
	req := k.client.CoreV1().Pods(k.namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container: "connector",
	})

	logs, err := req.Stream(ctx)
	if err != nil {
		log.Error("failed to stream pod logs", "podName", podName, "error", err)
		return "", fmt.Errorf("failed to get pod logs: %s", err)
	}
	defer func() {
		if err := logs.Close(); err != nil {
			log.Warn("failed to close log stream", "podName", podName, "error", err)
		}
	}()

	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, logs)
	if err != nil {
		log.Error("failed to read pod logs", "podName", podName, "error", err)
		return "", fmt.Errorf("failed to read pod logs: %s", err)
	}

	return buf.String(), nil
}

func (k *KubernetesExecutor) cleanupPod(ctx context.Context, podName string) error {
	log := logger.Log(ctx)
	log.Debug("cleaning up pod", "podName", podName, "namespace", k.namespace)

	// Delete the pod only
	err := k.client.CoreV1().Pods(k.namespace).Delete(ctx, podName, metav1.DeleteOptions{})
	if err != nil {
		// Treat "not found" as success - cleanup is idempotent
		if apierrors.IsNotFound(err) {
			log.Info("pod already deleted", "podName", podName, "namespace", k.namespace)
			return nil
		}
		log.Error("failed to delete pod", "podName", podName, "namespace", k.namespace, "error", err)
		return fmt.Errorf("failed to delete pod %s in namespace %s: %s", podName, k.namespace, err)
	}

	log.Debug("successfully cleaned up pod", "podName", podName, "namespace", k.namespace)
	return nil
}

func (k *KubernetesExecutor) CreatePodSpec(req *types.ExecutionRequest, workDir, imageName string) *corev1.Pod {
	subDir := filepath.Base(workDir)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      k.sanitizeName(req.WorkflowID), // Sanitized name safe for Kubernetes
			Namespace: k.namespace,                    // Target namespace for pod creation

			// Labels are used for querying, filtering, and organizing pods
			Labels: map[string]string{
				// Standard Kubernetes labels for ecosystem compatibility
				"app.kubernetes.io/name":       "olake",                                                      // Application name
				"app.kubernetes.io/component":  fmt.Sprintf("%s-%s", req.ConnectorType, string(req.Command)), // Component identifier
				"app.kubernetes.io/managed-by": "olake-workers",                                              // Management tool

				// Custom Olake labels for internal operations and queries
				"olake.io/operation-type": string(req.Command),            // sync, discover, or check
				"olake.io/connector":      req.ConnectorType,              // mysql, postgres, etc.
				"olake.io/job-id":         strconv.Itoa(req.JobID),        // Database job reference
				"olake.io/workflow-id":    k.sanitizeName(req.WorkflowID), // Sanitized workflow ID
			},

			// Annotations store metadata that doesn't affect pod selection/scheduling
			Annotations: map[string]string{
				"olake.io/created-by-pod": k.config.WorkerIdentity,         // Which worker pod created this
				"olake.io/created-at":     time.Now().Format(time.RFC3339), // Creation timestamp
				"olake.io/workflow-id":    req.WorkflowID,                  // Original unsanitized workflow ID
				"olake.io/operation-type": string(req.Command),             // Operation type for reference
				"olake.io/connector-type": req.ConnectorType,               // Connector type for reference
				"olake.io/job-id":         fmt.Sprintf("%d", req.JobID),    // Job ID for reference
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			NodeSelector:  k.GetNodeSelectorForJob(req.JobID, req.Command),
			Tolerations:   k.GetTolerationsForJob(req.JobID, req.Command),
			Affinity:      k.BuildAffinityForJob(req.JobID, req.Command),
			Containers: []corev1.Container{
				{
					Name:    "connector",
					Image:   imageName,
					Command: []string{},
					Args:    req.Args,
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "job-storage",
							MountPath: "/mnt/config",
							SubPath:   subDir,
						},
					},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceMemory: k.parseQuantity("256Mi"),
							corev1.ResourceCPU:    k.parseQuantity("100m"),
						},
						// No limits for flexibility
					},
					Env: []corev1.EnvVar{
						{
							Name:  "OLAKE_WORKFLOW_ID",
							Value: req.WorkflowID,
						},
						{
							Name:  "OLAKE_SECRET_KEY",
							Value: k.config.SecretKey,
						},
					},
					EnvFrom: []corev1.EnvFromSource{
						{
							ConfigMapRef: &corev1.ConfigMapEnvSource{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: "olake-global-env",
								},
								Optional: ptr.To(true),
							},
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "job-storage",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: k.config.PVCName,
						},
					},
				},
			},
		},
	}

	// Set ServiceAccountName only if configured (non-empty)
	// If empty, Kubernetes will use the namespace's default service account
	if k.config.JobServiceAccount != "" && k.config.JobServiceAccount != "default" {
		pod.Spec.ServiceAccountName = k.config.JobServiceAccount
	}

	// Add liveness probe for long-running sync operations
	if slices.Contains(constants.AsyncCommands, req.Command) {
		pod.Spec.Containers[0].LivenessProbe = &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				Exec: &corev1.ExecAction{
					Command: []string{
						"/bin/sh",
						"-c",
						"echo ok > /mnt/config/.healthcheck",
					},
				},
			},
			InitialDelaySeconds: 10,
			PeriodSeconds:       30,
			TimeoutSeconds:      5,
			FailureThreshold:    3,
			SuccessThreshold:    1,
		}
	}

	return pod
}

func (k *KubernetesExecutor) createPod(ctx context.Context, podSpec *corev1.Pod) (*corev1.Pod, error) {
	log := logger.Log(ctx)
	result, err := k.client.CoreV1().Pods(k.namespace).Create(ctx, podSpec, metav1.CreateOptions{})
	if err != nil {
		if !apierrors.IsAlreadyExists(err) {
			log.Error("failed to create pod", "podName", podSpec.Name, "error", err)
			return nil, fmt.Errorf("failed to create pod: %s", err)
		}

		log.Info("pod already exists, resuming polling", "podName", podSpec.Name)

		// Fetch the existing pod
		existing, getErr := k.client.CoreV1().Pods(k.namespace).Get(ctx, podSpec.Name, metav1.GetOptions{})
		if getErr != nil {
			log.Error("pod exists but failed to fetch", "podName", podSpec.Name, "error", getErr)
			return nil, fmt.Errorf("pod exists but failed to fetch: %s", getErr)
		}
		return existing, nil
	}

	log.Info("successfully created pod", "podName", podSpec.Name)
	return result, nil
}
