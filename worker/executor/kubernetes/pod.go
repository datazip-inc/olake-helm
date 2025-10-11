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

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/executor"
	"github.com/datazip-inc/olake-helm/worker/logger"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
)

func (k *KubernetesExecutor) RunPod(ctx context.Context, req *executor.ExecutionRequest, workDir string) (string, error) {
	imageName := utils.GetDockerImageName(req.ConnectorType, req.Version)

	podSpec := k.CreatePodSpec(req, workDir, imageName)

	logger.Infof("creating Pod %s with image %s", podSpec.Name, imageName)

	if _, err := k.createPod(ctx, podSpec); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return "", fmt.Errorf("failed to create pod: %v", err)
		}
		logger.Infof("pod already exists, resuming polling for Pod: %s", podSpec.Name)
	}
	logger.Debugf("successfully created Pod %s", podSpec.Name)

	if req.Command != types.Sync {
		defer func() {
			if err := k.cleanupPod(ctx, podSpec.Name); err != nil {
				logger.Errorf("failed to cleanup pod %s for %s operation (workflow: %s): %v",
					podSpec.Name, req.Command, req.WorkflowID, err)
			}
		}()
	}

	if err := k.waitForPodCompletion(ctx, podSpec.Name, req.Timeout, req.HeartbeatFunc); err != nil {
		return "", err
	}

	logs, err := k.getPodLogs(ctx, podSpec.Name)
	if err != nil {
		return "", fmt.Errorf("failed to get pod logs: %v", err)
	}

	return logs, nil
}

func (k *KubernetesExecutor) waitForPodCompletion(ctx context.Context, podName string, timeout time.Duration, heartbeatFunc func(context.Context, ...interface{})) error {
	logger.Debugf("waiting for Pod %s to complete (timeout: %v)", podName, timeout)
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		// Record heartbeat to enable cancellation detection if heartbeat function is provided
		if heartbeatFunc != nil {
			heartbeatFunc(ctx, fmt.Sprintf("Waiting for pod %s (status check)", podName))
		}

		pod, err := k.client.CoreV1().Pods(k.namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get pod status: %v", err)
		}

		// Check if pod completed successfully
		if pod.Status.Phase == corev1.PodSucceeded {
			logger.Infof("pod %s completed successfully", podName)
			return nil
		}

		// Check if pod failed
		if pod.Status.Phase == corev1.PodFailed {
			// Check if this is a retryable infrastructure failure
			retryableReasons := []string{"ImagePullBackOff", "ErrImagePull"}
			if slices.Contains(retryableReasons, pod.Status.Reason) {
				logger.Warnf("pod %s is not running: %s, message: %s - continuing to poll", podName, pod.Status.Reason, pod.Status.Message)
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
			return fmt.Errorf("%w: pod %s failed (%s)", constants.ErrExecutionFailed, podName, containerInfo)
		}

		// Wait before checking again, with responsive cancellation
		select {
		case <-time.After(5 * time.Second):
			// Continue to next iteration
		case <-ctx.Done():
			logger.Warnf("context cancelled while waiting for pod %s", podName)
			return ctx.Err()
		}
	}

	return fmt.Errorf("pod timed out after %v", timeout)
}

func (k *KubernetesExecutor) getPodLogs(ctx context.Context, podName string) (string, error) {
	req := k.client.CoreV1().Pods(k.namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container: "connector",
	})

	logs, err := req.Stream(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get pod logs: %v", err)
	}
	defer func() {
		if err := logs.Close(); err != nil {
			logger.Warnf("failed to close logs: %v", err)
		}
	}()

	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, logs)
	if err != nil {
		return "", fmt.Errorf("failed to read pod logs: %v", err)
	}

	return buf.String(), nil
}

func (k *KubernetesExecutor) cleanupPod(ctx context.Context, podName string) error {
	logger.Debugf("cleaning up pod %s in namespace %s", podName, k.namespace)

	// Delete the pod only
	err := k.client.CoreV1().Pods(k.namespace).Delete(ctx, podName, metav1.DeleteOptions{})
	if err != nil {
		// Treat "not found" as success - cleanup is idempotent
		if apierrors.IsNotFound(err) {
			logger.Infof("pod %s already deleted in namespace %s - cleanup complete", podName, k.namespace)
			return nil
		}
		return fmt.Errorf("failed to delete pod %s in namespace %s: %v", podName, k.namespace, err)
	}

	logger.Debugf("successfully cleaned up pod %s in namespace %s", podName, k.namespace)
	return nil
}

func (k *KubernetesExecutor) CreatePodSpec(req *executor.ExecutionRequest, workDir, imageName string) *corev1.Pod {
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
			Tolerations:   []corev1.Toleration{}, // No tolerations supported yet
			// Affinity:      k.buildAffinityForJob(spec.JobID, spec.Operation),
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

	return pod
}

func (k *KubernetesExecutor) createPod(ctx context.Context, podSpec *corev1.Pod) (*corev1.Pod, error) {
	result, err := k.client.CoreV1().Pods(k.namespace).Create(ctx, podSpec, metav1.CreateOptions{})
	if err != nil {
		// IMPORTANT: This is the resumption point for activity retries after worker death.
		// When a worker dies mid-activity, Temporal retries the activity on a new worker.
		// Since pod names are deterministic (derived from WorkflowID), the retry attempt
		// will try to create the same pod that the previous worker already created.
		// Kubernetes returns AlreadyExists error, which we handle in ExecutePodActivity
		// by resuming polling on the existing pod instead of failing the activity.
		// This error is expected during resumption and will be caught upstream.
		return nil, err
	}

	logger.Debugf("successfully created pod %s", podSpec.Name)
	return result, nil
}
