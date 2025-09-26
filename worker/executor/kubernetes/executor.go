package kubernetes

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/executor"
	"github.com/datazip-inc/olake-helm/worker/logger"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type KubernetesExecutor struct {
	client        kubernetes.Interface
	namespace     string
	config        *KubernetesConfig
	configWatcher *ConfigMapWatcher
}

type KubernetesConfig struct {
	Namespace         string
	PVCName           string
	ServiceAccount    string
	JobServiceAccount string
	SecretKey         string
	BasePath          string
}

func NewKubernetesExecutor() (*KubernetesExecutor, error) {
	// Use in-cluster configuration - this reads the service account token and CA cert
	// that Kubernetes automatically mounts into every pod at /var/run/secrets/kubernetes.io/serviceaccount/
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get in-cluster config: %v", err)
	}

	// Create the Kubernetes clientset using the in-cluster config
	// This clientset provides access to all Kubernetes API operations (pods, services, etc.)
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes client: %v", err)
	}

	// Get config from environment
	namespace := utils.GetEnv("WORKER_NAMESPACE", "default")
	pvcName := utils.GetEnv("OLAKE_STORAGE_PVC_NAME", "olake-storage-pvc")
	serviceAccount := utils.GetEnv("SERVICE_ACCOUNT", "")
	jobServiceAccount := utils.GetEnv("JOB_SERVICE_ACCOUNT_NAME", "")
	secretKey := utils.GetEnv("OLAKE_SECRET_KEY", "")
	basePath := "/data/olake-jobs"

	watcher := NewConfigMapWatcher(clientset, namespace)
	if err := watcher.Start(); err != nil {
		logger.Errorf("Failed to start config map watcher: %v", err)
	}

	return &KubernetesExecutor{
		client:    clientset,
		namespace: namespace,
		config: &KubernetesConfig{
			Namespace:         namespace,
			PVCName:           pvcName,
			ServiceAccount:    serviceAccount,
			JobServiceAccount: jobServiceAccount,
			SecretKey:         secretKey,
			BasePath:          basePath,
		},
		configWatcher: watcher,
	}, nil
}

func (k *KubernetesExecutor) Execute(ctx context.Context, req *executor.ExecutionRequest) (map[string]interface{}, error) {
	workDir, err := k.setupWorkDir(req)
	if err != nil {
		return nil, err
	}

	if len(req.Configs) > 0 {
		if err := k.writeConfigFiles(workDir, req.Configs); err != nil {
			return nil, err
		}
		defer k.cleanupConfigFiles(workDir, req.Configs)
	}

	rawLogs, err := k.runPod(ctx, req, workDir)
	if err != nil {
		return nil, err
	}

	if req.OutputFile != "" {
		response, err := k.readOutputFile(filepath.Join(workDir, req.OutputFile))
		if err != nil {
			return nil, fmt.Errorf("failed to read output file: %s", err)
		}
		return map[string]interface{}{"response": response}, nil
	}

	return map[string]interface{}{"response": rawLogs}, nil
}

func (k *KubernetesExecutor) setupWorkDir(req *executor.ExecutionRequest) (string, error) {
	var subDir string
	if req.Command == types.Sync {
		subDir = fmt.Sprintf("%x", sha256.Sum256([]byte(req.WorkflowID)))
	} else {
		subDir = req.WorkflowID
	}

	workDir := filepath.Join(k.config.BasePath, subDir)
	if err := os.MkdirAll(workDir, constants.DefaultDirPermissions); err != nil {
		return "", fmt.Errorf("failed to create work directory: %v", err)
	}

	return workDir, nil
}

func (k *KubernetesExecutor) writeConfigFiles(workDir string, configs []types.JobConfig) error {
	for _, config := range configs {
		filePath := filepath.Join(workDir, config.Name)
		if err := os.WriteFile(filePath, []byte(config.Data), constants.DefaultFilePermissions); err != nil {
			return fmt.Errorf("failed to write %s: %v", config.Name, err)
		}
	}
	return nil
}

func (k *KubernetesExecutor) cleanupConfigFiles(workDir string, configs []types.JobConfig) {
	for _, config := range configs {
		filePath := filepath.Join(workDir, config.Name)
		_ = os.Remove(filePath) // Ignore error for cleanup
	}
}

func (k *KubernetesExecutor) runPod(ctx context.Context, req *executor.ExecutionRequest, workDir string) (string, error) {
	imageName, err := k.getDockerImageName(req.ConnectorType, req.Version)
	if err != nil {
		return "", fmt.Errorf("failed to get docker image name: %v", err)
	}

	podSpec := k.createPodSpec(req, workDir, imageName)

	pod, err := k.client.CoreV1().Pods(k.namespace).Create(ctx, podSpec, metav1.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to create pod: %v", err)
	}
	defer func() {
		if err := k.cleanupPod(ctx, pod.Name); err != nil {
			logger.Errorf("Failed to cleanup pod: %v", err)
		}
	}()

	logger.Infof("Created pod: %s", pod.Name)

	if err := k.waitForPodCompletion(ctx, pod.Name, 10*time.Minute); err != nil {
		return "", err
	}

	logs, err := k.getPodLogs(ctx, pod.Name)
	if err != nil {
		return "", fmt.Errorf("failed to get pod logs: %v", err)
	}

	return logs, nil
}

func (k *KubernetesExecutor) createPodSpec(req *executor.ExecutionRequest, workDir, imageName string) *corev1.Pod {
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
				"olake.io/created-by-pod": "olake-worker",                  // Which worker pod created this
				"olake.io/created-at":     time.Now().Format(time.RFC3339), // Creation timestamp
				"olake.io/workflow-id":    req.WorkflowID,                  // Original unsanitized workflow ID
				"olake.io/operation-type": string(req.Command),             // Operation type for reference
				"olake.io/connector-type": req.ConnectorType,               // Connector type for reference
				"olake.io/job-id":         fmt.Sprintf("%d", req.JobID),    // Job ID for reference
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			NodeSelector:  k.getNodeSelectorForJob(req.JobID, req.Command),
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

// getNodeSelectorForJob returns node selector configuration for the given jobID
// Returns empty map if no mapping is found (graceful fallback)
// Only applies node mapping for sync operations
func (k *KubernetesExecutor) getNodeSelectorForJob(jobID int, operation types.Command) map[string]string {
	// Only apply node mapping for sync operations
	if operation != types.Sync {
		return make(map[string]string)
	}

	// Use live mapping from ConfigMapWatcher
	if mapping, exists := k.configWatcher.GetJobMapping(jobID); exists {
		logger.Infof("Found node mapping for JobID %d: %v", jobID, mapping)
		return mapping
	}
	return make(map[string]string)
}

func (k *KubernetesExecutor) waitForPodCompletion(ctx context.Context, podName string, timeout time.Duration) error {
	logger.Debugf("Waiting for Pod %s to complete (timeout: %v)", podName, timeout)
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		pod, err := k.client.CoreV1().Pods(k.namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get pod status: %v", err)
		}

		// Check if pod completed successfully
		if pod.Status.Phase == corev1.PodSucceeded {
			logger.Infof("Pod %s completed successfully", podName)
			return nil
		}

		// Check if pod failed
		if pod.Status.Phase == corev1.PodFailed {
			logger.Errorf("Pod %s failed", podName)
			return fmt.Errorf("pod %s failed with status: %s", podName, pod.Status.Phase)
		}

		// Wait before checking again
		time.Sleep(5 * time.Second)
	}

	logger.Errorf("Pod %s timed out after %v", podName, timeout)
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
			logger.Warnf("Failed to close logs: %v", err)
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
	logger.Debugf("Cleaning up Pod %s in namespace %s", podName, k.namespace)

	// Delete the pod only
	err := k.client.CoreV1().Pods(k.namespace).Delete(ctx, podName, metav1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("failed to delete pod %s in namespace %s: %v", podName, k.namespace, err)
	}

	logger.Debugf("Successfully cleaned up Pod %s in namespace %s", podName, k.namespace)
	return nil
}

func (k *KubernetesExecutor) getDockerImageName(connectorType, version string) (string, error) {
	// Strict validation: version is required (no 'latest' fallback) - same as old worker
	if version == "" {
		return "", fmt.Errorf("version cannot be empty")
	}

	// Construct the full image name using the olakego registry convention
	// Examples: olakego/source-mysql:v0.1.7, olakego/source-postgres:v1.2.3
	return fmt.Sprintf("olakego/source-%s:%s", connectorType, version), nil
}

func (k *KubernetesExecutor) sanitizeName(name string) string {
	name = strings.ToLower(name)
	name = strings.ReplaceAll(name, "_", "-")
	name = strings.ReplaceAll(name, ".", "-")
	name = strings.ReplaceAll(name, ":", "-")
	name = strings.Trim(name, "-")

	if len(name) > 63 {
		name = name[:63]
		name = strings.TrimSuffix(name, "-")
	}

	return name
}

func (k *KubernetesExecutor) parseQuantity(s string) resource.Quantity {
	q, _ := resource.ParseQuantity(s)
	return q
}

func (k *KubernetesExecutor) readOutputFile(filePath string) (string, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (k *KubernetesExecutor) Close() error {
	k.configWatcher.cancel()
	return nil
}

func init() {
	executor.RegisteredExecutors[executor.Kubernetes] = func() (executor.Executor, error) {
		return NewKubernetesExecutor()
	}
}
