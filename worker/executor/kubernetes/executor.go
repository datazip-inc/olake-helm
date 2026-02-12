package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"github.com/spf13/viper"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	DefaultQPS   = 50  // DefaultQPS defines the maximum queries per second allowed to the Kubernetes API server
	DefaultBurst = 100 // DefaultBurst defines the maximum number of requests allowed in a burst to the Kubernetes API server
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
	WorkerIdentity    string
	ImagePullSecrets  []string
}

func NewKubernetesExecutor(ctx context.Context) (*KubernetesExecutor, error) {
	// Use in-cluster configuration - this reads the service account token and CA cert
	// that Kubernetes automatically mounts into every pod at /var/run/secrets/kubernetes.io/serviceaccount/
	clusterConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get in-cluster config: %s", err)
	}

	// Configure QPS and Burst limits to avoid client-side throttling
	clusterConfig.QPS = DefaultQPS
	clusterConfig.Burst = DefaultBurst

	// Create the Kubernetes clientset using the in-cluster config
	// This clientset provides access to all Kubernetes API operations (pods, services, etc.)
	clientset, err := kubernetes.NewForConfig(clusterConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes client: %s", err)
	}

	// Get config from environment
	namespace := viper.GetString(constants.EnvNamespace)
	pvcName := viper.GetString(constants.EnvStoragePVCName)
	serviceAccount := viper.GetString(constants.EnvJobServiceAccountName)
	jobServiceAccount := viper.GetString(constants.EnvJobServiceAccountName)
	secretKey := viper.GetString(constants.EnvSecretKey)
	basePath := utils.GetConfigDir()

	// Parse image pull secrets from environment (comma-separated or json list)
	var imagePullSecrets []string
	if secretsStr := viper.GetString(constants.EnvImagePullSecrets); secretsStr != "" {
		// Try parsing as JSON first
		if err := json.Unmarshal([]byte(secretsStr), &imagePullSecrets); err != nil {
			// Fallback to comma-separated for backward compatibility or simple usage
			for _, s := range strings.Split(secretsStr, ",") {
				if trimmed := strings.TrimSpace(s); trimmed != "" {
					imagePullSecrets = append(imagePullSecrets, trimmed)
				}
			}
		}
	}

	// Set worker identity
	podName := viper.GetString(constants.EnvPodName)
	workerIdenttity := fmt.Sprintf("olake.io/olake-workers/%s", podName)

	watcher := NewConfigMapWatcher(ctx, clientset, namespace)
	if err := watcher.Start(); err != nil {
		logger.Errorf("failed to start config map watcher: %s", err)
	}

	return &KubernetesExecutor{
		client:        clientset,
		namespace:     namespace,
		configWatcher: watcher,
		config: &KubernetesConfig{
			Namespace:         namespace,
			PVCName:           pvcName,
			ServiceAccount:    serviceAccount,
			JobServiceAccount: jobServiceAccount,
			SecretKey:         secretKey,
			BasePath:          basePath,
			WorkerIdentity:    workerIdenttity,
			ImagePullSecrets:  imagePullSecrets,
		},
	}, nil
}

func (k *KubernetesExecutor) Execute(ctx context.Context, req *types.ExecutionRequest, workdir string) (string, error) {
	log := logger.Log(ctx)
	imageName := utils.GetDockerImageName(req.ConnectorType, req.Version)
	podSpec := k.CreatePodSpec(req, workdir, imageName)
	log.Info("creating pod", "podName", podSpec.Name, "image", imageName)

	if _, err := k.createPod(ctx, podSpec); err != nil {
		log.Error("failed to create pod", "podName", podSpec.Name, "error", err)
		return "", err
	}

	if !slices.Contains(constants.AsyncCommands, req.Command) {
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second*constants.ContainerCleanupTimeout)
			defer cancel()

			if err := k.cleanupPod(cleanupCtx, podSpec.Name); err != nil {
				log.Error("failed to cleanup pod", "podName", podSpec.Name, "command", req.Command, "workflowID", req.WorkflowID, "error", err)
			}
		}()
	}

	if err := k.waitForPodCompletion(ctx, podSpec.Name, req.Timeout, req.HeartbeatFunc); err != nil {
		log.Error("pod failed to complete", "podName", podSpec.Name, "error", err)
		return "", err
	}

	logs, err := k.getPodLogs(ctx, podSpec.Name)
	if err != nil {
		log.Error("failed to get pod logs", "podName", podSpec.Name, "error", err)
		return "", fmt.Errorf("failed to get pod logs: %s", err)
	}

	return logs, nil
}

func (k *KubernetesExecutor) Cleanup(ctx context.Context, req *types.ExecutionRequest) error {
	log := logger.Log(ctx)
	podName := k.sanitizeName(req.WorkflowID)
	log.Info("cleaning up pod", "podName", podName, "workflowID", req.WorkflowID)

	if err := k.cleanupPod(ctx, podName); err != nil {
		log.Error("failed to cleanup pod", "podName", podName, "error", err)
		return fmt.Errorf("failed to cleanup pod: %s", err)
	}

	log.Info("pod cleanup completed", "podName", podName)
	return nil
}

func (k *KubernetesExecutor) Close() error {
	k.configWatcher.cancel()
	return nil
}
