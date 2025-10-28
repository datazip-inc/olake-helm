package kubernetes

import (
	"context"
	"fmt"
	"time"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
	"github.com/spf13/viper"
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
	WorkerIdentity    string
}

func NewKubernetesExecutor() (*KubernetesExecutor, error) {
	// Use in-cluster configuration - this reads the service account token and CA cert
	// that Kubernetes automatically mounts into every pod at /var/run/secrets/kubernetes.io/serviceaccount/
	clusterConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get in-cluster config: %s", err)
	}

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

	// Set worker identity
	podName := viper.GetString(constants.EnvPodName)
	workerIdenttity := fmt.Sprintf("olake.io/olake-workers/%s", podName)

	watcher := NewConfigMapWatcher(clientset, namespace)
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
		},
	}, nil
}

func (k *KubernetesExecutor) Execute(ctx context.Context, req *types.ExecutionRequest, workdir string) (string, error) {
	imageName := utils.GetDockerImageName(req.ConnectorType, req.Version)
	podSpec := k.CreatePodSpec(req, workdir, imageName)
	logger.Infof("creating Pod %s with image %s", podSpec.Name, imageName)

	if _, err := k.createPod(ctx, podSpec); err != nil {
		return "", err
	}

	if req.Command != types.Sync {
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second*constants.ContainerCleanupTimeout)
			defer cancel()

			if err := k.cleanupPod(cleanupCtx, podSpec.Name); err != nil {
				logger.Errorf("failed to cleanup pod %s for %s operation (workflow: %s): %s",
					podSpec.Name, req.Command, req.WorkflowID, err)
			}
		}()
	}

	if err := k.waitForPodCompletion(ctx, podSpec.Name, req.Timeout, req.HeartbeatFunc); err != nil {
		return "", err
	}

	logs, err := k.getPodLogs(ctx, podSpec.Name)
	if err != nil {
		return "", fmt.Errorf("failed to get pod logs: %s", err)
	}

	return logs, nil
}

func (k *KubernetesExecutor) Cleanup(ctx context.Context, req *types.ExecutionRequest) error {
	podName := k.sanitizeName(req.WorkflowID)
	if err := k.cleanupPod(ctx, podName); err != nil {
		return fmt.Errorf("failed to cleanup pod: %s", err)
	}
	return nil
}

func (k *KubernetesExecutor) Close() error {
	k.configWatcher.cancel()
	return nil
}
