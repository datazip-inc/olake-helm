package kubernetes

import (
	"context"
	"crypto/sha256"
	"fmt"
	"path/filepath"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/executor"
	"github.com/datazip-inc/olake-helm/worker/logger"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils"
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
		return nil, fmt.Errorf("failed to get in-cluster config: %v", err)
	}

	// Create the Kubernetes clientset using the in-cluster config
	// This clientset provides access to all Kubernetes API operations (pods, services, etc.)
	clientset, err := kubernetes.NewForConfig(clusterConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes client: %v", err)
	}

	// Get config from environment
	namespace := viper.GetString(constants.EnvNamespace)
	pvcName := viper.GetString(constants.EnvStoragePVCName)
	serviceAccount := viper.GetString(constants.EnvJobServiceAccountName)
	jobServiceAccount := viper.GetString(constants.EnvJobServiceAccountName)
	secretKey := viper.GetString(constants.EnvSecretKey)
	basePath := viper.GetString(constants.EnvContainerPersistentDir)

	// Set worker identity
	podName := viper.GetString(constants.EnvPodName)
	workerIdenttity := fmt.Sprintf("olake.io/olake-workers/%s", podName)

	watcher := NewConfigMapWatcher(clientset, namespace)
	if err := watcher.Start(); err != nil {
		logger.Errorf("Failed to start config map watcher: %v", err)
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

func (k *KubernetesExecutor) Execute(ctx context.Context, req *executor.ExecutionRequest) (map[string]interface{}, error) {
	subDir := utils.Ternary(req.Command == types.Sync, fmt.Sprintf("%x", sha256.Sum256([]byte(req.WorkflowID))), req.WorkflowID).(string)
	workDir, err := utils.SetupWorkDirectory(k.config.BasePath, subDir)
	if err != nil {
		return nil, err
	}

	if err := utils.WriteConfigFiles(workDir, req.Configs); err != nil {
		return nil, err
	}
	// Question: Telemetry requires streams.json, so cleaning up fails telemetry. Do we need cleanup?
	// defer utils.CleanupConfigFiles(workDir, req.Configs)

	out, err := k.runPod(ctx, req, workDir)
	if err != nil {
		return nil, err
	}

	if req.OutputFile != "" {
		fileContent, err := utils.ReadFile(filepath.Join(workDir, req.OutputFile))
		if err != nil {
			return nil, fmt.Errorf("failed to read output file: %s", err)
		}
		return map[string]interface{}{"response": fileContent}, nil
	}

	return map[string]interface{}{"response": out}, nil
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
