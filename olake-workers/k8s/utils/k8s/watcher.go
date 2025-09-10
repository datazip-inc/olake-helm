package k8s

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	appConfig "github.com/datazip-inc/olake-helm/olake-workers/k8s/config"
	"github.com/datazip-inc/olake-helm/olake-workers/k8s/logger"
)

// ConfigMapWatcher watches for ConfigMap changes and provides thread-safe access to job mapping
type ConfigMapWatcher struct {
	// Kubernetes infrastructure
	clientset       kubernetes.Interface
	informerFactory informers.SharedInformerFactory
	namespace       string
	configMapName   string

	// Thread-safe job mapping storage
	mu         sync.RWMutex
	jobMapping map[int]map[string]string

	ctx    context.Context
	cancel context.CancelFunc
}

// NewConfigMapWatcher creates a new ConfigMap watcher
func NewConfigMapWatcher(clientset kubernetes.Interface, namespace string) *ConfigMapWatcher {
	ctx, cancel := context.WithCancel(context.Background())

	return &ConfigMapWatcher{
		clientset:     clientset,
		namespace:     namespace,
		configMapName: "olake-workers-config",
		jobMapping:    make(map[int]map[string]string),
		ctx:           ctx,
		cancel:        cancel,
	}
}

// Start begins watching the ConfigMap for changes
func (w *ConfigMapWatcher) Start() error {
	logger.Infof("Starting ConfigMap watcher for %s/%s", w.namespace, w.configMapName)

	// Load initial configuration
	if err := w.loadInitialConfig(); err != nil {
		logger.Errorf("Failed to load initial config: %v", err)
		// Continue anyway - watcher will pick up changes
	}

	// Create informer factory scoped to our namespace
	w.informerFactory = informers.NewSharedInformerFactoryWithOptions(
		w.clientset,
		30*time.Second, // Resync period
		informers.WithNamespace(w.namespace),
	)

	// Get ConfigMap informer
	configMapInformer := w.informerFactory.Core().V1().ConfigMaps()

	// Add event handlers with error handling
	_, err := configMapInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if cm, valid := obj.(*corev1.ConfigMap); valid && cm.Name == w.configMapName {
				logger.Debugf("ConfigMap %s added", w.configMapName)
				w.updateJobMapping(cm)
			}
		},
		UpdateFunc: func(oldObj, newObj any) {
			oldCm, oldValid := oldObj.(*corev1.ConfigMap)
			newCm, newValid := newObj.(*corev1.ConfigMap)

			// Skip resync events: client-go informers trigger UpdateFunc every resyncPeriod (30s)
			// even when ConfigMap hasn't changed. Compare ResourceVersion to detect actual updates.
			// ResourceVersion changes only when the object is modified in etcd.
			if oldValid && newValid && oldCm.ResourceVersion == newCm.ResourceVersion {
				return // This is a resync, not a real update
			}

			if newValid && newCm.Name == w.configMapName {
				logger.Debugf("ConfigMap %s updated", w.configMapName)
				w.updateJobMapping(newCm)
			}
		},
		DeleteFunc: func(obj any) {
			if cm, valid := obj.(*corev1.ConfigMap); valid && cm.Name == w.configMapName {
				logger.Warnf("ConfigMap %s deleted - using cached mapping", w.configMapName)
				// Keep existing mapping on delete
			}
		},
	})
	if err != nil {
		return fmt.Errorf("failed to add ConfigMap event handler: %v", err)
	}

	// Start informer factory
	w.informerFactory.Start(w.ctx.Done())

	// Wait for cache to sync
	if !cache.WaitForCacheSync(w.ctx.Done(), configMapInformer.Informer().HasSynced) {
		logger.Errorf("Failed to sync ConfigMap cache")
		return fmt.Errorf("failed to sync ConfigMap cache")
	}

	logger.Infof("ConfigMap watcher started successfully")
	return nil
}

// Stop gracefully shuts down the watcher
func (w *ConfigMapWatcher) Stop() error {
	logger.Infof("Stopping ConfigMap watcher")
	w.cancel()
	logger.Infof("ConfigMap watcher stopped")
	return nil
}

// GetJobMapping returns mapping for specific jobID (thread-safe)
// Uses RWMutex because this function is called concurrently by multiple pod creation goroutines
// while the ConfigMap informer goroutine may be updating w.jobMapping in the background.
// RLock allows multiple concurrent readers while preventing data races with writer updates.
func (w *ConfigMapWatcher) GetJobMapping(jobID int) (map[string]string, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	mapping, exists := w.jobMapping[jobID]
	if !exists {
		return make(map[string]string), false
	}

	// Return a copy to prevent external modification
	result := make(map[string]string)
	for k, v := range mapping {
		result[k] = v
	}
	return result, true
}

// loadInitialConfig loads the initial configuration from the ConfigMap
func (w *ConfigMapWatcher) loadInitialConfig() error {
	cm, err := w.clientset.CoreV1().ConfigMaps(w.namespace).Get(
		w.ctx,
		w.configMapName,
		metav1.GetOptions{},
	)
	if err != nil {
		logger.Errorf("Failed to get initial ConfigMap %s: %v", w.configMapName, err)
		return fmt.Errorf("failed to get initial ConfigMap %s: %v", w.configMapName, err)
	}

	w.updateJobMapping(cm)
	return nil
}

// updateJobMapping updates the job mapping using existing validation logic
func (w *ConfigMapWatcher) updateJobMapping(cm *corev1.ConfigMap) {
	rawMapping, exists := cm.Data["OLAKE_JOB_MAPPING"]
	if !exists {
		logger.Debugf("No OLAKE_JOB_MAPPING found in ConfigMap %s", w.configMapName)
		w.mu.Lock()
		w.jobMapping = make(map[int]map[string]string)
		w.mu.Unlock()
		return
	}

	if rawMapping == "" {
		logger.Debugf("Empty OLAKE_JOB_MAPPING in ConfigMap %s", w.configMapName)
		w.mu.Lock()
		w.jobMapping = make(map[int]map[string]string)
		w.mu.Unlock()
		return
	}

	// Create a temporary config struct to reuse existing LoadJobMapping logic
	tempConfig := &appConfig.Config{
		Kubernetes: appConfig.KubernetesConfig{
			JobMappingRaw: rawMapping,
		},
	}

	// Use existing validation logic from scheduling.go
	newMapping := LoadJobMapping(tempConfig)

	// Update thread-safe storage with exclusive lock
	// Multiple Temporal activity goroutines call GetJobMapping() concurrently (readers)
	// while this ConfigMap informer goroutine updates jobMapping (writer).
	// RWMutex prevents data races and map corruption during concurrent access.
	w.mu.Lock()
	w.jobMapping = newMapping
	w.mu.Unlock()
}
