package kubernetes

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/datazip-inc/olake-helm/worker/utils/logger"
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
	jobMapping map[int]JobSchedulingConfig

	ctx    context.Context
	cancel context.CancelFunc
}

func NewConfigMapWatcher(clientset kubernetes.Interface, namespace string) *ConfigMapWatcher {
	ctx, cancel := context.WithCancel(context.Background())
	return &ConfigMapWatcher{
		clientset:     clientset,
		namespace:     namespace,
		configMapName: "olake-workers-config",
		jobMapping:    make(map[int]JobSchedulingConfig),
		ctx:           ctx,
		cancel:        cancel,
	}
}

func (w *ConfigMapWatcher) Start() error {
	logger.Infof("starting ConfigMap watcher for %s/%s", w.namespace, w.configMapName)

	// Create informer factory scoped to our namespace
	w.informerFactory = informers.NewSharedInformerFactoryWithOptions(
		w.clientset,
		30*time.Second, // Resync period
		informers.WithNamespace(w.namespace),
	)

	configMapInformer := w.informerFactory.Core().V1().ConfigMaps()

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
				logger.Warnf("ConfigMap %s deleted - keeping cached mapping", w.configMapName)
				// keep existing mapping on delete
			}
		},
	})
	if err != nil {
		return fmt.Errorf("failed to add ConfigMap handler: %s", err)
	}

	// Start informer factory and wait for cache sync
	w.informerFactory.Start(w.ctx.Done())
	if !cache.WaitForCacheSync(w.ctx.Done(), configMapInformer.Informer().HasSynced) {
		return fmt.Errorf("failed to sync ConfigMap cache")
	}

	logger.Infof("ConfigMap watcher started")
	return nil
}

func (w *ConfigMapWatcher) Stop() {
	logger.Infof("stopping ConfigMap watcher")
	w.cancel()
}

// GetJobMapping returns mapping for specific jobID
func (w *ConfigMapWatcher) GetJobMapping(jobID int) (JobSchedulingConfig, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	config, exists := w.jobMapping[jobID]
	return config, exists
}

func (w *ConfigMapWatcher) updateJobMapping(cm *corev1.ConfigMap) {
	// TODO: Remove legacy OLAKE_JOB_MAPPING loading logic (Deprecated).
	// This block supports the legacy `jobMapping` configuration which only supports NodeSelectors.
	// It has been superseded by `jobProfiles` and should be removed in a future major release to clean up the codebase.
	// 1. Load Legacy Mapping
	var legacyMapping map[int]JobSchedulingConfig
	if mapping, exists := cm.Data["OLAKE_JOB_MAPPING"]; exists && mapping != "" {
		legacyMapping = LoadJobMapping(mapping)
	} else {
		legacyMapping = make(map[int]JobSchedulingConfig)
	}

	// 2. Load New Profiles
	var jobProfiles map[int]JobSchedulingConfig
	if profiles, exists := cm.Data["OLAKE_JOB_PROFILES"]; exists && profiles != "" {
		jobProfiles = LoadJobProfiles(profiles)
	} else {
		jobProfiles = make(map[int]JobSchedulingConfig)
	}

	// 3. Merge: Start with Legacy, Overwrite with Profiles
	finalConfig := make(map[int]JobSchedulingConfig)

	// Add Legacy entries
	for jobID, config := range legacyMapping {
		finalConfig[jobID] = config
	}

	// Overwrite/Add Profiles
	for jobID, config := range jobProfiles {
		finalConfig[jobID] = config
	}

	w.mu.Lock()
	w.jobMapping = finalConfig
	w.mu.Unlock()

	logger.Infof("updated job configuration: %d legacy entries, %d profiles, %d total merged",
		len(legacyMapping), len(jobProfiles), len(finalConfig))
}
