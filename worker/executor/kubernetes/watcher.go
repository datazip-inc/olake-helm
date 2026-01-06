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
	mu          sync.RWMutex
	jobMapping  map[int]map[string]string // TODO: use sync.Map
	jobProfiles map[int]JobSchedulingConfig

	ctx    context.Context
	cancel context.CancelFunc
}

func NewConfigMapWatcher(clientset kubernetes.Interface, namespace string) *ConfigMapWatcher {
	ctx, cancel := context.WithCancel(context.Background())
	return &ConfigMapWatcher{
		clientset:     clientset,
		namespace:     namespace,
		configMapName: "olake-workers-config",
		jobMapping:    make(map[int]map[string]string),
		jobProfiles:   make(map[int]JobSchedulingConfig),
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

// GetJobMapping returns mapping for specific jobID (thread-safe)
// Uses RWMutex because this function is called concurrently by multiple pod creation goroutines
// while the ConfigMap informer goroutine may be updating w.jobMapping in the background.
// RLock allows multiple concurrent readers while preventing data races with writer updates.
func (w *ConfigMapWatcher) GetJobMapping(jobID int) (map[string]string, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	mapping, exists := w.jobMapping[jobID]
	if !exists {
		return map[string]string{}, false
	}

	result := make(map[string]string, len(mapping))
	for k, v := range mapping {
		result[k] = v
	}
	return result, true
}

// GetJobProfile returns profile for specific jobID (thread-safe)
func (w *ConfigMapWatcher) GetJobProfile(jobID int) (JobSchedulingConfig, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	profile, exists := w.jobProfiles[jobID]
	return profile, exists
}

// GetAllJobMapping returns all job mappings (thread-safe)
// Returns a deep copy to prevent external modification of internal state
func (w *ConfigMapWatcher) GetAllJobMapping() map[int]map[string]string {
	w.mu.RLock()
	defer w.mu.RUnlock()

	result := make(map[int]map[string]string, len(w.jobMapping))
	for jobID, labels := range w.jobMapping {
		labelsCopy := make(map[string]string, len(labels))
		for k, v := range labels {
			labelsCopy[k] = v
		}
		result[jobID] = labelsCopy
	}
	return result
}

func (w *ConfigMapWatcher) updateJobMapping(cm *corev1.ConfigMap) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// [TO BE DEPRECATED]
	// 1. Load legacy job mapping
	if rawMapping, exists := cm.Data["OLAKE_JOB_MAPPING"]; exists && rawMapping != "" {
		w.jobMapping = LoadJobMapping(rawMapping)
		logger.Infof("updated job mapping with %d entries", len(w.jobMapping))
	} else {
		logger.Debugf("no OLAKE_JOB_MAPPING in ConfigMap %s", w.configMapName)
		w.jobMapping = map[int]map[string]string{}
	}

	// 2. Load job profiles
	if rawProfiles, exists := cm.Data["OLAKE_JOB_PROFILES"]; exists && rawProfiles != "" {
		w.jobProfiles = LoadJobProfiles(rawProfiles)
		logger.Infof("updated job profiles with %d entries", len(w.jobProfiles))
	} else {
		logger.Debugf("no OLAKE_JOB_PROFILES in ConfigMap %s", w.configMapName)
		w.jobProfiles = map[int]JobSchedulingConfig{}
	}
}
