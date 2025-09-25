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

	"github.com/datazip-inc/olake-helm/worker/logger"
)

// ConfigMapWatcher watches for ConfigMap changes and provides thread-safe access to job mapping
type ConfigMapWatcher struct {
	clientset       kubernetes.Interface
	informerFactory informers.SharedInformerFactory
	namespace       string
	configMapName   string

	mu         sync.RWMutex
	jobMapping map[int]map[string]string

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
		ctx:           ctx,
		cancel:        cancel,
	}
}

func (w *ConfigMapWatcher) Start() error {
	logger.Infof("Starting ConfigMap watcher for %s/%s", w.namespace, w.configMapName)

	w.informerFactory = informers.NewSharedInformerFactoryWithOptions(
		w.clientset,
		30*time.Second,
		informers.WithNamespace(w.namespace),
	)
	cmi := w.informerFactory.Core().V1().ConfigMaps()

	_, err := cmi.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if cm, ok := obj.(*corev1.ConfigMap); ok && cm.Name == w.configMapName {
				w.update(cm)
			}
		},
		UpdateFunc: func(oldObj, newObj any) {
			oldCm, okOld := oldObj.(*corev1.ConfigMap)
			newCm, okNew := newObj.(*corev1.ConfigMap)
			if !okOld || !okNew || newCm.Name != w.configMapName {
				return
			}
			if oldCm.ResourceVersion == newCm.ResourceVersion {
				return
			}
			w.update(newCm)
		},
		DeleteFunc: func(obj any) {
			if cm, ok := obj.(*corev1.ConfigMap); ok && cm.Name == w.configMapName {
				logger.Warnf("ConfigMap %s deleted - keeping cached mapping", w.configMapName)
			}
		},
	})
	if err != nil {
		return fmt.Errorf("failed to add ConfigMap handler: %v", err)
	}

	w.informerFactory.Start(w.ctx.Done())
	if !cache.WaitForCacheSync(w.ctx.Done(), cmi.Informer().HasSynced) {
		return fmt.Errorf("failed to sync ConfigMap cache")
	}
	logger.Infof("ConfigMap watcher started")
	return nil
}

func (w *ConfigMapWatcher) Stop() {
	logger.Infof("Stopping ConfigMap watcher")
	w.cancel()
}

func (w *ConfigMapWatcher) GetJobMapping(jobID int) (map[string]string, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	m, ok := w.jobMapping[jobID]
	if !ok {
		return map[string]string{}, false
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out, true
}

func (w *ConfigMapWatcher) update(cm *corev1.ConfigMap) {
	raw, ok := cm.Data["OLAKE_JOB_MAPPING"]
	if !ok || raw == "" {
		logger.Debugf("No OLAKE_JOB_MAPPING in %s", w.configMapName)
		w.mu.Lock()
		w.jobMapping = map[int]map[string]string{}
		w.mu.Unlock()
		return
	}
	newMapping := LoadJobMapping(raw)
	w.mu.Lock()
	w.jobMapping = newMapping
	w.mu.Unlock()
	logger.Infof("Updated job mapping with %d entries", len(newMapping))
}
