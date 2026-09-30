package kubernetes

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
	"github.com/datazip-inc/olake-helm/worker/utils/logger"
)

// indexVolume is the resolved index volume for a single pod: which claim to
// mount, where, and the Pebble tuning the connector needs alongside it.
type indexVolume struct {
	claimName    string
	mountPath    string
	cacheSizeMB  int
	maxOpenFiles int
}

// resolveIndexStorage layers the built-in defaults, jobIndexes.default and the
// job's own entry, in increasing order of precedence. Every field is populated
// by the base, so callers never re-check for empties.
func (k *KubernetesExecutor) resolveIndexStorage(jobID int) IndexStorageConfig {
	override := k.configWatcher.GetDefaultJobIndex()
	resolved := mergeIndexStorage(defaultIndexStorage(), override)

	if entry, exists := k.configWatcher.GetJobIndex(jobID); exists {
		resolved = mergeIndexStorage(resolved, entry)
	}

	return resolved
}

// ensureIndexVolume resolves the index storage config for a job and makes sure
// the backing claim exists. It returns nil when the job gets no index volume:
// short-lived operations (spec, check, discover) never carry one, and neither
// does a job that did not ask for one.
func (k *KubernetesExecutor) ensureIndexVolume(ctx context.Context, jobID int, operation types.Command, indexRequired bool, heartbeat func(context.Context, ...interface{})) (*indexVolume, error) {
	// Only sync and clear-destination touch the Iceberg index.
	if !slices.Contains(constants.AsyncCommands, operation) || !indexRequired {
		return nil, nil
	}

	cfg := k.resolveIndexStorage(jobID)
	if err := validateIndexMountPath(cfg.MountPath, jobID); err != nil {
		return nil, err
	}

	claimName, err := k.resolveIndexClaim(ctx, jobID, cfg, heartbeat)
	if err != nil {
		return nil, err
	}

	return &indexVolume{
		claimName:    claimName,
		mountPath:    cfg.MountPath,
		cacheSizeMB:  cfg.CacheSizeMB,
		maxOpenFiles: cfg.MaxOpenFiles,
	}, nil
}

// resolveIndexClaim returns the claim to mount: the operator's own when
// existingClaim names one, otherwise the per-job claim the worker manages.
func (k *KubernetesExecutor) resolveIndexClaim(ctx context.Context, jobID int, cfg IndexStorageConfig, heartbeat func(context.Context, ...interface{})) (string, error) {
	if cfg.ExistingClaim != "" {
		return k.useExistingClaim(ctx, jobID, cfg.ExistingClaim)
	}

	return k.ensureIndexPVC(ctx, jobID, cfg, heartbeat)
}

// useExistingClaim mounts a claim the worker does not own: nothing is created,
// expanded or labelled. Its existence is checked because mounting an absent
// claim leaves the pod Pending until the activity times out, hiding the cause.
func (k *KubernetesExecutor) useExistingClaim(ctx context.Context, jobID int, name string) (string, error) {
	log := logger.Log(ctx)

	claim, err := k.client.CoreV1().PersistentVolumeClaims(k.namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "", fmt.Errorf("jobIndexes existingClaim %q for job %d does not exist in namespace %s", name, jobID, k.namespace)
		}
		return "", indexClaimError("get", name, err)
	}

	if claim.DeletionTimestamp != nil {
		return "", fmt.Errorf("jobIndexes existingClaim %q for job %d is being deleted; wait for it to disappear or point at another claim", name, jobID)
	}

	log.Info("using existing index PVC", "pvcName", name, "jobID", jobID)
	return name, nil
}

// validateIndexMountPath rejects the two mount paths Kubernetes would accept but
// that break the pod: the container root, and anything inside the shared config
// directory, which would hide the job configuration the connector reads at
// startup. A relative path is rejected by the API server with its own message.
func validateIndexMountPath(mountPath string, jobID int) error {
	// Container paths are POSIX regardless of where the worker runs.
	clean := path.Clean(mountPath)

	if clean == "/" {
		return fmt.Errorf("jobIndexes mountPath for job %d cannot be the container root", jobID)
	}

	configDir := path.Clean(constants.ContainerMountDir)
	if clean == configDir || strings.HasPrefix(clean, configDir+"/") {
		return fmt.Errorf("jobIndexes mountPath %q for job %d would shadow the shared config directory %s; choose a path outside it",
			mountPath, jobID, configDir)
	}

	return nil
}

// indexClaimError describes a failed claim operation. A 403 has two very
// different causes - the Role missing the verb, or a cluster policy rejecting
// the claim - so the server's own message is always carried through.
func indexClaimError(action, name string, err error) error {
	if apierrors.IsForbidden(err) {
		return fmt.Errorf("failed to %s index PVC %s: %s. The worker needs [get, create, update] on "+
			"persistentvolumeclaims; if instead an admission policy rejected the claim, the message above "+
			"names the rule and the jobIndexes labels/annotations are how to satisfy it", action, name, err)
	}
	return fmt.Errorf("failed to %s index PVC %s: %s", action, name, err)
}

// ensureIndexPVC creates the per-job claim on first use and is a no-op on every
// later run. The claim is never deleted here: it outlives the pods that use it
// so the index survives between runs.
func (k *KubernetesExecutor) ensureIndexPVC(ctx context.Context, jobID int, cfg IndexStorageConfig, heartbeat func(context.Context, ...interface{})) (string, error) {
	log := logger.Log(ctx)
	name := fmt.Sprintf("olake-index-%d", jobID)
	claims := k.client.CoreV1().PersistentVolumeClaims(k.namespace)

	requested, err := resource.ParseQuantity(cfg.Size)
	if err != nil {
		return "", fmt.Errorf("invalid jobIndexes size %q for job %d: %s", cfg.Size, jobID, err)
	}

	existing, err := claims.Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		// A terminating claim is never re-created under the same name here, and a
		// pod that references one stays Pending until the activity times out.
		if existing.DeletionTimestamp != nil {
			return "", fmt.Errorf("index PVC %s is being deleted; wait for it to disappear and re-run, "+
				"or remove its finalizers - the next run will provision a fresh volume and rebuild the index", name)
		}

		if err := k.expandIndexPVC(ctx, existing, requested, heartbeat); err != nil {
			return "", err
		}

		return name, nil
	}
	if !apierrors.IsNotFound(err) {
		return "", indexClaimError("get", name, err)
	}

	created, err := claims.Create(ctx, k.buildIndexPVC(name, jobID, cfg, requested), metav1.CreateOptions{})
	if err != nil {
		// Another pod for the same job won the race - that is the expected claim.
		if apierrors.IsAlreadyExists(err) {
			log.Debug("index PVC already created concurrently", "pvcName", name, "jobID", jobID)
			return name, nil
		}
		return "", indexClaimError("create", name, err)
	}

	log.Info("created index PVC", "pvcName", created.Name, "jobID", jobID, "size", cfg.Size, "storageClass", cfg.StorageClass)
	return name, nil
}

func (k *KubernetesExecutor) buildIndexPVC(name string, jobID int, cfg IndexStorageConfig, requested resource.Quantity) *corev1.PersistentVolumeClaim {
	accessModes := make([]corev1.PersistentVolumeAccessMode, 0, len(cfg.AccessModes))
	for _, mode := range cfg.AccessModes {
		accessModes = append(accessModes, corev1.PersistentVolumeAccessMode(mode))
	}

	// The app.kubernetes.io/* keys are defaults a user's labels may replace.
	// olake.io/job-id is written last because it is derived from the run: an
	// overridden one would mislabel the claim for every query that groups on it.
	labels := map[string]string{
		"app.kubernetes.io/name":       "olake",
		"app.kubernetes.io/component":  "index-storage",
		"app.kubernetes.io/managed-by": "olake-workers",
	}
	maps.Copy(labels, cfg.Labels)
	labels["olake.io/job-id"] = strconv.Itoa(jobID)

	claim := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   k.namespace,
			Labels:      labels,
			Annotations: cfg.Annotations,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: accessModes,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: requested,
				},
			},
		},
	}

	// Empty storageClass means "use the cluster default"; setting it to "" explicitly
	// would instead disable dynamic provisioning.
	if cfg.StorageClass != "" {
		claim.Spec.StorageClassName = ptr.To(cfg.StorageClass)
	}

	return claim
}

// expandIndexPVC applies the one change Kubernetes allows in place - growing the
// volume. Labels and annotations are creation-time only, and nothing is deleted
// here, since discarding a bound claim would discard the index with it.
//
// Failure fails the run: a sync given a volume smaller than it was configured
// with runs until it fills the disk. A shrink is only a warning, since the
// volume on disk is already larger than what was asked for.
func (k *KubernetesExecutor) expandIndexPVC(ctx context.Context, existing *corev1.PersistentVolumeClaim, requested resource.Quantity, heartbeat func(context.Context, ...interface{})) error {
	log := logger.Log(ctx)
	current := existing.Spec.Resources.Requests[corev1.ResourceStorage]

	switch requested.Cmp(current) {
	case 1:
		patch := existing.DeepCopy()
		if patch.Spec.Resources.Requests == nil {
			patch.Spec.Resources.Requests = corev1.ResourceList{}
		}

		patch.Spec.Resources.Requests[corev1.ResourceStorage] = requested
		if _, err := k.client.CoreV1().PersistentVolumeClaims(k.namespace).Update(ctx, patch, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("failed to expand index PVC %s from %s to %s: %s. Expansion needs "+
				"allowVolumeExpansion: true on the volume's StorageClass, and [get, update] on "+
				"persistentvolumeclaims; set jobIndexes size back to %s to run on the volume as it is",
				existing.Name, current.String(), requested.String(), err, current.String())
		}

		log.Info("expanding index PVC", "pvcName", existing.Name, "from", current.String(), "to", requested.String())
	case -1:
		log.Warn("index PVC shrink requested but Kubernetes does not support it; keeping the current size",
			"pvcName", existing.Name, "current", current.String(), "configured", requested.String())
	}

	return k.waitForIndexResize(ctx, existing.Name, requested, heartbeat)
}

// waitForIndexResize blocks until the CSI driver has grown the backing device,
// so a sync never starts on a volume still at the old size.
//
// It does not wait for the filesystem too. That half runs in kubelet during the
// mount of the very pod this function runs before, so waiting for it would block
// on a mount that cannot happen until the wait ends. FileSystemResizePending is
// therefore success: the pod stays in ContainerCreating until kubelet finishes.
func (k *KubernetesExecutor) waitForIndexResize(ctx context.Context, name string, target resource.Quantity, heartbeat func(context.Context, ...interface{})) error {
	log := logger.Log(ctx)
	deadline := time.Now().Add(constants.IndexResizeTimeout)

	var lastStatus string
	for {
		if heartbeat != nil {
			heartbeat(ctx, fmt.Sprintf("waiting for index volume %s to reach %s", name, target.String()))
		}

		claim, err := k.client.CoreV1().PersistentVolumeClaims(k.namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return indexClaimError("get", name, err)
		}

		// An unbound claim has no volume to grow, and under WaitForFirstConsumer
		// it cannot bind until the pod this runs before exists. A claim that
		// stays unbound is caught later, when the pod cannot be scheduled.
		if claim.Status.Phase != corev1.ClaimBound {
			log.Debug("index PVC is not bound yet; its size applies when it is provisioned", "pvcName", name)
			return nil
		}

		capacity := claim.Status.Capacity[corev1.ResourceStorage]
		if capacity.Cmp(target) >= 0 {
			return nil
		}

		state, status := indexResizeProgress(claim)
		switch state {
		case indexResizeFailed:
			return fmt.Errorf("index PVC %s cannot grow from %s to %s: %s. Set jobIndexes size back to %s "+
				"to run on the volume as it is", name, capacity.String(), target.String(), status, capacity.String())
		case indexResizeMountPending:
			log.Info("index volume grown; the pod's mount will resize its filesystem", "pvcName", name, "status", status)
			return nil
		}

		if status != "" && status != lastStatus {
			log.Info("waiting for index PVC expansion", "pvcName", name, "status", status)
			lastStatus = status
		}

		if !time.Now().Before(deadline) {
			return fmt.Errorf("index PVC %s did not grow from %s to %s within %v (last status: %q). "+
				"Check that a CSI resizer is running for its StorageClass and that the storage quota is not "+
				"exhausted; set jobIndexes size back to %s to run on the volume as it is",
				name, capacity.String(), target.String(), constants.IndexResizeTimeout, lastStatus, capacity.String())
		}

		select {
		case <-time.After(constants.IndexResizePollInterval):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// indexResizeState is how far an expansion has got, as far as the claim says.
// The values are ranked: where a claim reports several at once, the highest
// wins, so nothing depends on the order the API server lists them in.
type indexResizeState int

const (
	// indexResizeUnknown - the claim says nothing about an expansion.
	indexResizeUnknown indexResizeState = iota
	// indexResizeGrowing - the driver is still growing the device.
	indexResizeGrowing
	// indexResizeMountPending - the device is grown and only a pod's mount can
	// resize the filesystem on top of it.
	indexResizeMountPending
	// indexResizeFailed - the expansion cannot succeed.
	indexResizeFailed
)

// indexResizeProgress reads how far an expansion has got, with the driver's own
// wording for it. allocatedResourceStatuses is authoritative on the clusters
// that maintain it; the conditions are the fallback for those that do not, and
// Kubernetes leaves several of them set at once - a device that is grown keeps
// Resizing alongside FileSystemResizePending - so every condition is read and ranked.
func indexResizeProgress(claim *corev1.PersistentVolumeClaim) (indexResizeState, string) {
	switch status := claim.Status.AllocatedResourceStatuses[corev1.ResourceStorage]; status {
	case corev1.PersistentVolumeClaimControllerResizeInfeasible, corev1.PersistentVolumeClaimNodeResizeInfeasible:
		return indexResizeFailed, string(status)
	case corev1.PersistentVolumeClaimControllerResizeInProgress:
		return indexResizeGrowing, string(status)
	case corev1.PersistentVolumeClaimNodeResizePending, corev1.PersistentVolumeClaimNodeResizeInProgress:
		return indexResizeMountPending, string(status)
	}

	state, message := indexResizeUnknown, ""
	record := func(candidate indexResizeState, condition corev1.PersistentVolumeClaimCondition) {
		if candidate > state {
			state, message = candidate, cmp.Or(condition.Message, string(condition.Type))
		}
	}

	for _, condition := range claim.Status.Conditions {
		if condition.Status != corev1.ConditionTrue {
			continue
		}

		switch condition.Type {
		case corev1.PersistentVolumeClaimControllerResizeError:
			record(indexResizeFailed, condition)
		case corev1.PersistentVolumeClaimFileSystemResizePending:
			record(indexResizeMountPending, condition)
		case corev1.PersistentVolumeClaimResizing:
			record(indexResizeGrowing, condition)
		}
	}
	return state, message
}

// JobIndexes is the parsed jobIndexes block: the settings every job starts from,
// and the per-JobID entries that are deep-merged over them.
type JobIndexes struct {
	Default IndexStorageConfig
	Jobs    map[int]IndexStorageConfig
}

func LoadJobIndexes(raw string) JobIndexes {
	loaded := JobIndexes{Jobs: map[int]IndexStorageConfig{}}
	if strings.TrimSpace(raw) == "" {
		return loaded
	}

	// Each entry is decoded on its own so a malformed one is skipped rather than
	// partially applied to the jobs around it.
	var wire struct {
		Default json.RawMessage            `json:"default"`
		Jobs    map[string]json.RawMessage `json:"jobs"`
	}
	if err := json.Unmarshal([]byte(raw), &wire); err != nil {
		logger.Errorf("failed to parse OLAKE_JOB_INDEXES as json: %s", err)
		return loaded
	}

	if len(wire.Default) > 0 {
		if err := json.Unmarshal(wire.Default, &loaded.Default); err != nil {
			logger.Errorf("ignoring jobIndexes.default: %s", err)
			loaded.Default = IndexStorageConfig{}
		}
	}

	for key, value := range wire.Jobs {
		jobID, err := strconv.Atoi(key)
		if err != nil {
			logger.Warnf("ignoring jobIndexes.jobs key %q: expected a JobID", key)
			continue
		}

		var cfg IndexStorageConfig
		if err := json.Unmarshal(value, &cfg); err != nil {
			logger.Errorf("ignoring jobIndexes.jobs entry %d: %s", jobID, err)
			continue
		}
		loaded.Jobs[jobID] = cfg
	}

	logger.Infof("job index settings loaded: %d job entries", len(loaded.Jobs))
	return loaded
}

// IndexStorageConfig describes the per-job block volume that holds the Pebble
// index. Every async operation of a job mounts the same one.
type IndexStorageConfig struct {
	// Size is grow-only; Kubernetes rejects shrinking.
	Size         string   `json:"size,omitempty"`
	StorageClass string   `json:"storageClass,omitempty"`
	AccessModes  []string `json:"accessModes,omitempty"`
	MountPath    string   `json:"mountPath,omitempty"`
	// CacheSizeMB and MaxOpenFiles are Pebble limits, applied per stream.
	CacheSizeMB  int `json:"cacheSizeMB,omitempty"`
	MaxOpenFiles int `json:"maxOpenFiles,omitempty"`
	// Labels override the worker's app.kubernetes.io/* defaults, so a cluster
	// label policy can be satisfied; only olake.io/job-id is reserved.
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
	// ExistingClaim is mounted as-is, so every provisioning field above is
	// ignored for that job.
	ExistingClaim string `json:"existingClaim,omitempty"`
}

func defaultIndexStorage() IndexStorageConfig {
	return IndexStorageConfig{
		Size:         constants.DefaultIndexSize,
		MountPath:    constants.DefaultIndexMountPath,
		AccessModes:  []string{string(corev1.ReadWriteOnce)},
		CacheSizeMB:  constants.DefaultIndexCacheSizeMB,
		MaxOpenFiles: constants.DefaultIndexMaxOpenFiles,
	}
}

// mergeIndexStorage deep-merges override onto base per key, so a job that only
// overrides `size` keeps the inherited storageClass, mountPath and the rest.
func mergeIndexStorage(base, override IndexStorageConfig) IndexStorageConfig {
	merged := base
	if override.Size != "" {
		merged.Size = override.Size
	}
	if override.StorageClass != "" {
		merged.StorageClass = override.StorageClass
	}
	if len(override.AccessModes) > 0 {
		merged.AccessModes = override.AccessModes
	}
	if override.MountPath != "" {
		merged.MountPath = override.MountPath
	}
	if override.CacheSizeMB != 0 {
		merged.CacheSizeMB = override.CacheSizeMB
	}
	if override.MaxOpenFiles != 0 {
		merged.MaxOpenFiles = override.MaxOpenFiles
	}
	if override.ExistingClaim != "" {
		merged.ExistingClaim = override.ExistingClaim
	}
	// Unlike every field above, these merge per key: a job that adds one label
	// must not drop cluster-wide labels an admission policy may require.
	merged.Labels = mergeStringMaps(base.Labels, override.Labels)
	// TODO: labels and annotations are not updated in already created PVCs
	merged.Annotations = mergeStringMaps(base.Annotations, override.Annotations)
	return merged
}

// mergeStringMaps returns base with override applied on top, per key. Returns nil
// when both are empty so the claim carries no empty map.
func mergeStringMaps(base, override map[string]string) map[string]string {
	if len(base) == 0 && len(override) == 0 {
		return nil
	}
	merged := make(map[string]string, len(base)+len(override))
	maps.Copy(merged, base)
	maps.Copy(merged, override)
	return merged
}
