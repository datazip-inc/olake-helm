package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"path"
	"slices"
	"strconv"
	"strings"

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
// mount, where, and the Pebble tuning the connector needs alongside it. Sync and
// clear-destination of the same JobID resolve to the same claim, which is what
// makes them share one index.
type indexVolume struct {
	claimName    string
	mountPath    string
	cacheSizeMB  int
	maxOpenFiles int
}

// indexPVCName returns the deterministic per-job claim name. It keys on JobID
// only - never on the operation - so every async run of a job mounts the same
// volume.
func indexPVCName(jobID int) string {
	return fmt.Sprintf("olake-index-%d", jobID)
}

// resolveIndexStorage layers the built-in defaults, jobIndexes.default and the
// job's own entry under jobIndexes.jobs, in increasing order of precedence. Every
// field is populated by the base, so callers never re-check for empties.
//
// The whole chain arrives in OLAKE_JOB_INDEXES and is picked up by the ConfigMap
// watcher, so a values change takes effect without restarting the worker.
func (k *KubernetesExecutor) resolveIndexStorage(jobID int) IndexStorageConfig {
	base := k.configWatcher.GetDefaultJobIndex()
	resolved := mergeIndexStorage(defaultIndexStorage(), &base)

	// existingClaim names one specific volume, so it is never inherited. Under
	// `default` it would point every job at the same claim, and a ReadWriteOnce
	// volume would then serialise every sync in the deployment.
	if resolved.ExistingClaim != "" {
		logger.Warnf("ignoring jobIndexes.default.existingClaim %q: it is only honoured under jobIndexes.jobs", resolved.ExistingClaim)
		resolved.ExistingClaim = ""
	}

	if entry, exists := k.configWatcher.GetJobIndex(jobID); exists {
		resolved = mergeIndexStorage(resolved, &entry)
	}

	return resolved
}

// ensureIndexVolume resolves the index storage config for a job and makes sure
// the backing claim exists. It returns nil when the job gets no index volume:
// short-lived operations (spec, check, discover) never carry one, a job that
// did not ask for one never carries one, and neither does mode "none".
func (k *KubernetesExecutor) ensureIndexVolume(ctx context.Context, jobID int, operation types.Command, indexRequired bool) (*indexVolume, error) {
	log := logger.Log(ctx)

	// Only sync and clear-destination touch the Iceberg index.
	if !slices.Contains(constants.AsyncCommands, operation) {
		return nil, nil
	}

	if !indexRequired {
		log.Debug("job did not request an index volume", "jobID", jobID)
		return nil, nil
	}

	cfg := k.resolveIndexStorage(jobID)

	switch cfg.Mode {
	case indexStorageModeNone:
		log.Debug("index storage disabled for job", "jobID", jobID)
		return nil, nil
	case indexStorageModePVC:
	default:
		return nil, fmt.Errorf("unknown jobIndexes mode %q for job %d (expected %q or %q)",
			cfg.Mode, jobID, indexStorageModePVC, indexStorageModeNone)
	}

	if err := validateIndexMountPath(cfg.MountPath, jobID); err != nil {
		return nil, err
	}

	claimName, err := k.resolveIndexClaim(ctx, jobID, cfg)
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
func (k *KubernetesExecutor) resolveIndexClaim(ctx context.Context, jobID int, cfg IndexStorageConfig) (string, error) {
	if cfg.ExistingClaim != "" {
		return k.useExistingClaim(ctx, jobID, cfg.ExistingClaim)
	}

	return k.ensureIndexPVC(ctx, jobID, cfg)
}

// useExistingClaim mounts a claim the operator created and the worker does not
// own. Nothing about it is created, expanded or labelled here. It is checked for
// existence because mounting a claim that is absent leaves the pod Pending until
// the activity times out, which hides the cause.
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
// directory, which hides the job configuration the connector reads at startup
// and surfaces as an unrelated-looking failure inside the container. A path that
// is not absolute is rejected by the API server with its own message.
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

// indexClaimError describes a failed claim operation. A 403 gets an extra hint
// because it has two very different causes - the Role missing the verb, or a
// cluster policy rejecting the claim - and only the server's own message
// distinguishes them, so it is always carried through.
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
func (k *KubernetesExecutor) ensureIndexPVC(ctx context.Context, jobID int, cfg IndexStorageConfig) (string, error) {
	log := logger.Log(ctx)
	name := indexPVCName(jobID)
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
		k.expandIndexPVC(ctx, existing, requested)
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

	// The app.kubernetes.io/* keys are conventional defaults, so a cluster whose
	// label policy demands different values can replace them. olake.io/job-id is
	// written last because its value is derived from the run rather than chosen:
	// an overridden one would mislabel the claim for every operator query and
	// every cost-allocation tool that groups on it.
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
// volume. Labels and annotations are deliberately not reapplied: they are set
// when the claim is created and left alone afterwards. Nothing is deleted here
// either, since discarding a bound claim would discard the index with it.
//
// Failure is logged, not returned. Expansion needs allowVolumeExpansion on the
// StorageClass, and not growing is no reason to fail a sync the existing volume
// can still serve.
func (k *KubernetesExecutor) expandIndexPVC(ctx context.Context, existing *corev1.PersistentVolumeClaim, requested resource.Quantity) {
	log := logger.Log(ctx)

	current := existing.Spec.Resources.Requests[corev1.ResourceStorage]
	switch requested.Cmp(current) {
	case 0:
		return
	case -1:
		log.Warn("index PVC shrink requested but Kubernetes does not support it; keeping the current size",
			"pvcName", existing.Name, "current", current.String(), "configured", requested.String())
		return
	}

	patch := existing.DeepCopy()
	if patch.Spec.Resources.Requests == nil {
		patch.Spec.Resources.Requests = corev1.ResourceList{}
	}
	patch.Spec.Resources.Requests[corev1.ResourceStorage] = requested
	if _, err := k.client.CoreV1().PersistentVolumeClaims(k.namespace).Update(ctx, patch, metav1.UpdateOptions{}); err != nil {
		log.Warn("failed to expand index PVC; continuing with the current size",
			"pvcName", existing.Name, "current", current.String(), "configured", requested.String(), "error", err)
		return
	}

	log.Info("expanded index PVC", "pvcName", existing.Name, "from", current.String(), "to", requested.String())
}

// JobIndexes is the parsed jobIndexes block: the settings every job starts from,
// and the per-JobID entries that are deep-merged over them.
type JobIndexes struct {
	Default IndexStorageConfig
	Jobs    map[int]IndexStorageConfig
}

// LoadJobIndexes parses OLAKE_JOB_INDEXES. The default entry and each job entry
// are decoded on their own so one malformed entry costs only itself: failing the
// whole block would silently move every job back to the built-in defaults,
// including its StorageClass, with nothing but a log line to say so.
func LoadJobIndexes(raw string) JobIndexes {
	loaded := JobIndexes{Jobs: map[int]IndexStorageConfig{}}
	if strings.TrimSpace(raw) == "" {
		logger.Info("no job index settings found")
		return loaded
	}

	// Job keys stay strings until they are parsed one at a time. Decoding
	// straight into map[int] would fail the whole block on a single key that is
	// not a number or overflows int.
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

// Index storage modes and the defaults applied when a field is left unset.
const (
	indexStorageModePVC  = "pvc"
	indexStorageModeNone = "none"
)

// IndexStorageConfig describes the per-job block volume that holds the Pebble
// index used by the direct positional-delete / deletion-vector write path.
// The same volume is mounted by every async operation of a job (sync and
// clear-destination), so both see the same index.
type IndexStorageConfig struct {
	// Mode selects whether the worker provisions a per-job PVC ("pvc") or the
	// job runs without an index volume ("none").
	Mode string `json:"mode,omitempty"`
	// Size is the requested volume size. Growing it is applied on the next run;
	// Kubernetes rejects shrinking.
	Size string `json:"size,omitempty"`
	// StorageClass is a passthrough to the PVC. Empty uses the cluster default.
	StorageClass string `json:"storageClass,omitempty"`
	// AccessModes defaults to ReadWriteOnce, which block storage requires.
	AccessModes []string `json:"accessModes,omitempty"`
	// MountPath is where the volume is mounted inside the connector container.
	MountPath string `json:"mountPath,omitempty"`
	// CacheSizeMB is the Pebble block cache size in megabytes, per stream.
	CacheSizeMB int `json:"cacheSizeMB,omitempty"`
	// MaxOpenFiles caps the file descriptors Pebble keeps open, per stream.
	MaxOpenFiles int `json:"maxOpenFiles,omitempty"`
	// Labels are merged into the index PVC. They override the worker's
	// app.kubernetes.io/* defaults, so a cluster whose label policy demands
	// different values can satisfy it; only olake.io/job-id is reserved.
	Labels map[string]string `json:"labels,omitempty"`
	// Annotations are applied to the index PVC, for backup tooling and the like.
	Annotations map[string]string `json:"annotations,omitempty"`
	// ExistingClaim names a PVC the operator created. When set the worker mounts
	// it as-is and creates nothing, so Size, StorageClass, AccessModes, Labels
	// and Annotations do not apply to that job.
	ExistingClaim string `json:"existingClaim,omitempty"`
}

// defaultIndexStorage returns the built-in index settings. They are the base
// jobIndexes is merged onto, and they stand on their own when the chart emits no
// default entry at all, so a job that asked for an index always gets a usable
// one rather than a claim with no size or mount path.
func defaultIndexStorage() IndexStorageConfig {
	return IndexStorageConfig{
		Mode:         indexStorageModePVC,
		Size:         constants.DefaultIndexSize,
		MountPath:    constants.DefaultIndexMountPath,
		AccessModes:  []string{string(corev1.ReadWriteOnce)},
		CacheSizeMB:  constants.DefaultIndexCacheSizeMB,
		MaxOpenFiles: constants.DefaultIndexMaxOpenFiles,
	}
}

// mergeIndexStorage deep-merges override onto base per key, so a job that only
// overrides `size` keeps the inherited storageClass, mountPath and the rest.
func mergeIndexStorage(base IndexStorageConfig, override *IndexStorageConfig) IndexStorageConfig {
	if override == nil {
		return base
	}

	merged := base
	if override.Mode != "" {
		merged.Mode = override.Mode
	}
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
	// Unlike every field above, these merge per key rather than replacing: a job
	// that adds one label must not drop the cluster-wide labels set on profile 0,
	// which an admission policy may require for the claim to be created at all.
	merged.Labels = mergeStringMaps(base.Labels, override.Labels)
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
