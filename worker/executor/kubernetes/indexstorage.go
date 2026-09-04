package kubernetes

import (
	"context"
	"fmt"
	"maps"
	"path"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
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

// resolveIndexStorage layers the built-in defaults, the default profile
// (JobID 0) and the job's own profile, in increasing order of precedence.
// Every field is populated by the base, so callers never re-check for empties.
//
// The chart ships its defaults as profile 0, so this whole chain arrives in
// OLAKE_JOB_PROFILES and is picked up by the ConfigMap watcher.
func (k *KubernetesExecutor) resolveIndexStorage(jobID int) IndexStorageConfig {
	resolved := defaultIndexStorage()

	if profile, exists := k.configWatcher.GetJobProfile(0); exists {
		resolved = mergeIndexStorage(resolved, profile.IndexStorage)

		// existingClaim names one specific volume, so it is never inherited.
		// On profile 0 it would point every job at the same claim, and a
		// ReadWriteOnce volume would then serialise every sync in the deployment.
		if resolved.ExistingClaim != "" {
			logger.Warnf("ignoring indexStorage.existingClaim %q on profile 0: it is only honoured on a specific JobID profile", resolved.ExistingClaim)
			resolved.ExistingClaim = ""
		}
	}
	// jobID 0 is the default profile itself and is already merged above.
	if jobID != 0 {
		if profile, exists := k.configWatcher.GetJobProfile(jobID); exists {
			resolved = mergeIndexStorage(resolved, profile.IndexStorage)
		}
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
		return nil, fmt.Errorf("unknown indexStorage.mode %q for job %d (expected %q or %q)",
			cfg.Mode, jobID, indexStorageModePVC, indexStorageModeNone)
	}

	if err := validateIndexMountPath(cfg.MountPath, jobID); err != nil {
		return nil, err
	}

	claimName, err := k.resolveIndexClaim(ctx, jobID, operation, cfg)
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
func (k *KubernetesExecutor) resolveIndexClaim(ctx context.Context, jobID int, operation types.Command, cfg IndexStorageConfig) (string, error) {
	if cfg.ExistingClaim != "" {
		return k.useExistingClaim(ctx, jobID, cfg.ExistingClaim)
	}

	// Only the worker-managed path writes metadata onto a claim, so only it has
	// to reject metadata the API server would refuse.
	if err := validateIndexMetadata(cfg, jobID); err != nil {
		return "", err
	}

	// A per-job claim needs a real JobID to key on. Running the pod anyway would
	// write the index to the container's writable layer and lose it, so this
	// fails the run instead of degrading silently.
	if jobID <= 0 {
		return "", fmt.Errorf("cannot provision an index volume for %s: invalid JobID %d", operation, jobID)
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
			return "", fmt.Errorf("indexStorage.existingClaim %q for job %d does not exist in namespace %s", name, jobID, k.namespace)
		}
		return "", indexClaimError("get", name, err)
	}
	if claim.DeletionTimestamp != nil {
		return "", fmt.Errorf("indexStorage.existingClaim %q for job %d is being deleted; wait for it to disappear or point at another claim", name, jobID)
	}

	log.Info("using existing index PVC", "pvcName", name, "jobID", jobID)
	return name, nil
}

// validateIndexMountPath rejects a mount path that would break the pod rather
// than merely misplace the index. Mounting on top of the shared config
// directory hides the job configuration the connector reads at startup, which
// surfaces as an unrelated-looking failure inside the container.
func validateIndexMountPath(mountPath string, jobID int) error {
	// Container paths are POSIX regardless of where the worker runs.
	clean := path.Clean(mountPath)

	if !path.IsAbs(clean) {
		return fmt.Errorf("indexStorage.mountPath %q for job %d must be an absolute path", mountPath, jobID)
	}
	if clean == "/" {
		return fmt.Errorf("indexStorage.mountPath for job %d cannot be the container root", jobID)
	}

	configDir := path.Clean(constants.ContainerMountDir)
	if clean == configDir || strings.HasPrefix(clean, configDir+"/") {
		return fmt.Errorf("indexStorage.mountPath %q for job %d would shadow the shared config directory %s; choose a path outside it",
			mountPath, jobID, configDir)
	}

	return nil
}

// validateIndexMetadata rejects label and annotation keys the API server would
// reject anyway, so the run fails naming the offending key instead of surfacing
// as a generic claim failure at create time.
func validateIndexMetadata(cfg IndexStorageConfig, jobID int) error {
	for key, value := range cfg.Labels {
		if errs := validation.IsQualifiedName(key); len(errs) > 0 {
			return fmt.Errorf("invalid indexStorage.labels key %q for job %d: %s", key, jobID, strings.Join(errs, "; "))
		}
		if errs := validation.IsValidLabelValue(value); len(errs) > 0 {
			return fmt.Errorf("invalid indexStorage.labels value %q for key %q on job %d: %s", value, key, jobID, strings.Join(errs, "; "))
		}
	}
	// Annotation values are unconstrained; only the key is a qualified name.
	for key := range cfg.Annotations {
		if errs := validation.IsQualifiedName(key); len(errs) > 0 {
			return fmt.Errorf("invalid indexStorage.annotations key %q for job %d: %s", key, jobID, strings.Join(errs, "; "))
		}
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
			"names the rule and indexStorage.labels/annotations are how to satisfy it", action, name, err)
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
		return "", fmt.Errorf("invalid indexStorage.size %q for job %d: %s", cfg.Size, jobID, err)
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
	if len(accessModes) == 0 {
		accessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	}

	labels := map[string]string{
		"app.kubernetes.io/name":       "olake",
		"app.kubernetes.io/component":  "index-storage",
		"app.kubernetes.io/managed-by": "olake-workers",
	}
	maps.Copy(labels, cfg.Labels)
	labels["olake.io/job-id"] = fmt.Sprintf("%d", jobID)

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
// volume. Nothing is deleted here: discarding a bound claim would discard the
// index with it.
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
		// Expansion needs allowVolumeExpansion on the StorageClass. Failing to grow
		// is not a reason to fail the sync - the existing volume is still usable.
		log.Warn("failed to expand index PVC; continuing with the current size",
			"pvcName", existing.Name, "current", current.String(), "configured", requested.String(), "error", err)
		return
	}

	log.Info("expanded index PVC", "pvcName", existing.Name, "from", current.String(), "to", requested.String())
}
