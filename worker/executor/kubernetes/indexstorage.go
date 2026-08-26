package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
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

// IndexVolume is the resolved index volume for a single pod: which claim to
// mount and where. Sync and clear-destination of the same JobID resolve to the
// same ClaimName, which is what makes them share one Pebble index.
type IndexVolume struct {
	ClaimName string
	MountPath string
	SubPath   string
	// Pebble tuning handed to the connector alongside the mount.
	CacheSizeMB  int
	MaxOpenFiles int
}

// errIndexRBAC marks a claim operation the worker is not permitted to perform.
// It is handled rather than returned to the caller: see EnsureIndexVolume.
var errIndexRBAC = errors.New("not permitted to manage index PVCs")

// IndexPVCName returns the deterministic per-job claim name. It keys on JobID
// only - never on the operation - so every async run of a job mounts the same
// volume.
func IndexPVCName(jobID int) string {
	return fmt.Sprintf("olake-index-%d", jobID)
}

// ResolveIndexStorage merges the chart-wide default with the job profile and
// the default profile (JobID 0), in increasing order of precedence, and fills in
// the built-in defaults so callers never have to re-check for empty fields.
func (k *KubernetesExecutor) ResolveIndexStorage(jobID int) IndexStorageConfig {
	resolved := k.config.IndexStorage

	if profile, exists := k.configWatcher.GetJobProfile(0); exists {
		resolved = MergeIndexStorage(resolved, profile.IndexStorage)
	}
	if profile, exists := k.configWatcher.GetJobProfile(jobID); exists {
		resolved = MergeIndexStorage(resolved, profile.IndexStorage)
	}

	if resolved.Mode == "" {
		resolved.Mode = DefaultIndexStorageMode
	}
	if resolved.Size == "" {
		resolved.Size = DefaultIndexStorageSize
	}
	if resolved.MountPath == "" {
		resolved.MountPath = constants.DefaultIndexMountPath
	}
	if resolved.CacheSizeMB <= 0 {
		resolved.CacheSizeMB = constants.DefaultIndexCacheSizeMB
	}
	if resolved.MaxOpenFiles <= 0 {
		resolved.MaxOpenFiles = constants.DefaultIndexMaxOpenFiles
	}

	return resolved
}

// EnsureIndexVolume resolves the index storage config for a job and makes sure
// the backing claim exists. It returns nil when the job gets no index volume:
// short-lived operations (spec, check, discover) never carry one, and neither
// does mode "none".
func (k *KubernetesExecutor) EnsureIndexVolume(ctx context.Context, jobID int, operation types.Command) (*IndexVolume, error) {
	log := logger.Log(ctx)

	// Only sync and clear-destination touch the Iceberg index.
	if !slices.Contains(constants.AsyncCommands, operation) {
		return nil, nil
	}

	cfg := k.ResolveIndexStorage(jobID)

	if cfg.Mode == IndexStorageModeNone {
		log.Debug("index storage disabled for job", "jobID", jobID)
		return nil, nil
	}

	if err := validateIndexMountPath(cfg.MountPath, jobID); err != nil {
		return nil, err
	}

	volume, err := k.resolveIndexVolume(ctx, jobID, operation, cfg)
	if err != nil {
		// A worker that upgraded ahead of its RBAC cannot manage claims. The
		// index is derived state that the connector rebuilds when it is absent,
		// so the run continues without it rather than breaking every sync until
		// an operator notices. Loud, repeated, and recoverable.
		if errors.Is(err, errIndexRBAC) {
			log.Warn("running without an index volume; the connector will rebuild its index every run until this is fixed",
				"jobID", jobID, "error", err)
			return nil, nil
		}
		return nil, err
	}
	return volume, nil
}

func (k *KubernetesExecutor) resolveIndexVolume(ctx context.Context, jobID int, operation types.Command, cfg IndexStorageConfig) (*IndexVolume, error) {
	switch cfg.Mode {
	case IndexStorageModeExistingClaim:
		if err := k.checkExistingClaim(ctx, jobID, cfg.ExistingClaim); err != nil {
			return nil, err
		}
		return newIndexVolume(cfg.ExistingClaim, cfg), nil

	case IndexStorageModePVC:
		// A per-job claim needs a real JobID to key on. Running the pod anyway
		// would write the index to the container's writable layer and lose it,
		// so this fails the run instead of degrading silently.
		if jobID <= 0 {
			return nil, fmt.Errorf("cannot provision an index volume for %s: invalid JobID %d", operation, jobID)
		}
		claimName, err := k.ensureIndexPVC(ctx, jobID, cfg)
		if err != nil {
			return nil, err
		}
		return newIndexVolume(claimName, cfg), nil

	default:
		return nil, fmt.Errorf("unknown indexStorage.mode %q for job %d (expected %q, %q or %q)",
			cfg.Mode, jobID, IndexStorageModePVC, IndexStorageModeExistingClaim, IndexStorageModeNone)
	}
}

func newIndexVolume(claimName string, cfg IndexStorageConfig) *IndexVolume {
	return &IndexVolume{
		ClaimName:    claimName,
		MountPath:    cfg.MountPath,
		SubPath:      cfg.SubPath,
		CacheSizeMB:  cfg.CacheSizeMB,
		MaxOpenFiles: cfg.MaxOpenFiles,
	}
}

// checkExistingClaim verifies a user-supplied claim is usable before a pod is
// built around it. A pod referencing a claim that does not exist is not
// rejected - it waits to be scheduled until the activity times out, which is the
// hang this feature must not reintroduce.
func (k *KubernetesExecutor) checkExistingClaim(ctx context.Context, jobID int, claimName string) error {
	if claimName == "" {
		return fmt.Errorf("indexStorage.mode is %q for job %d but indexStorage.existingClaim is empty",
			IndexStorageModeExistingClaim, jobID)
	}

	_, err := k.client.CoreV1().PersistentVolumeClaims(k.namespace).Get(ctx, claimName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("indexStorage.existingClaim %q for job %d does not exist in namespace %s",
			claimName, jobID, k.namespace)
	}
	if err != nil {
		return indexClaimError("get", claimName, err)
	}
	return nil
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

// indexClaimError describes a failed claim operation, adding the fix when the
// worker simply is not allowed to perform it. Releases installed with
// `useStandardResources: false` keep their RBAC behind a pre-install hook that
// `helm upgrade` does not re-run, so an upgraded worker can be left without the
// persistentvolumeclaims verbs the chart now grants.
func indexClaimError(action, name string, err error) error {
	if apierrors.IsForbidden(err) {
		return fmt.Errorf("%w: failed to %s index PVC %s. The olake-workers Role is missing "+
			"persistentvolumeclaims permissions. Re-apply the chart RBAC with `helm upgrade` "+
			"(useStandardResources: true), or add [get, create, update] on persistentvolumeclaims "+
			"to the Role by hand", errIndexRBAC, action, name)
	}
	return fmt.Errorf("failed to %s index PVC %s: %s", action, name, err)
}

func accessModesToStrings(modes []corev1.PersistentVolumeAccessMode) []string {
	out := make([]string, 0, len(modes))
	for _, mode := range modes {
		out = append(out, string(mode))
	}
	return out
}

// normalizeAccessModes renders a set of access modes as a comparable,
// order-independent string.
func normalizeAccessModes(modes []string) string {
	sorted := slices.Clone(modes)
	slices.Sort(sorted)
	return strings.Join(sorted, ",")
}

// ensureIndexPVC creates the per-job claim on first use and is a no-op on every
// later run. The claim is never deleted here: the index is expensive to rebuild,
// so it outlives the pods that use it.
func (k *KubernetesExecutor) ensureIndexPVC(ctx context.Context, jobID int, cfg IndexStorageConfig) (string, error) {
	log := logger.Log(ctx)
	name := IndexPVCName(jobID)
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
		k.reconcileIndexPVC(ctx, existing, cfg, requested)
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
		"olake.io/job-id":              fmt.Sprintf("%d", jobID),
	}
	for key, val := range cfg.Labels {
		if _, reserved := labels[key]; !reserved {
			labels[key] = val
		}
	}

	annotations := make(map[string]string, len(cfg.Annotations)+1)
	for key, val := range cfg.Annotations {
		annotations[key] = val
	}
	annotations["olake.io/created-by-pod"] = k.config.WorkerIdentity

	claim := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   k.namespace,
			Labels:      labels,
			Annotations: annotations,
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

// reconcileIndexPVC applies the one change Kubernetes allows in place - growing
// the volume - and warns about changes it cannot apply. Nothing is deleted here:
// discarding a bound claim would discard the index with it.
func (k *KubernetesExecutor) reconcileIndexPVC(ctx context.Context, existing *corev1.PersistentVolumeClaim, cfg IndexStorageConfig, requested resource.Quantity) {
	log := logger.Log(ctx)

	if cfg.StorageClass != "" && existing.Spec.StorageClassName != nil && *existing.Spec.StorageClassName != cfg.StorageClass {
		log.Warn("index PVC storageClass differs from configuration and cannot be changed in place; keeping the existing volume",
			"pvcName", existing.Name, "existing", *existing.Spec.StorageClassName, "configured", cfg.StorageClass)
	}

	if len(cfg.AccessModes) > 0 {
		configured := normalizeAccessModes(cfg.AccessModes)
		current := normalizeAccessModes(accessModesToStrings(existing.Spec.AccessModes))
		if current != configured {
			log.Warn("index PVC accessModes differ from configuration and cannot be changed in place; keeping the existing volume",
				"pvcName", existing.Name, "existing", current, "configured", configured)
		}
	}

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
