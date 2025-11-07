# OLake Uninstallation Guide

This guide provides detailed instructions for uninstalling the OLake Helm chart and cleaning up all associated resources.

## Table of Contents

- [Quick Uninstall](#quick-uninstall)
- [Complete Uninstall Script](#complete-uninstall-script)
- [Manual Cleanup](#manual-cleanup)
- [Troubleshooting](#troubleshooting)

## Quick Uninstall

For a basic uninstallation that removes the Helm release but preserves some resources:

```bash
# Uninstall the release
helm uninstall olake -n olake

# Delete the namespace
kubectl delete namespace olake
```

⚠️ **Note**: This method leaves behind resources protected by the `helm.sh/resource-policy: keep` annotation:
- NFS Server components (StatefulSet, Service, ClusterRole, etc.)
- PersistentVolumeClaims
- StorageClass

## Complete Uninstall Script

The recommended way to completely remove OLake is to use the provided uninstallation script.

### Basic Usage

```bash
cd helm/olake
./uninstall.sh
```

This will remove **everything** including protected resources.

### Script Options

| Option | Description | Default |
|--------|-------------|---------|
| `-n, --namespace` | Namespace where OLake is installed | `olake` |
| `-r, --release` | Helm release name | `olake` |
| `--force` | Force delete stuck resources | `false` |
| `--keep-pvcs` | Keep PersistentVolumeClaims | `false` |
| `--keep-namespace` | Keep namespace after cleanup | `false` |
| `-h, --help` | Show help message | - |

### Usage Examples

#### 1. Complete Removal (Default)
```bash
./uninstall.sh
```
Removes everything including data volumes.

#### 2. Preserve Data Volumes
```bash
./uninstall.sh --keep-pvcs
```
Removes OLake but keeps PVCs for data preservation. Useful if you plan to reinstall later.

#### 3. Custom Namespace/Release
```bash
./uninstall.sh -n my-namespace -r my-release
```
Uninstall from a custom namespace with a custom release name.

#### 4. Force Cleanup (Stuck Resources)
```bash
./uninstall.sh --force
```
Forcefully removes resources that are stuck in terminating state.

#### 5. Keep Namespace
```bash
./uninstall.sh --keep-namespace
```
Removes all OLake resources but preserves the namespace.

### What the Script Does

The uninstallation script performs the following steps in order:

1. **Uninstalls Helm Release**: Removes the primary Helm installation
2. **DevSpace Cleanup**: Removes any DevSpace development deployments
3. **Pods**: Deletes all remaining pods
4. **NFS Server**: Removes StatefulSet, Service, and ServiceAccount
5. **PVCs**: Deletes PersistentVolumeClaims (unless `--keep-pvcs` is specified)
6. **PVs**: Cleans up orphaned PersistentVolumes in "Released" state
7. **StorageClass**: Removes the NFS StorageClass
8. **ClusterRole**: Deletes the NFS Server ClusterRole
9. **ClusterRoleBinding**: Removes the NFS Server ClusterRoleBinding
10. **Remaining Resources**: Cleans up ConfigMaps, Secrets, Services, etc.
11. **Namespace**: Deletes the namespace (unless `--keep-namespace` is specified)

## Manual Cleanup

If you prefer to clean up resources manually or the script doesn't work for some reason, follow these steps:

### 1. Uninstall Helm Release
```bash
helm uninstall olake -n olake
```

### 2. Delete DevSpace Resources (if any)
```bash
kubectl delete deployment -n olake -l devspace.sh/replaced=true
```

### 3. Delete Remaining Pods
```bash
kubectl delete pods --all -n olake
```

### 4. Remove NFS Server Components
```bash
# StatefulSet
kubectl delete statefulset -n olake -l app.kubernetes.io/component=nfs-server

# Service
kubectl delete service -n olake -l app.kubernetes.io/component=nfs-server

# ServiceAccount
kubectl delete serviceaccount -n olake -l app.kubernetes.io/component=nfs-server
```

### 5. Delete PersistentVolumeClaims
```bash
kubectl delete pvc --all -n olake
```

### 6. Clean Up Orphaned PersistentVolumes
```bash
# List released PVs
kubectl get pv | grep Released | grep olake

# Delete specific PVs
kubectl delete pv <pv-name>
```

### 7. Remove NFS StorageClass
```bash
kubectl delete storageclass nfs-server
```

### 8. Delete ClusterRole and ClusterRoleBinding
```bash
kubectl delete clusterrole olake-nfs-server
kubectl delete clusterrolebinding olake-nfs-server
```

### 9. Delete Namespace
```bash
kubectl delete namespace olake
```

## Troubleshooting

### Namespace Stuck in "Terminating"

If the namespace is stuck in "Terminating" state:

```bash
# Check what resources are preventing deletion
kubectl api-resources --verbs=list --namespaced -o name | \
  xargs -n 1 kubectl get --show-kind --ignore-not-found -n olake

# Force finalize the namespace (use with caution)
kubectl get namespace olake -o json | \
  jq '.spec.finalizers = []' | \
  kubectl replace --raw "/api/v1/namespaces/olake/finalize" -f -
```

Or use the script with `--force`:
```bash
./uninstall.sh --force
```

### PVC Stuck in "Terminating"

If PVCs are stuck:

```bash
# Remove finalizers from PVC
kubectl patch pvc <pvc-name> -n olake -p '{"metadata":{"finalizers":null}}'

# Or use the force flag
./uninstall.sh --force
```

### PV in "Released" State

Released PVs won't be automatically deleted:

```bash
# List released PVs
kubectl get pv | grep Released

# Delete them manually
kubectl delete pv <pv-name>

# Or use the uninstall script which handles this automatically
./uninstall.sh
```

### ClusterRole/ClusterRoleBinding Not Deleted

These are cluster-wide resources:

```bash
# Check if they exist
kubectl get clusterrole | grep olake
kubectl get clusterrolebinding | grep olake

# Delete them
kubectl delete clusterrole olake-nfs-server
kubectl delete clusterrolebinding olake-nfs-server
```

### StorageClass Not Deleted

```bash
# Check if it exists
kubectl get storageclass nfs-server

# Delete it
kubectl delete storageclass nfs-server
```

## Data Preservation

If you want to preserve your data for future reinstallation:

1. **Keep PVCs**:
   ```bash
   ./uninstall.sh --keep-pvcs
   ```

2. **Backup PVC Data** (before uninstalling):
   ```bash
   # For PostgreSQL data
   kubectl exec -n olake postgresql-0 -- pg_dump -U temporal > backup.sql
   
   # For shared storage
   kubectl cp olake/olake-nfs-server-0:/export ./backup-storage
   ```

3. **Restore After Reinstall**:
   After reinstalling, restore the data back to the new PVCs.

## Post-Uninstall Verification

Verify that all resources have been removed:

```bash
# Check namespace (should not exist)
kubectl get namespace olake

# Check PVs (should not see any olake-related PVs)
kubectl get pv | grep olake

# Check StorageClass
kubectl get storageclass nfs-server

# Check ClusterRole/Binding
kubectl get clusterrole | grep olake
kubectl get clusterrolebinding | grep olake
```

## Reinstallation

After uninstallation, you can reinstall OLake:

```bash
helm install olake ./helm/olake -n olake --create-namespace
```

If you kept PVCs during uninstall, the new installation will try to use them.

## Support

For issues or questions:
- [GitHub Issues](https://github.com/datazip-inc/olake-ui/issues)
- [Slack Community](https://join.slack.com/t/getolake/shared_invite/zt-2utw44do6-g4XuKKeqBghBMy2~LcJ4ag)
- [Documentation](https://olake.io/docs)

