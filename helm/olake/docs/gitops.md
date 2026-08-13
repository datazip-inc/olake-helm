# GitOps (Helm)

Manage OLake sources, destinations, jobs, and stream selections as `olake.io/v1` custom resources. The reconciler runs **inside olake-ui** when GitOps is enabled.

Operator source code and API docs live in [olake-ui](https://github.com/datazip-inc/olake-ui) (`server/internal/gitops/`, `docs/gitops/`).

## Enable in Helm

```yaml
gitops:
  enabled: true
  rbac:
    create: true

# TODO (QUESTION - remove before merge): will we have replicaCount > 1 ?
olakeUI:
  replicaCount: 1
```

```bash
helm upgrade --install olake ./helm/olake -f my-values.yaml
```

This sets `GITOPS_ENABLED=true` on the olake-ui Deployment and installs CRDs from `crds/` on first install.

### Requirements

- `global.jobServiceAccount.create: true` **or** `gitops.serviceAccount.create: true` so olake-ui has a ServiceAccount with GitOps RBAC.
- olake-ui image built with GitOps support (controller-runtime embedded).
- GitOps RBAC is **cluster-scoped** (`ClusterRole` on `olake.io` resources only). CRs may live in any namespace; the reconciler watches the whole cluster.

### CRD upgrades

Helm installs CRDs from `crds/` on `helm install` only. Edit `helm/olake/crds/` in this repo when the schema changes (keep aligned with olake-ui reconciler types in `server/internal/gitops/api/v1/`), then re-apply:

```bash
kubectl apply -f helm/olake/crds/
```

Status schema is `phase` / `message` / `entityId` / `observedGeneration` (no `conditions`). Argo CD health still uses `.status.phase` via [argocd-health.yaml](./argocd-health.yaml).

## Argo CD

1. Sync OLake Helm release (CRDs + `gitops.enabled: true`).
2. Merge [argocd-health.yaml](./argocd-health.yaml) into the cluster `argocd-cm` ConfigMap (once per cluster).
3. Apply example manifests from [examples/gitops/](./examples/gitops/). Do not use Argo CD sync waves — the Job reconciler waits for Source, Destination, and Streams. Job `spec.config` uses `source` and `destination` (OLake entity name or numeric ID); see [olake-ui docs/gitops](https://github.com/datazip-inc/olake-ui/blob/master/docs/gitops/README.md).

Health reads `.status.phase`: `Ready` → Healthy, `Failed` → Degraded, otherwise Progressing.

**Disable prune** for `olake.io` kinds until delete lifecycle is supported (v1 is create/update only).

## Example values overlay

```yaml
gitops:
  enabled: true

olakeUI:
  replicaCount: 1
  env:
    RUN_MODE: production

global:
  jobServiceAccount:
    create: true
  env:
    RUN_MODE: production
```

## Troubleshooting

| Issue | Check |
|-------|--------|
| Reconciler not starting | `kubectl logs deploy/olake-ui`, confirm `GITOPS_ENABLED=true` |
| Forbidden on CR watch | GitOps ClusterRoleBinding → olake-ui ServiceAccount |
| Job stuck Pending | Source/Destination/Streams not Ready yet, or Job `config.source`/`destination` name mismatch |
| Argo CD Degraded | `kubectl describe source …` → `.status.message` |

See [olake-ui docs/gitops](https://github.com/datazip-inc/olake-ui/blob/master/docs/gitops/README.md) for manifest shapes and lifecycle details.
