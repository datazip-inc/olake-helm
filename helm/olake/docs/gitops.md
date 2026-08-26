# GitOps (Helm)

Manage OLake sources, destinations, jobs, and stream selections as labelled **ConfigMaps**. The reconciler runs inside **olake-ui** when GitOps is enabled.

Operator source code and API docs live in [olake-ui](https://github.com/datazip-inc/olake-ui) (`server/internal/gitops/`, `docs/gitops/`).

## Enable in Helm

```yaml
gitops:
  enabled: true
  rbac:
    create: true

olakeUI:
  replicaCount: 1
```

```bash
helm upgrade --install olake ./helm/olake -f my-values.yaml
```

This sets `GITOPS_ENABLED=true` and `POD_NAMESPACE` on the olake-ui Deployment.

### Requirements

- `global.jobServiceAccount.create: true` **or** `gitops.serviceAccount.create: true` so olake-ui has a ServiceAccount with GitOps RBAC.
- olake-ui image built with GitOps support (controller-runtime embedded).
- GitOps RBAC is **namespace-scoped** (`Role` on `configmaps` and `events` in the release namespace). olake-ui does **not** receive `pods` create/delete — failure indicators are created by **olake-worker** via Temporal `IndicatorWorkflow`.

### Failure indicators

On reconcile failure, olake-ui starts `IndicatorWorkflow` on the worker task queue. The worker creates a short-lived busybox Pod (Kubernetes) or container (Docker Compose) that exits with the error in the termination log. Configure Pod-failure alerts on `olake.io/indicator=true`.

## Argo CD / Flux

1. Sync OLake Helm release with `gitops.enabled: true`.
2. Apply labelled ConfigMap manifests from [examples/gitops/](./examples/gitops/). Do not use sync waves — the Job reconciler waits for Source, Destination, and Streams.
3. **TODO:** Add tool-specific health checks (Argo CD Lua, Flux) keyed off `metadata.annotations.olake.io/phase` on managed ConfigMaps.

Status lives in annotations: `olake.io/phase`, `olake.io/message`, `olake.io/entity-id`, `olake.io/observed-hash`. Kubernetes Events are emitted with reason `Synced` or `SyncFailed`.

**Disable prune** for managed ConfigMaps until delete lifecycle is supported (v1 is create/update only).

## Example values overlay

```yaml
gitops:
  enabled: true

olakeUI:
  replicaCount: 1

global:
  jobServiceAccount:
    create: true
```

## Troubleshooting

| Issue | Check |
|-------|--------|
| Reconciler not starting | `kubectl logs deploy/olake-ui`, confirm `GITOPS_ENABLED=true` |
| Forbidden on ConfigMap watch | GitOps RoleBinding → olake-ui ServiceAccount |
| Job stuck Pending | Source/Destination/Streams not Ready yet, or name mismatch in job config |
| Failed but no indicator Pod | Worker logs, Temporal `IndicatorWorkflow` runs; worker RBAC includes `pods` create |
| Phase Failed | `kubectl describe configmap <name>` → `olake.io/message` annotation |

See [olake-ui docs/gitops](https://github.com/datazip-inc/olake-ui/blob/master/docs/gitops/README.md) for manifest shapes and lifecycle details.
