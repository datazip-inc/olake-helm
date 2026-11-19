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

This sets `OLAKE_GITOPS_MODE=kubernetes` and `POD_NAMESPACE` on the olake-ui Deployment.

### Requirements

- `global.jobServiceAccount.create: true` **or** `gitops.serviceAccount.create: true` so olake-ui has a ServiceAccount with GitOps RBAC.
  To use an existing ServiceAccount instead, set `gitops.serviceAccount.create: false` and `gitops.serviceAccount.name: <existing-sa>`.
  The GitOps Role is bound to it, and the chart does not create it.
- `olakeUI.replicaCount: 1`. The reconciler has no leader election, so the chart refuses more replicas.
- olake-ui image built with GitOps support (controller-runtime embedded).
- GitOps RBAC is **namespace-scoped** (`Role` on `configmaps`, `secrets`, and `events` in the release namespace). olake-ui does **not** receive `pods` create/delete - failure indicators are created by **olake-worker** via Temporal `IndicatorWorkflow`.
- `${secrets:<name>#<key>}` references in a config read only Secrets in the release namespace labelled `olake.io/managed=true`, through the same `secrets` grant.
  That label limit is enforced by olake-ui's code, not by RBAC: the Role grants get, list, watch, update and patch on every Secret in the namespace.
  Deploy OLake in a dedicated namespace if other Secrets live there.

### Failure indicators

On reconcile failure, olake-ui starts `IndicatorWorkflow` on the worker task queue.
The worker creates a Pod (Kubernetes) or container (Docker Compose) named `olake-failed-<kind>-<name>`, for example `olake-failed-source-pg`.
It runs the worker's own image, so no extra image is needed, including in offline environments.
The Pod prints the error to its termination log and exits, so it shows as `Error`; the container prints the error to its logs and keeps running.
On Kubernetes the worker also records a `Warning` event with reason `SyncFailed` on the Pod, so `kubectl describe pod` and event-based alerting show the error.
The indicator is deleted when the resource syncs again.
Delete is not supported yet (see below), so if a failed resource is removed from git, its `olake-failed-*` indicator stays until you delete it by hand.
Alert on the `olake.io/indicator=true` label, or on `SyncFailed` events.

Keep managed ConfigMap and Secret names at 63 characters or fewer, and distinct within their first 38 characters.
The Pod's `olake.io/resource` label holds the full name, and Kubernetes rejects a label value over 63 characters, so a longer name gets no indicator (its status is still `Failed`).
The `<name>` part is cut to fit 63 characters.
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
| Reconciler not starting | `kubectl logs deploy/olake-ui`, confirm `OLAKE_GITOPS_MODE=kubernetes` |
| Forbidden on ConfigMap watch | GitOps RoleBinding → olake-ui ServiceAccount |
| Job stuck Pending | Source/Destination/Streams not Ready yet, or name mismatch in job config |
| Failed but no indicator Pod | Worker logs (`read worker image failed`, `spawn indicator pod failed`), Temporal `IndicatorWorkflow` runs; worker RBAC includes `pods` get and create; the object name is at most 63 characters |
| Indicator Pod but no `SyncFailed` event | Worker logs (`record indicator pod event failed`); worker RBAC includes `events` create |
| Phase Failed | `kubectl describe configmap <name>` → `olake.io/message` annotation |

See [olake-ui docs/gitops](https://github.com/datazip-inc/olake-ui/blob/master/docs/gitops/README.md) for manifest shapes and lifecycle details.
