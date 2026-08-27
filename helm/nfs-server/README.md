# nfs-server

Shared storage for the OLake charts: an in-cluster NFS server with dynamic
provisioning, plus the ReadWriteMany claim that OLake workers and Fusion mount
for logs and connector state.

This chart is not meant to be installed on its own. It is a dependency of both
[`olake`](../olake/README.md) and [`fusion`](../fusion/README.md), aliased to
`nfsServer` in each, so it is configured through `nfsServer.*` in whichever
chart you install:

```yaml
nfsServer:
  enabled: true
  persistence:
    size: 20Gi
```

Set `nfsServer.enabled: false` to skip the NFS server and provision the shared
claim from a ReadWriteMany StorageClass you already have (AWS EFS, Azure Files,
GCP Filestore, Longhorn, Rook/CephFS). `nfsServer.external.storageClass` is
required in that case. The claim is created either way — that is why this chart
always renders.

## Resource names

Resource names are built from `nameOverride` (default `olake`) rather than this
chart's own name, so they match what the `olake` chart created before this chart
was split out of it. The StatefulSet, Service, ServiceAccount, ClusterRole,
ClusterRoleBinding and StorageClass all carry `helm.sh/resource-policy: keep`;
renaming them would orphan the live objects instead of upgrading them. The
`fusion` chart overrides `nameOverride` to `fusion` and sets its own
`claimName` and `storageClass.name` so a standalone Fusion install does not
collide with an `olake` install on the same cluster.

## Caveats

The bundled NFS server runs as a single pod: convenient for development and
quick starts, a single point of failure in production. It is also incompatible
with Bottlerocket OS worker nodes on AWS EKS — use EFS there via
`nfsServer.enabled: false`.

See [values.yaml](values.yaml) — every key is documented inline.
