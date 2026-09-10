# OLake Fusion Helm Chart

Fusion is OLake's Iceberg table maintenance and compaction service, a fork of
[Apache Amoro](https://amoro.apache.org). This chart deploys Fusion and the
Spark-on-Kubernetes optimizer it schedules compaction onto.

Fusion used to ship inside the `olake` chart under a `fusion:` values block. It
is now its own chart so it can be installed, upgraded and scaled independently
of the OLake control plane.

## Components

| Component | What it is | How it is created |
|---|---|---|
| **Fusion server** | The AMS process — REST API, dashboard, table service, optimizing service | `Deployment` in this chart (always exactly one replica) |
| **Optimizer master** | The Spark **driver** pod. A lightweight, always-on orchestrator, one per optimizer | Submitted by the Fusion server; labelled `olake.io/workload=fusion-master` |
| **Optimizer workers** | The Spark **executor** pods. Short-lived and heavy — this is where compaction runs | Submitted by the optimizer master; labelled `olake.io/workload=fusion-executor` |

Only the Fusion server is a Kubernetes object this chart creates. Master and
worker pods are created at runtime by Fusion itself, which is why their
scheduling, image and Spark configuration are supplied through the Fusion
ConfigMap rather than through a Deployment.

On start-up the Fusion container registers an optimizer group
(`optimizer.group.name`, default `spark-container`) and requests
`optimizer.parallelism` optimizers through the REST API; on shutdown it releases
them again so no master or worker pods are orphaned. Set `bootstrap.enabled:
false` to manage optimizers by hand from the dashboard instead.

## Installing

Fusion is a dependency of the `olake` chart, so the usual way to run it is to
enable it there:

```bash
helm repo add olake https://datazip-inc.github.io/olake-helm
helm install olake olake/olake -n olake --set fusion.enabled=true
```

That wires everything up for you. Fusion reuses two things the `olake` release
already manages, rather than standing up its own copies:

- the PostgreSQL that chart deploys, handed over as an external database
- the ReadWriteMany PVC `olake-shared-storage`, used for Fusion and optimizer logs

OLake UI is pointed at Fusion automatically — no `releaseName` needed.

### Standalone install

Fusion does not require OLake. Installed on its own it is self-contained: it
deploys its own PostgreSQL, and `sharedStorage.create` brings up the shared
ReadWriteMany claim through its bundled `nfs-server` dependency.

```bash
helm install fusion olake/fusion -n fusion --set sharedStorage.create=true
```

To use a database you already run, turn off the bundled PostgreSQL:

```yaml
postgresql:
  enabled: false
  external:
    properties:
      host: pg.example.com
      port: 5432
      username: fusion
      password: change-me
      fusion_database: fusion

sharedStorage:
  enabled: false
```

Or supply a secret you already manage:

```yaml
postgresql:
  enabled: false
  external:
    existingSecret: my-fusion-postgres-secret
    secretKeys:
      host: PGHOST
      port: PGPORT
      username: PGUSER
      password: PGPASSWORD
      fusion_database: PGDATABASE
      ssl_mode: PGSSLMODE
```

When `external.existingSecret` is set, the JDBC URL is assembled at runtime from
the secret rather than baked into the ConfigMap.

### As a separate release alongside OLake

Fusion can still be installed as its own release next to an existing `olake`
release. Point OLake UI at it by naming the release in the olake values:

```yaml
# olake values
fusion:
  enabled: true
  releaseName: "fusion"   # must match the fusion release name
```

## Scheduling master and workers separately

The optimizer master is a small, always-on process; the workers are large and
bursty. Running both on the same node pool means keeping a big node around even
when no compaction is happening. Schedule them independently:

```yaml
optimizer:
  master:
    nodeSelector:
      node-type: standard
  worker:
    nodeSelector:
      node-type: compaction
    tolerations:
      - key: compaction
        operator: Exists
        effect: NoSchedule
```

Anything left empty falls back to the Fusion pod's own top-level
`nodeSelector` / `tolerations` / `affinity`.

The values are rendered into `master-pod-template.yaml` and
`worker-pod-template.yaml` in the Fusion ConfigMap and handed to Spark through
`spark.kubernetes.driver.podTemplateFile` and
`spark.kubernetes.executor.podTemplateFile`.

## Sizing the optimizer

```yaml
optimizer:
  parallelism: 3          # number of optimizer workers requested for the group
  spark:
    extraConfig:
      spark.driver.memory: "1g"
      spark.executor.memory: "4g"
      spark.executor.cores: "1"
```

`parallelism` also caps Fusion's planning parallelism
(`ams.optimizer.max-planning-parallelism`).

## Accessing the dashboard

```bash
kubectl port-forward -n olake svc/fusion-rest 1630:1630
```

Or expose it with an Ingress:

```yaml
ingress:
  enabled: true
  ingressClassName: nginx
  hostname: fusion.example.com
```

A Gateway API `HTTPRoute` is available as an alternative under `httpRoute`.

## Metrics

Fusion can expose Prometheus metrics through the metric reporter plugin:

```yaml
plugin:
  metricReporters:
    prometheusExporter:
      name: prometheus-exporter
      enabled: true
      properties:
        port: 7001
        metric-filter.includes: "table_optimizing_.*|optimizer_group_.*"
        metric-filter.excludes: "table_summary_.*"
      service:
        type: ClusterIP
        port: "7001"
        annotations:
          prometheus.io/scrape: "true"
```

The container port is opened automatically; the `service` block is optional and
creates a Service named `<release>-prometheus-exporter` for scraping.

## Private registries

```yaml
global:
  env:
    CONTAINER_REGISTRY_BASE: "my-registry.example.com"
  imagePullSecrets:
    - name: my-registry-secret
```

`CONTAINER_REGISTRY_BASE` prefixes every image this chart references (Fusion,
the Spark optimizer, and the PostgreSQL image used by the database init
container). The pull secrets are attached to the Fusion pod, to its
ServiceAccount, and passed to Spark as
`spark.kubernetes.container.image.pullSecrets` so master and worker pods can pull
too.

## Values

See [values.yaml](values.yaml) — every key is documented inline.

| Key | Default | Description |
|---|---|---|
| `image.repository` | `olakego/fusion` | Fusion server image |
| `postgresql.enabled` | `true` | Deploy a PostgreSQL as part of this chart. The olake chart sets this false and hands Fusion its own database instead |
| `postgresql.external.existingSecret` | `""` | Secret to read credentials from when `enabled` is false |
| `sharedStorage.create` | `false` | Provision the RWX claim through the bundled `nfs-server` dependency |
| `sharedStorage.existingClaim` | `olake-shared-storage` | RWX PVC for Fusion and optimizer logs, when not creating one |
| `auth.adminUser.username` / `.password` | `admin` / `password` | Fusion dashboard credentials |
| `optimizer.group.name` | `spark-container` | Optimizer group registered at bootstrap |
| `optimizer.parallelism` | `3` | Optimizer workers requested for the group |
| `optimizer.master.*` | `{}` | Scheduling for the Spark driver pod |
| `optimizer.worker.*` | `{}` | Scheduling for the Spark executor pods |
| `optimizer.spark.image.repository` | `olakego/fusion-spark` | Image for master and worker pods |
| `bootstrap.enabled` | `true` | Register/release optimizers via lifecycle hooks |
| `serviceAccount.rbac.create` | `true` | Create the Role/RoleBinding Fusion needs to run optimizer pods |

## Images

| Image | Registry |
|---|---|
| `olakego/fusion:latest` | Docker Hub |
| `olakego/fusion-spark:latest` | Docker Hub |
| `library/postgres:14-alpine` | Docker Hub |
