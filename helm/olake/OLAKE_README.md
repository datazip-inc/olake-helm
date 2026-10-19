

<h1 align="center" style="border-bottom: none">
    <a href="https://datazip.io/olake" target="_blank">
        <img alt="olake" src="https://github.com/user-attachments/assets/d204f25f-5289-423c-b3f2-44b2194bdeaf" width="100" height="100"/>
    </a>
    <br>OLake
</h1>

# OLake Helm Chart

![Version: 0.0.30](https://img.shields.io/badge/Version-0.0.30-informational?style=flat-square)  ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square)  ![AppVersion: 0.3.11](https://img.shields.io/badge/AppVersion-0.3.11-informational?style=flat-square)

A Helm chart for OLake UI - Fastest open-source tool for replicating Databases to Apache Iceberg or Data Lakehouse

**Homepage:** <https://github.com/datazip-inc/olake-helm>

## Maintainers

| Name | Email | Url |
| ---- | ------ | --- |
| Team OLake | <hello@olake.io> |  |

## Source Code

* <https://github.com/datazip-inc/olake-helm>

> **Note:** This document provides a categorized reference of all configuration values for the OLake Helm Chart. For general architecture, prerequisites, installation, and deployment guides, please refer to the main [README.md](./README.md).

---

## Table of Contents

- [Global & Common Configurations](#global--common-configurations)
- [OLake UI Configurations](#olake-ui-configurations)
- [OLake Worker & Job Execution](#olake-worker--job-execution)
- [Storage Configurations (S3 / MinIO / NFS)](#storage-configurations-s3--minio--nfs)
- [Core Infrastructure (Temporal & PostgreSQL)](#core-infrastructure-temporal--postgresql)
- [Optional Add-ons (Fusion & Elasticsearch)](#optional-add-ons-fusion--elasticsearch)

---

## Global & Common Configurations

Configurations that apply globally across all components, or manage standard Kubernetes resource behaviors.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `fullnameOverride` | `string` | `""` | Provide a name to substitute for the full names of resources |
| `global` | `object` | `{"env":{},"imagePullSecrets":[],"jobMapping":{},"jobProfiles":{},"jobServiceAccount":{"annotations":{},"create":true,"name":""},"logFileStorageMode":"nfs","podAnnotations":{},"storageClass":""}` | Global configuration that applies to all components |
| `global.env` | `object` | `{}` | Global environment variables that apply to all OLake components These variables are inherited by all services (olakeUI, olakeWorker) Individual component env sections can override these values if needed  OLAKE_SECRET_KEY: Used for encrypting job metadata and sensitive data   - Leave empty or unset to disable encryption   - Set to a secure random string. Example: "LewgLVrgCZMadJverZdn"   - AWS KMS Key ARN can also be used. Example: "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"  RUN_MODE: Sets the deployment environment mode   - "development": Local development with debug features   - "staging": Pre-production testing environment   - "production": Production deployment with optimizations  Example configuration:   env:     OLAKE_SECRET_KEY: "your-secure-encryption-key-here"     RUN_MODE: "production" |
| `global.imagePullSecrets` | `list` | `[]` | Global image pull secrets for private container registries List of Kubernetes secret names containing registry credentials. Applied to all long-running Deployments AND to the connector job pods spawned by olake-workers (via the job ServiceAccount), so private connector images (Harbor, Nexus, Quay, etc.) can be pulled during test/discover/sync. To cover connector job pods, also set `global.jobServiceAccount.create: true`. NOTE: Secrets must be created separately using kubectl or other means Examples:   Create secret: kubectl create secret docker-registry my-registry-secret \     --docker-server=my-registry.com --docker-username=user --docker-password=pass   Reference in values:   imagePullSecrets:     - name: "my-registry-secret"     - name: "docker-hub-secret" |
| `global.jobMapping` | `object` | `{}` | [DEPRECATED] JobID-based node mapping configuration Use `global.jobProfiles` instead. This field will be removed in a future release. Maps JobID (integer) to specific node labels for pod scheduling (NodeSelector only). |
| `global.jobProfiles` | `object` | `{}` | JobID-based scheduling profiles for pods created by olake-workers Supports full Kubernetes scheduling capabilities (NodeSelector, Tolerations, Affinity) and optional compute resources (Resources). Overrides `jobMapping` if both are defined for the same JobID.  Special Key "0": Defines the DEFAULT profile for all short-lived jobs (test, discover, spec) and any Sync jobs not mapped to a specific profile.  Example:   jobProfiles:     0:    # Default Profile       nodeSelector:         node-type: "standard"       tolerations:         - key: "spot"           operator: "Exists"           effect: "NoSchedule"     123:                 # Specific Job Profile       affinity:         nodeAffinity:           requiredDuringSchedulingIgnoredDuringExecution:             nodeSelectorTerms:               - matchExpressions:                 - key: "gpu"                   operator: "In"                   values: ["true"]  Resources (optional): CPU/memory for the connector pod, in the standard Kubernetes shape. Only applied when set; otherwise pods keep the built-in default (requests: memory=256Mi, cpu=100m; no limits).   - Sync / clear-destination use the JobID's profile, then profile "0", then the built-in default.   - Test / discover / spec always use profile "0", then the built-in default.   - A profile's resources replace the fallback as a whole; fields are not merged.   - Setting only a limit makes Kubernetes use the same value as the request. Example:   jobProfiles:     0:       resources:         requests:           cpu: "500m"           memory: "1Gi"         limits:           memory: "2Gi"     123:       resources:         requests:           cpu: "2"           memory: "8Gi"         limits:           memory: "8Gi" |
| `global.jobServiceAccount` | `object` | `{"annotations":{},"create":true,"name":""}` | Service account configuration for job pods created by olake-workers Used for cloud provider IAM integration (AWS IRSA, GCP Workload Identity, Azure Workload Identity) |
| `global.logFileStorageMode` | `string` | `"nfs"` | Local storage mode for job config and log files   - "s3": S3-compatible storage (configure s3LogFileStorage below)   - "nfs": NFS shared volume (DEPRECATED — currently default, will be removed end of 2026; switch to "s3") |
| `global.podAnnotations` | `object` | `{}` | Global pod annotations applied to all Deployment pods and to activity pods (connector Job pods spawned by olake-workers) and Fusion Spark pods. Useful for service mesh sidecar injection (Istio, Linkerd) or other admission webhooks. Examples:   podAnnotations:     sidecar.istio.io/inject: "true"     linkerd.io/inject: enabled |
| `global.storageClass` | `string` | `""` | Global storage class for all persistent volumes Set to "" to use default storage class Examples: gp2, standard, managed |
| `nameOverride` | `string` | `""` | Provide a name in place of olake for `app:` labels |
| `useStandardResources` | `bool` | `true` | Deploy resources (RBAC, ServiceAccount, Secret) as standard resources instead of hooks. Defaults to true for robust resource management and ArgoCD compatibility. WARNING: If upgrading from an older version, either: 1. Set this to 'false' to maintain legacy hook behavior. 2. Or manually remove 'helm.sh/hook' annotations from existing resources before upgrading to allow Helm to adopt them. |

---

## OLake UI Configurations

Settings for the OLake UI frontend dashboard and backend API server, including replicas, ingress, authentication, and service settings.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `olakeUI.affinity` | `object` | `{}` | Affinity settings for pod assignment |
| `olakeUI.env` | `object` | `{}` | Environment variables for OLake UI Add custom environment variables here Example:   env:     CUSTOM_VAR: "value"     FEATURE_FLAG: "true" |
| `olakeUI.image` | `object` | `{"pullPolicy":"Always","repository":"olakego/ui","tag":"latest"}` | OLake UI image configuration |
| `olakeUI.ingress` | `object` | `{"annotations":{},"className":"","enabled":false,"hosts":[],"tls":[]}` | Ingress configuration for external access |
| `olakeUI.ingress.annotations` | `object` | `{}` | Ingress annotations Add custom annotations for your ingress controller here. Examples:   nginx.ingress.kubernetes.io/rewrite-target: /   kubernetes.io/ingress.class: nginx   kubernetes.io/tls-acme: "true"   cert-manager.io/cluster-issuer: "letsencrypt-prod" |
| `olakeUI.ingress.className` | `string` | `""` | Ingress class name Examples: nginx, traefik, alb |
| `olakeUI.ingress.enabled` | `bool` | `false` | Enable ingress |
| `olakeUI.ingress.hosts` | `list` | `[]` | Ingress hosts configuration List of host definitions for ingress. Each host can have multiple paths. Examples:   hosts:     - host: olake.local       paths:         - path: /           pathType: Prefix     - host: olake.example.com       paths:         - path: /           pathType: Prefix |
| `olakeUI.ingress.tls` | `list` | `[]` | TLS configuration Examples:   tls:     - secretName: olake-tls       hosts:         - olake.example.com |
| `olakeUI.initUser` | `object` | `{"adminUser":{"email":"admin@example.com","password":"password","username":"admin"},"enabled":true,"existingSecret":"","secretKeys":{"email":"email","password":"password","username":"username"}}` | Initialization user configuration |
| `olakeUI.initUser.adminUser` | `object` | `{"email":"admin@example.com","password":"password","username":"admin"}` | Credentials for the initial admin user. These values are used only if `existingSecret` is not provided. When Fusion is enabled, the same credentials are used for the Fusion UI and the Fusion optimizer bootstrap job. |
| `olakeUI.initUser.adminUser.email` | `string` | `"admin@example.com"` | Admin email address |
| `olakeUI.initUser.adminUser.password` | `string` | `"password"` | Admin password (change this in production!) |
| `olakeUI.initUser.adminUser.username` | `string` | `"admin"` | Admin username |
| `olakeUI.initUser.existingSecret` | `string` | `""` | Use an existing Kubernetes secret for the admin user credentials. If you provide a secret name, the job will use it instead of the direct values below. The secret must contain keys for username, password, and email. Example: existingSecret: "my-olake-admin-secret" |
| `olakeUI.initUser.secretKeys` | `object` | `{"email":"email","password":"password","username":"username"}` | Map the keys from your existing secret to the required fields. This is required only if `existingSecret` is set. |
| `olakeUI.nodeSelector` | `object` | `{}` | Pod scheduling constraints |
| `olakeUI.podAnnotations` | `object` | `{}` | Additional pod annotations |
| `olakeUI.podLabels` | `object` | `{}` | Additional pod labels |
| `olakeUI.replicaCount` | `int` | `1` | Number of OLake UI replicas |
| `olakeUI.resources` | `object` | `{}` | OLake UI resource requirements (optional overrides) Defaults: requests: memory=512Mi, cpu=500m (no limits by default) Example customizations:   resources:     requests:       memory: "1Gi"       cpu: "750m"     limits:       memory: "2Gi"       cpu: "1500m" |
| `olakeUI.securityContext` | `object` | `{}` | Olake UI security context Example customizations: securityContext:   runAsNonRoot: true   runAsUser: 1000   runAsGroup: 1000   fsGroup: 1000 |
| `olakeUI.service` | `object` | `{"annotations":{},"nodePort":"","type":"ClusterIP"}` | OLake UI service configuration For external access, change type to NodePort or LoadBalancer |
| `olakeUI.service.type` | `string` | `"ClusterIP"` | Service type (ClusterIP for internal, NodePort/LoadBalancer for external access) |
| `olakeUI.tolerations` | `list` | `[]` | Tolerations for pod assignment |

---

## OLake Worker & Job Execution

Configurations for the Temporal worker service that executes ingestion jobs, pod scheduling profiles, and per-job Pebble index storage volumes.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `jobIndexes` | `object` | `{}` | Per-job index volume for the Pebble index used by the Iceberg positional-delete / deletion-vector write path. `default` holds the settings every job starts from; entries under `jobs` are deep-merged over it field by field (labels and annotations merge per key, every other field is replaced), so set only what differs.  A job only gets a volume when its `index_required` is true, `index_required` is true when any of stream in job has positional-delete or deletion-vector enabled.  whatever is configured here. The claim is `olake-index-<JobID>` and outlives the pods that use it, so the index survives between runs.  Unset fields fall back to the worker defaults: 20Gi, cluster default StorageClass, /var/lib/olake/index, ReadWriteOnce, cacheSizeMB 512, maxOpenFiles 1000. Sizing, disk type, AZ pinning and attachment limits: see README, "Per-Job Index Storage".    jobIndexes:     default:                          # every job starts from this       size: 50Gi                      # grow-only; needs allowVolumeExpansion on the StorageClass. Can be increased safely without loosing volume/index       storageClass: "gp3"             # block storage only, never NFS/EFS; "" uses global.storageClass       accessModes: [ReadWriteOnce]    # block storage is single-attach       mountPath: /var/lib/olake/index       cacheSizeMB: 512                # Pebble block cache, PER STREAM       maxOpenFiles: 1000              # SSTable file descriptors, PER STREAM       labels: {}                      # extra labels on the PVC       annotations: {}                 # extra annotations on the PVC, for backup tooling     jobs:       123:                            # one job; unset fields inherit default         size: 100Gi         existingClaim: ""             # mount this PVC instead of provisioning one. Per-JobID                                       # only, never under `default`. size/storageClass/                                       # accessModes/labels/annotations are ignored for this job |
| `olakeWorker.affinity` | `object` | `{}` | Affinity settings for pod assignment |
| `olakeWorker.config` | `object` | `{"logging":{"format":"console","level":"info"}}` | OLake Worker application configuration |
| `olakeWorker.config.logging` | `object` | `{"format":"console","level":"info"}` | Logging configuration |
| `olakeWorker.env` | `object` | `{}` | Environment variables for OLake Worker Add custom environment variables here Example:   env:     CUSTOM_VAR: "value"     FEATURE_FLAG: "true" |
| `olakeWorker.image` | `object` | `{"pullPolicy":"Always","repository":"olakego/ui-worker","tag":"latest"}` | OLake Worker image configuration |
| `olakeWorker.nodeSelector` | `object` | `{}` | Pod scheduling constraints |
| `olakeWorker.podAnnotations` | `object` | `{}` | Additional pod annotations |
| `olakeWorker.podLabels` | `object` | `{}` | Additional pod labels |
| `olakeWorker.resources` | `object` | `{}` | OLake Worker resource requirements (optional overrides) Sizes the worker controller pod only. For sync pods, use `global.jobProfiles.<JobID>.resources`. Defaults: requests: memory=256Mi, cpu=250m (no limits by default) Example customizations:   resources:     requests:       memory: "512Mi"       cpu: "500m"     limits:       memory: "1Gi"       cpu: "1000m" |
| `olakeWorker.securityContext` | `object` | `{}` | Olake Worker security context Example customizations: securityContext:   runAsNonRoot: true   runAsUser: 1000   runAsGroup: 1000   fsGroup: 1000 |
| `olakeWorker.serviceAccount` | `object` | `{"annotations":{},"name":""}` | Service Account configuration for Kubernetes API access |
| `olakeWorker.serviceAccount.annotations` | `object` | `{}` | Service account annotations |
| `olakeWorker.serviceAccount.name` | `string` | `""` | Service account name If not set, a name is generated using the fullname template |
| `olakeWorker.tolerations` | `list` | `[]` | Tolerations for pod assignment |

---

## Storage Configurations (S3 / MinIO / NFS)

Settings for shared file and log storage between the worker and connector pods. Supports both S3-compatible object storage (MinIO or AWS S3) and shared NFS.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `nfsServer.affinity` | `object` | `{}` | Affinity settings for pod assignment |
| `nfsServer.enabled` | `bool` | `true` | Enable NFS server deployment with dynamic provisioning IMPORTANT: NFS components are created once and persist across helm operations. They will NOT be deleted on 'helm uninstall' or modified on 'helm upgrade'. This ensures data persistence but requires manual management for updates. |
| `nfsServer.external` | `object` | `{"deletePVCOnUninstall":false,"name":"","size":"20Gi","storageClass":""}` | External storage configuration (used when enabled: false) When disabled, you must provide an existing ReadWriteMany (RWX) supported StorageClass  CLOUD PROVIDER GUIDES FOR RWX STORAGECLASS:  AWS EFS: Use the Amazon EFS CSI driver with 'efs.csi.aws.com' provisioner See: https://github.com/kubernetes-sigs/aws-efs-csi-driver  Azure Files: Use 'azurefile' StorageClass with 'kubernetes.io/azure-file' provisioner See: https://docs.microsoft.com/en-us/azure/aks/azure-files-csi  GCP Filestore: Use Google Cloud Filestore CSI driver with 'filestore.csi.storage.gke.io' provisioner See: https://cloud.google.com/filestore/docs/csi-driver |
| `nfsServer.external.deletePVCOnUninstall` | `bool` | `false` | Delete the external PVC on helm uninstall |
| `nfsServer.external.name` | `string` | `""` | Name for the external PVC If not provided, defaults to "olake-shared-storage" Use this to reference an existing PVC or customize the name |
| `nfsServer.external.size` | `string` | `"20Gi"` | Size for the dynamically created external PVC |
| `nfsServer.external.storageClass` | `string` | `""` | StorageClass for dynamic RWX PVC creation Required when nfsServer.enabled is false Examples: "efs-sc", "azurefile", "filestore-csi", "longhorn", "rook-cephfs" |
| `nfsServer.nodeSelector` | `object` | `{}` | Pod scheduling constraints |
| `nfsServer.persistence` | `object` | `{"size":"20Gi"}` | Persistence configuration for NFS server backing storage |
| `nfsServer.persistence.size` | `string` | `"20Gi"` | Size of backing storage for NFS server |
| `nfsServer.podAnnotations` | `object` | `{}` | Additional pod annotations |
| `nfsServer.podLabels` | `object` | `{}` | Additional pod labels |
| `nfsServer.resources` | `object` | `{}` | NFS Server resource requirements (optional overrides) Defaults: requests: memory=256Mi, cpu=250m ; limits: memory=512Mi, cpu=500m Example customizations: resources:   requests:     memory: "1Gi"     cpu: "750m"   limits:     memory: "2Gi"     cpu: "1500m" |
| `nfsServer.storageClass` | `object` | `{"name":"nfs-server"}` | StorageClass configuration for dynamic provisioning |
| `nfsServer.storageClass.name` | `string` | `"nfs-server"` | Name of the StorageClass created by NFS server |
| `nfsServer.tolerations` | `list` | `[]` | Tolerations for pod assignment |
| `s3LogFileStorage.bucket` | `string` | `"olake"` | Settings shared by MinIO and external S3 |
| `s3LogFileStorage.external` | `object` | `{"endpoint":"","existingSecret":"","role":{"annotations":{},"enabled":false}}` | External S3 (AWS S3, GCS S3 API, external MinIO). Used only when minio.enabled is false. Credentials: set either existingSecret or role.enabled. |
| `s3LogFileStorage.external.endpoint` | `string` | `""` | S3-compatible endpoint. Leave empty for AWS S3. Set for anything else, e.g. "https://storage.googleapis.com" or "http://minio.example.com:9000". |
| `s3LogFileStorage.external.existingSecret` | `string` | `""` | Secret with static credentials. Not needed when role.enabled is true. For GCS, use HMAC keys. kubectl create secret generic s3-log-file-storage-secret \   --from-literal=OLAKE_S3_ACCESS_KEY_ID=<your-access-key-id> \   --from-literal=OLAKE_S3_SECRET_ACCESS_KEY=<your-secret-access-key> |
| `s3LogFileStorage.external.role` | `object` | `{"annotations":{},"enabled":false}` | IAM role auth for AWS S3 (EKS IRSA or EKS Pod Identity) instead of a secret. |
| `s3LogFileStorage.external.role.annotations` | `object` | `{}` | IRSA only; leave empty for EKS Pod Identity. Added to the worker and job service accounts. Example:   eks.amazonaws.com/role-arn: "arn:aws:iam::123456789012:role/olake-s3-log-storage-role" |
| `s3LogFileStorage.minio` | `object` | `{"affinity":{},"auth":{"rootPassword":"password","rootUser":"olake"},"enabled":true,"image":{"pullPolicy":"IfNotPresent","repository":"olakego/minio","tag":"RELEASE.2025-04-03T14-56-28Z"},"nodeSelector":{},"persistence":{"size":"20Gi"},"podAnnotations":{},"podLabels":{},"resources":{},"securityContext":{},"tolerations":[]}` | In-cluster MinIO. Runs as a single pod, so it is a single point of failure. Not recommended for production; use `external` instead. Endpoint and credentials secret are managed by the chart. |
| `s3LogFileStorage.minio.affinity` | `object` | `{}` | Affinity settings for pod assignment |
| `s3LogFileStorage.minio.auth.rootUser` | `string` | `"olake"` | MinIO root credentials, also used by OLake as OLAKE_S3_* keys. Override rootPassword for any non-local environment. |
| `s3LogFileStorage.minio.nodeSelector` | `object` | `{}` | Pod scheduling constraints |
| `s3LogFileStorage.minio.persistence.size` | `string` | `"20Gi"` | Backing volume size (uses global.storageClass when set) |
| `s3LogFileStorage.minio.podAnnotations` | `object` | `{}` | Additional pod annotations |
| `s3LogFileStorage.minio.podLabels` | `object` | `{}` | Additional pod labels |
| `s3LogFileStorage.minio.resources` | `object` | `{}` | MinIO resource requirements (optional overrides) Defaults: requests: cpu=250m, memory=256Mi ; limits: cpu=500m, memory=512Mi |
| `s3LogFileStorage.minio.securityContext` | `object` | `{}` | MinIO security context Example customizations: securityContext:   runAsNonRoot: true   runAsUser: 1000   runAsGroup: 1000   fsGroup: 1000 |
| `s3LogFileStorage.minio.tolerations` | `list` | `[]` | Tolerations for pod assignment |
| `s3LogFileStorage.prefix` | `string` | `""` | Optional key prefix inside the bucket |
| `s3LogFileStorage.region` | `string` | `"us-east-1"` | Required by OLake. MinIO accepts any value; for AWS S3 use the bucket's region. |

---

## Core Infrastructure (Temporal & PostgreSQL)

Configurations for the underlying workflow orchestration engine (Temporal) and metadata persistence database (PostgreSQL).

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `postgresql.affinity` | `object` | `{}` | Affinity settings for pod assignment |
| `postgresql.auth` | `object` | `{"postgresPassword":"temporal","postgresUser":"temporal"}` | Authentication configuration |
| `postgresql.auth.postgresPassword` | `string` | `"temporal"` | PostgreSQL super user password |
| `postgresql.auth.postgresUser` | `string` | `"temporal"` | PostgreSQL super user name |
| `postgresql.enabled` | `bool` | `true` | Enable internal PostgreSQL deployment Set to false to use an external PostgreSQL database |
| `postgresql.external` | `object` | `{"existingSecret":"","properties":{"fusion_database":"","host":"","olake_database":"","password":"","port":5432,"ssl_mode":"disable","temporal_database":"","username":""},"secretKeys":{"fusion_database":"fusion_database","host":"host","olake_database":"olake_database","password":"password","port":"port","ssl_mode":"ssl_mode","temporal_database":"temporal_database","username":"username"}}` | External PostgreSQL configuration (used when enabled: false) Provide Kubernetes secret containing database credentials |
| `postgresql.external.existingSecret` | `string` | `""` | Name of the Kubernetes secret containing external database credentials The secret must contain keys: host, port, database, username, password Example secret creation:   kubectl create secret generic my-postgres-secret \     --from-literal=host=my-postgres.example.com \     --from-literal=port=5432 \     --from-literal=olake_database=olake \     --from-literal=temporal_database=temporal \     --from-literal=username=olake_user \     --from-literal=password=my_secure_password \     --from-literal=ssl_mode=disable |
| `postgresql.external.properties` | `object` | `{"fusion_database":"","host":"","olake_database":"","password":"","port":5432,"ssl_mode":"disable","temporal_database":"","username":""}` | Explicit PostgreSQL properties (used if existingSecret is not set) |
| `postgresql.external.secretKeys` | `object` | `{"fusion_database":"fusion_database","host":"host","olake_database":"olake_database","password":"password","port":"port","ssl_mode":"ssl_mode","temporal_database":"temporal_database","username":"username"}` | Map the keys from existing secret to the required fields |
| `postgresql.image` | `object` | `{"pullPolicy":"Always","repository":"library/postgres","tag":"14-alpine"}` | PostgreSQL image configuration |
| `postgresql.nodeSelector` | `object` | `{}` | Pod scheduling constraints |
| `postgresql.persistence` | `object` | `{"size":"8Gi"}` | PostgreSQL persistence configuration |
| `postgresql.persistence.size` | `string` | `"8Gi"` | Size of persistent volume for PostgreSQL data |
| `postgresql.podAnnotations` | `object` | `{}` | Additional pod annotations |
| `postgresql.podLabels` | `object` | `{}` | Additional pod labels |
| `postgresql.resources` | `object` | `{}` | PostgreSQL resource requirements (optional overrides) Defaults: requests: memory=256Mi, cpu=250m (no limits by default) Example customizations:   resources:     requests:       memory: "512Mi"       cpu: "500m"     limits:       memory: "1Gi"       cpu: "1000m" |
| `postgresql.securityContext` | `object` | `{"fsGroup":70,"runAsGroup":70,"runAsNonRoot":true,"runAsUser":70}` | Run PostgreSQL container as the 'postgres' user for enhanced security UID 70 / GID 70 is REQUIRED for the postgres:XX-alpine image. Alpine Linux's postgresql APK package creates the 'postgres' system user with a fixed UID/GID of 70. The entrypoint script uses 'gosu postgres' to drop privileges and expects data directories to be owned by this UID. |
| `postgresql.tolerations` | `list` | `[]` | Tolerations for pod assignment |
| `temporal.adminTools` | `object` | `{"image":{"pullPolicy":"Always","repository":"temporalio/admin-tools","tag":"1.32.0"}}` | temporalio/admin-tools image, used by the schema-migration and namespace-creation initContainers (temporal-sql-tool / temporal CLI). Keep the tag in sync with temporal.server.image.tag. |
| `temporal.enabled` | `bool` | `true` | Enable internal Temporal deployment Set to false to use an external Temporal Cloud/server |
| `temporal.external` | `object` | `{"existingSecret":"","properties":{"TEMPORAL_ADDRESS":"","TEMPORAL_API_KEY":"","TEMPORAL_ENABLE_TLS":"true","TEMPORAL_EXTERNAL":"true","TEMPORAL_NAMESPACE":"","TEMPORAL_TASK_QUEUE":""},"secretKeys":{"TEMPORAL_ADDRESS":"TEMPORAL_ADDRESS","TEMPORAL_API_KEY":"TEMPORAL_API_KEY","TEMPORAL_ENABLE_TLS":"TEMPORAL_ENABLE_TLS","TEMPORAL_EXTERNAL":"TEMPORAL_EXTERNAL","TEMPORAL_NAMESPACE":"TEMPORAL_NAMESPACE","TEMPORAL_TASK_QUEUE":"TEMPORAL_TASK_QUEUE"}}` | External Temporal configuration (used when enabled: false) Provide Kubernetes secret containing Temporal credentials/configuration |
| `temporal.external.existingSecret` | `string` | `""` | Name of the Kubernetes secret containing external Temporal configuration The secret must contain keys: TEMPORAL_ADDRESS, TEMPORAL_API_KEY, TEMPORAL_ENABLE_TLS, TEMPORAL_EXTERNAL, TEMPORAL_NAMESPACE, TEMPORAL_TASK_QUEUE Example secret creation:   kubectl create secret generic my-temporal-secret \     --from-literal=TEMPORAL_ADDRESS=<region>.<cloud>.api.temporal.io:7233 \     --from-literal=TEMPORAL_API_KEY=ldjflskjdglsed.sidhfksdhf.sdlkjglksjdgljsd-sldjglksjdgljs-sd;kg;sldjg;ljsd-sdklhglksdhglskjd \     --from-literal=TEMPORAL_ENABLE_TLS=true \     --from-literal=TEMPORAL_EXTERNAL=true \     --from-literal=TEMPORAL_NAMESPACE=<namespace> \     --from-literal=TEMPORAL_TASK_QUEUE=<queue_name> |
| `temporal.external.properties` | `object` | `{"TEMPORAL_ADDRESS":"","TEMPORAL_API_KEY":"","TEMPORAL_ENABLE_TLS":"true","TEMPORAL_EXTERNAL":"true","TEMPORAL_NAMESPACE":"","TEMPORAL_TASK_QUEUE":""}` | Explicit Temporal properties (used if existingSecret is not set) |
| `temporal.external.secretKeys` | `object` | `{"TEMPORAL_ADDRESS":"TEMPORAL_ADDRESS","TEMPORAL_API_KEY":"TEMPORAL_API_KEY","TEMPORAL_ENABLE_TLS":"TEMPORAL_ENABLE_TLS","TEMPORAL_EXTERNAL":"TEMPORAL_EXTERNAL","TEMPORAL_NAMESPACE":"TEMPORAL_NAMESPACE","TEMPORAL_TASK_QUEUE":"TEMPORAL_TASK_QUEUE"}` | Map the keys from existing secret to the required fields |
| `temporal.server` | `object` | `{"affinity":{},"image":{"pullPolicy":"Always","repository":"temporalio/server","tag":"1.32.0"},"nodeSelector":{},"podAnnotations":{},"podLabels":{},"resources":{},"securityContext":{},"strategy":{},"tolerations":[]}` | Temporal server configuration |
| `temporal.server.affinity` | `object` | `{}` | Affinity settings for pod assignment |
| `temporal.server.image` | `object` | `{"pullPolicy":"Always","repository":"temporalio/server","tag":"1.32.0"}` | Temporal server image configuration |
| `temporal.server.nodeSelector` | `object` | `{}` | Pod scheduling constraints |
| `temporal.server.podAnnotations` | `object` | `{}` | Additional pod annotations |
| `temporal.server.podLabels` | `object` | `{}` | Additional pod labels |
| `temporal.server.resources` | `object` | `{}` | Temporal server resource requirements (optional overrides) Defaults: requests: memory=512Mi, cpu=500m (no limits by default) Example customizations:   resources:     requests:       memory: "1Gi"       cpu: "750m"     limits:       memory: "2Gi"       cpu: "1500m" |
| `temporal.server.securityContext` | `object` | `{}` | Temporal server security context Example customizations: securityContext:   runAsNonRoot: true   runAsUser: 1000   runAsGroup: 1000 |
| `temporal.server.strategy` | `object` | `{}` | Deployment upgrade strategy. Empty uses the Kubernetes default (RollingUpdate) |
| `temporal.server.tolerations` | `list` | `[]` | Tolerations for pod assignment |
| `temporal.ui` | `object` | `{"enabled":false}` | Temporal Web UI configuration (disabled by default) |
| `temporal.ui.enabled` | `bool` | `false` | Enable Temporal Web UI |

---

## Optional Add-ons (Fusion & Elasticsearch)

Settings for optional components including Fusion (Apache Amoro for Iceberg compaction) and Elasticsearch (for Temporal advanced visibility).

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `elasticsearch.affinity` | `object` | `{}` | Affinity settings for pod assignment |
| `elasticsearch.enabled` | `bool` | `false` | Enable Elasticsearch deployment when using it as visibility store for Temporal Shifting default visibility store to postgresql from Jan 30th, 2026 onwards. |
| `elasticsearch.image` | `object` | `{"pullPolicy":"Always","repository":"elasticsearch","tag":"7.17.10"}` | Elasticsearch image configuration |
| `elasticsearch.nodeSelector` | `object` | `{}` | Pod scheduling constraints |
| `elasticsearch.persistence` | `object` | `{"size":"8Gi"}` | Elasticsearch persistence configuration |
| `elasticsearch.persistence.size` | `string` | `"8Gi"` | Size of persistent volume for Elasticsearch data |
| `elasticsearch.podAnnotations` | `object` | `{}` | Additional pod annotations |
| `elasticsearch.podLabels` | `object` | `{}` | Additional pod labels |
| `elasticsearch.resources` | `object` | `{}` | Elasticsearch resource requirements (optional overrides) Defaults: requests: memory=1Gi, cpu=500m (no limits by default) Example customizations:   resources:     requests:       memory: "2Gi"       cpu: "1000m"     limits:       memory: "4Gi"       cpu: "2000m" |
| `elasticsearch.securityContext` | `object` | `{"fsGroup":1000,"runAsGroup":1000,"runAsUser":1000}` | Elasticsearch security context |
| `elasticsearch.tolerations` | `list` | `[]` | Tolerations for pod assignment |
| `fusion.affinity` | `object` | `{}` |  |
| `fusion.enabled` | `bool` | `false` | Enable Fusion (Amoro) components. Set to false to disable all Fusion resources. Fusion requires NFS/RWX shared storage and is not supported when global.logFileStorageMode is s3. NOTE: NFS mode is deprecated and will be removed end of 2026. |
| `fusion.env` | `object` | `{}` | Environment variables for OLake Fusion Add custom environment variables here Example:   env:     CUSTOM_VAR: "value"     FEATURE_FLAG: "true" |
| `fusion.image` | `object` | `{"pullPolicy":"Always","repository":"olakego/fusion","tag":"latest"}` | - Fusion (Amoro) container image |
| `fusion.nodeSelector` | `object` | `{}` | - Pod scheduling constraints for the Fusion pod |
| `fusion.optimizer` | `object` | `{"affinity":{},"nodeSelector":{},"spark":{"desiredParallelism":3,"executor":{"affinity":{},"nodeSelector":{},"tolerations":[]},"extraConfig":{"spark.driver.memory":"1g","spark.dynamicAllocation.enabled":"true","spark.dynamicAllocation.shuffleTracking.enabled":"true","spark.executor.cores":"1","spark.executor.memory":"4g","spark.shuffle.service.enabled":"false"},"image":{"pullPolicy":"Always","repository":"olakego/fusion-spark","tag":"latest"},"jobUri":"local:///opt/spark/usrlib/optimizer-job.jar","properties":{"deploy-mode":"cluster","export.HADOOP_USER_NAME":"spark","export.SPARK_CONF_DIR":"/opt/spark/conf/","master":"k8s://https://kubernetes.default.svc:443","spark-home":"/opt/spark/"}},"tolerations":[]}` | - Optimizer configuration (Spark-on-Kubernetes) |
| `fusion.podAnnotations` | `object` | `{}` | Additional pod annotations |
| `fusion.podLabels` | `object` | `{}` | Additional pod labels |
| `fusion.resources` | `object` | `{}` | - Fusion pod resource requirements (optional overrides) Example:   resources:     requests:       cpu: "500m"       memory: "1Gi"     limits:       cpu: "1"       memory: "2Gi" |
| `fusion.securityContext` | `object` | `{}` | - Fusion pod security context Example customizations for Fusion (Amoro): securityContext:   runAsNonRoot: false   runAsUser: 1000   runAsGroup: 1000   fsGroup: 1000 |
| `fusion.serviceAccount` | `object` | `{"annotations":{},"create":true,"name":""}` | Service Account configuration |
| `fusion.serviceAccount.annotations` | `object` | `{}` | Service account annotations for Fusion. |
| `fusion.serviceAccount.create` | `bool` | `true` | Create a service account for Fusion. |
| `fusion.serviceAccount.name` | `string` | `""` | Service account name for Fusion. If not set and `create` is true, a name is generated using the fullname template. |
| `fusion.shade` | `object` | `{"identifier":"default","sensitiveKeywords":"admin-password;database.password"}` | - Amoro config shade properties |
| `fusion.tolerations` | `list` | `[]` |  |
