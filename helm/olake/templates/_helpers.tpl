{{/*
Expand the name of the chart.
*/}}
{{- define "olake.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "olake.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "olake.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "olake.labels" -}}
app.kubernetes.io/managed-by: {{ .Release.Service }}
olake.io/part-of: olake
{{- end }}

{{/*
Create the name of the service account to use for olake-workers
*/}}
{{- define "olake.workerServiceAccountName" -}}
{{- default (printf "%s-workers" (include "olake.fullname" .)) .Values.olakeWorker.serviceAccount.name }}
{{- end }}

{{/*
Create the name of the service account to use for Fusion
*/}}
{{- define "olake.fusionServiceAccountName" -}}
{{- if .Values.fusion.serviceAccount.create }}
{{- default (printf "%s-fusion" (include "olake.fullname" .)) .Values.fusion.serviceAccount.name }}
{{- else }}
{{- .Values.fusion.serviceAccount.name | default "default" }}
{{- end }}
{{- end }}

{{/*
Create the name of the service account to use for job pods
*/}}
{{- define "olake.jobServiceAccountName" -}}
{{- if .Values.global.jobServiceAccount.name }}
{{- .Values.global.jobServiceAccount.name }}
{{- else if .Values.global.jobServiceAccount.create }}
{{- printf "%s-job" (include "olake.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- "" }}
{{- end }}
{{- end }}

{{/*
Service account used by the olake-ui pod (job SA when present, else dedicated UI SA for GitOps).
*/}}
{{- define "olake.uiServiceAccountName" -}}
{{- if (include "olake.jobServiceAccountName" .) }}
{{- include "olake.jobServiceAccountName" . }}
{{- else if and .Values.gitops.enabled .Values.gitops.serviceAccount.create }}
{{- default (printf "%s-ui" (include "olake.fullname" .)) .Values.gitops.serviceAccount.name }}
{{- else }}
{{- "" }}
{{- end }}
{{- end }}

{{/*
Worker/UI probe shell: include shared-storage write only in NFS mode.
*/}}
{{- define "olake.workerReadinessProbeShell" -}}
{{- if eq (include "olake.storageMode" .) "nfs" }}echo ok > /data/olake-jobs/.healthcheck && {{ end }}wget -q --spider --timeout=2 http://localhost:8090/ready
{{- end -}}

{{- define "olake.workerLivenessProbeShell" -}}
{{- if eq (include "olake.storageMode" .) "nfs" }}echo ok > /data/olake-jobs/.healthcheck && {{ end }}wget -q --spider --timeout=2 http://localhost:8090/health
{{- end -}}

{{- define "olake.uiProbeShell" -}}
{{- if eq (include "olake.storageMode" .) "nfs" }}echo ok > /tmp/olake-config/.healthcheck-ui && {{ end }}nc -z localhost {{ .Values.olakeUI.env.HTTP_PORT | default "8000" }}
{{- end -}}

{{/*
S3 log file storage: in-cluster MinIO (minio.enabled: true) or external S3 (minio.enabled: false).
External covers AWS S3 (no endpoint) and S3-compatible stores like GCS or external MinIO (endpoint set).
*/}}
{{- define "olake.minio.serviceName" -}}
{{- printf "%s-minio" (include "olake.fullname" .) -}}
{{- end -}}

{{- define "olake.minio.secretName" -}}
{{- printf "%s-minio-secret" (include "olake.fullname" .) -}}
{{- end -}}

{{/* Empty for AWS S3, the SDK resolves the regional endpoint itself. */}}
{{- define "olake.s3Endpoint" -}}
{{- if .Values.s3LogFileStorage.minio.enabled -}}
{{- printf "http://%s.%s.svc.cluster.local:9000" (include "olake.minio.serviceName" .) (include "olake.namespace" .) -}}
{{- else -}}
{{- .Values.s3LogFileStorage.external.endpoint -}}
{{- end -}}
{{- end -}}

{{/* Empty when external.role.enabled; access then comes from IRSA / Pod Identity. */}}
{{- define "olake.s3CredentialsSecretName" -}}
{{- if .Values.s3LogFileStorage.minio.enabled -}}
{{- include "olake.minio.secretName" . -}}
{{- else if not .Values.s3LogFileStorage.external.role.enabled -}}
{{- .Values.s3LogFileStorage.external.existingSecret -}}
{{- end -}}
{{- end -}}

{{/*
Log storage mode from global.logFileStorageMode (default nfs).
When s3, validates s3LogFileStorage is complete before render.
Fusion still requires the shared RWX volume and is not supported in s3 mode.
*/}}
{{- define "olake.storageMode" -}}
{{- $mode := .Values.global.logFileStorageMode | default "nfs" -}}
{{- if eq $mode "s3" -}}
{{- $s3 := .Values.s3LogFileStorage -}}
{{- if not (and $s3.bucket $s3.region) -}}
{{- fail "s3LogFileStorage.bucket and s3LogFileStorage.region are required when global.logFileStorageMode is s3" -}}
{{- end -}}
{{- if not (or $s3.minio.enabled $s3.external.role.enabled $s3.external.existingSecret) -}}
{{- fail "S3 credentials are required when s3LogFileStorage.minio.enabled is false; set s3LogFileStorage.external.existingSecret or s3LogFileStorage.external.role.enabled (IRSA / EKS Pod Identity)" -}}
{{- end -}}
{{- if .Values.fusion.enabled -}}
{{- fail "Fusion is not supported when global.logFileStorageMode is s3; set fusion.enabled to false or use nfs" -}}
{{- end -}}
{{- end -}}
{{- $mode -}}
{{- end -}}

{{/*
S3 IRSA annotations for external log storage (worker and job service accounts).
Returns a YAML map when active, empty otherwise.
*/}}
{{- define "olake.s3IRSAAnnotations" -}}
{{- if and (eq (include "olake.storageMode" .) "s3") (not .Values.s3LogFileStorage.minio.enabled) .Values.s3LogFileStorage.external.role.enabled }}
{{- .Values.s3LogFileStorage.external.role.annotations | default dict | toYaml }}
{{- end }}
{{- end -}}

{{/*
S3 log file storage environment variables for olake-global-env.
*/}}
{{- define "olake.storageEnv" -}}
{{- if eq (include "olake.storageMode" .) "s3" }}
OLAKE_S3_BUCKET: {{ required "s3LogFileStorage.bucket is required when S3 log storage is active" .Values.s3LogFileStorage.bucket | quote }}
OLAKE_S3_REGION: {{ required "s3LogFileStorage.region is required when S3 log storage is active" .Values.s3LogFileStorage.region | quote }}
{{- if .Values.s3LogFileStorage.prefix }}
OLAKE_S3_PREFIX: {{ .Values.s3LogFileStorage.prefix | quote }}
{{- end }}
{{- $endpoint := include "olake.s3Endpoint" . -}}
{{- if $endpoint }}
OLAKE_S3_ENDPOINT: {{ $endpoint | quote }}
{{- end }}
{{- end }}
{{- end -}}

{{/*
Shared storage PVC name
*/}}
{{- define "olake.sharedStoragePVC" -}}
{{- if .Values.nfsServer.enabled -}}
olake-shared-storage
{{- else -}}
{{- .Values.nfsServer.external.name | default "olake-shared-storage" -}}
{{- end -}}
{{- end -}}

{{/*
Get the namespace name
*/}}
{{- define "olake.namespace" -}}
{{- .Release.Namespace -}}
{{- end -}}

{{/*
Calculate shared storage size based on NFS server backing storage
Reserves 2Gi for filesystem overhead
*/}}
{{- define "olake.sharedStorageSize" -}}
{{- $nfsSize := .Values.nfsServer.persistence.size | default "20Gi" -}}
{{- $sizeValue := regexReplaceAll "([0-9]+).*" $nfsSize "${1}" | int -}}
{{- $sizeUnit := regexReplaceAll "[0-9]+(.*)" $nfsSize "${1}" -}}
{{- $adjustedSize := sub $sizeValue 2 -}}
{{- printf "%d%s" $adjustedSize $sizeUnit -}}
{{- end -}}

{{/*
Return the container registry base URL.
Uses CONTAINER_REGISTRY_BASE from global.env if set, otherwise defaults to registry-1.docker.io
*/}}
{{- define "olake.registryBase" -}}
{{- .Values.global.env.CONTAINER_REGISTRY_BASE | default "registry-1.docker.io" -}}
{{- end -}}

{{/*
Return a container image reference. If repository is already a full registry path
(e.g. asia-south1-docker.pkg.dev/.../ui), use it as-is; otherwise prefix registryBase.
Usage: include "olake.containerImage" (dict "context" . "repository" .Values.olakeUI.image.repository "tag" .Values.olakeUI.image.tag)
*/}}
{{- define "olake.containerImage" -}}
{{- $repo := .repository -}}
{{- $tag := .tag -}}
{{- if or (contains ".pkg.dev/" $repo) (contains "gcr.io/" $repo) (contains "amazonaws.com/" $repo) (hasPrefix "ghcr.io/" $repo) -}}
{{- printf "%s:%s" $repo $tag -}}
{{- else -}}
{{- printf "%s/%s:%s" (include "olake.registryBase" .context) $repo $tag -}}
{{- end -}}
{{- end -}}

{{/*
Return the PostgreSQL secret name
*/}}
{{- define "olake.postgresql.secretName" -}}
{{- if .Values.postgresql.enabled }}
{{- printf "%s-postgresql-secret" (include "olake.fullname" .) }}
{{- else if .Values.postgresql.external.existingSecret }}
{{- .Values.postgresql.external.existingSecret }}
{{- else }}
{{- printf "%s-external-postgresql" (include "olake.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Return the Temporal secret name
*/}}
{{- define "olake.temporal.secretName" -}}
{{- if .Values.temporal.external.existingSecret }}
{{- .Values.temporal.external.existingSecret }}
{{- else }}
{{- printf "%s-external-temporal" (include "olake.fullname" .) }}
{{- end }}
{{- end }}
