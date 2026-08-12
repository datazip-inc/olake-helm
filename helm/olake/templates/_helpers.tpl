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
Shared storage mode: s3 when bucket+region are set, otherwise storage.mode (default nfs).
*/}}
{{- define "olake.storageMode" -}}
{{- if and .Values.s3.bucket .Values.s3.region -}}s3{{- else -}}{{- .Values.storage.mode | default "nfs" -}}{{- end -}}
{{- end -}}

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
S3 storage environment variables for olake-global-env (NFS uses olake-workers-config).
*/}}
{{- define "olake.storageEnv" -}}
{{- if eq (include "olake.storageMode" .) "s3" }}
OLAKE_S3_BUCKET: {{ required "s3.bucket is required when S3 storage is active" .Values.s3.bucket | quote }}
OLAKE_S3_REGION: {{ required "s3.region is required when S3 storage is active" .Values.s3.region | quote }}
{{- if .Values.s3.prefix }}
OLAKE_S3_PREFIX: {{ .Values.s3.prefix | quote }}
{{- end }}
{{- if and (not .Values.s3.auth.useIRSA) (or .Values.s3.auth.existingSecret .Values.s3.auth.credentials.accessKeyId) }}
OLAKE_S3_CREDENTIALS_SECRET: {{ include "olake.s3.secretName" . | quote }}
{{- end }}
{{- end }}
{{- end -}}

{{/*
S3 credentials secret name
*/}}
{{- define "olake.s3.secretName" -}}
{{- if .Values.s3.auth.existingSecret }}
{{- .Values.s3.auth.existingSecret }}
{{- else }}
{{- printf "%s-s3-credentials" (include "olake.fullname" .) }}
{{- end }}
{{- end }}

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
