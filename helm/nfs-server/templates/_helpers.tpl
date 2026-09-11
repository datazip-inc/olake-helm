{{/*
Base name that resource names are built from.

This deliberately mirrors the `olake.fullname` helper this chart was split out
of, using `nameOverride` (default "olake") in place of `.Chart.Name`. Keeping
the base identical means the StatefulSet, Service, ServiceAccount, ClusterRole,
ClusterRoleBinding and StorageClass keep the names they already have in live
clusters - all of them carry `helm.sh/resource-policy: keep`, so a rename would
orphan the existing objects instead of upgrading them.
*/}}
{{- define "nfs-server.base" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := .Values.nameOverride | default "olake" -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Short name used in `app.kubernetes.io/name` labels
*/}}
{{- define "nfs-server.name" -}}
{{- .Values.nameOverride | default "olake" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Fully qualified name of the NFS server resources
*/}}
{{- define "nfs-server.fullname" -}}
{{- printf "%s-nfs-server" (include "nfs-server.base" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Get the namespace name
*/}}
{{- define "nfs-server.namespace" -}}
{{- .Values.namespaceOverride | default .Release.Namespace -}}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "nfs-server.labels" -}}
app.kubernetes.io/managed-by: {{ .Release.Service }}
olake.io/part-of: olake
{{- end }}

{{/*
Name of the shared ReadWriteMany claim that Fusion and the OLake worker mount.
Release independent on purpose: both parent charts resolve the same claim name
from the same values, so either can create it and the other can mount it.
*/}}
{{- define "nfs-server.sharedStoragePVC" -}}
{{- if .Values.claimName -}}
{{- .Values.claimName -}}
{{- else if .Values.enabled -}}
olake-shared-storage
{{- else -}}
{{- .Values.external.name | default "olake-shared-storage" -}}
{{- end -}}
{{- end -}}

{{/*
Calculate shared storage size based on NFS server backing storage
Reserves 2Gi for filesystem overhead
*/}}
{{- define "nfs-server.sharedStorageSize" -}}
{{- $nfsSize := .Values.persistence.size | default "20Gi" -}}
{{- $sizeValue := regexReplaceAll "([0-9]+).*" $nfsSize "${1}" | int -}}
{{- $sizeUnit := regexReplaceAll "[0-9]+(.*)" $nfsSize "${1}" -}}
{{- $adjustedSize := sub $sizeValue 2 -}}
{{- printf "%d%s" $adjustedSize $sizeUnit -}}
{{- end -}}

{{/*
Return the container registry base URL.
Uses CONTAINER_REGISTRY_BASE from global.env if set, otherwise defaults to registry-1.docker.io
*/}}
{{- define "nfs-server.registryBase" -}}
{{- .Values.global.env.CONTAINER_REGISTRY_BASE | default "registry-1.docker.io" -}}
{{- end -}}
