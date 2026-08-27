{{/*
Base name that the secret name is built from.

Mirrors the `olake.fullname` helper this chart was split out of, using
`nameOverride` (default "olake") in place of `.Chart.Name`, so the secret keeps
the name it already has in live clusters.
*/}}
{{- define "postgresql.base" -}}
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
Name of the StatefulSet and the headless Service.

Defaults to the bare "postgresql" the olake chart has always used. That name is
load bearing: the StatefulSet's volume claim is `postgresql-storage-<name>-0`,
so changing it strands the database volume. A second instance in the same
namespace (a standalone Fusion install) must override `serviceName`.
*/}}
{{- define "postgresql.fullname" -}}
{{- .Values.serviceName | default "postgresql" -}}
{{- end -}}

{{/*
Name of the credentials secret
*/}}
{{- define "postgresql.secretName" -}}
{{- .Values.existingSecret | default (printf "%s-postgresql-secret" (include "postgresql.base" .)) -}}
{{- end -}}

{{- define "postgresql.namespace" -}}
{{- .Values.namespaceOverride | default .Release.Namespace -}}
{{- end -}}

{{/*
In-cluster DNS name the secret publishes as `host`
*/}}
{{- define "postgresql.host" -}}
{{- printf "%s.%s.svc.%s" (include "postgresql.fullname" .) (include "postgresql.namespace" .) (.Values.clusterDomain | default "cluster.local") -}}
{{- end -}}

{{/*
Selector labels. Immutable on an existing StatefulSet - do not change these.
*/}}
{{- define "postgresql.selectorLabels" -}}
app.kubernetes.io/name: postgresql
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: {{ .Values.componentLabel | default "database" }}
{{- end -}}

{{- define "postgresql.labels" -}}
app.kubernetes.io/managed-by: {{ .Release.Service }}
olake.io/part-of: olake
{{- end -}}

{{- define "postgresql.image" -}}
{{- printf "%s/%s:%s" (.Values.global.env.CONTAINER_REGISTRY_BASE | default "registry-1.docker.io") .Values.image.repository .Values.image.tag -}}
{{- end -}}
