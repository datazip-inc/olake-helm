{{/*
Expand the name of the chart.
*/}}
{{- define "fusion.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "fusion.fullname" -}}
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
{{- define "fusion.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Get the namespace name
*/}}
{{- define "fusion.namespace" -}}
{{- default .Release.Namespace .Values.namespaceOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "fusion.labels" -}}
helm.sh/chart: {{ include "fusion.chart" . }}
{{ include "fusion.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
olake.io/part-of: olake
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "fusion.selectorLabels" -}}
app.kubernetes.io/name: {{ include "fusion.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Fusion home directory inside the container
*/}}
{{- define "fusion.home" -}}
{{- .Values.fusionHome | default "/usr/local/amoro" -}}
{{- end -}}

{{/*
Return the container registry base URL.
Uses CONTAINER_REGISTRY_BASE from global.env if set, otherwise defaults to registry-1.docker.io
*/}}
{{- define "fusion.registryBase" -}}
{{- .Values.global.env.CONTAINER_REGISTRY_BASE | default "registry-1.docker.io" -}}
{{- end -}}

{{/*
Fusion server image
*/}}
{{- define "fusion.image" -}}
{{- printf "%s/%s:%s" (include "fusion.registryBase" .) .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) -}}
{{- end -}}

{{/*
Spark optimizer image, used both by the Fusion pod's init container and by the
optimizer master/worker pods.
*/}}
{{- define "fusion.optimizer.spark.image" -}}
{{- $spark := .Values.optimizer.spark.image -}}
{{- printf "%s/%s:%s" (include "fusion.registryBase" .) $spark.repository ($spark.tag | default .Chart.AppVersion) -}}
{{- end -}}

{{/*
Image used by the database bootstrap init container
*/}}
{{- define "fusion.postgresql.image" -}}
{{- printf "%s/%s:%s" (include "fusion.registryBase" .) .Values.postgresql.image.repository .Values.postgresql.image.tag -}}
{{- end -}}

{{/*
Create the name of the service account used by Fusion and by the optimizer pods
*/}}
{{- define "fusion.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "fusion.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- .Values.serviceAccount.name | default "default" }}
{{- end }}
{{- end }}

{{/*
Fully qualified name of the optimizing (thrift) Service.
Optimizer master pods connect back to Fusion on this address.
*/}}
{{- define "fusion.svc.optimizing.fullname" -}}
{{- printf "%s-optimizing.%s.svc.%s" (include "fusion.fullname" .) (include "fusion.namespace" .) .Values.clusterDomain -}}
{{- end -}}

{{/*
Thrift URI the optimizers use to reach Fusion
*/}}
{{- define "fusion.svc.optimizing.uri" -}}
{{- printf "thrift://%s:%v" (include "fusion.svc.optimizing.fullname" .) .Values.server.optimizing.port -}}
{{- end -}}

{{/*
Name of the secret holding the Fusion admin credentials
*/}}
{{- define "fusion.auth.secretName" -}}
{{- if .Values.auth.existingSecret }}
{{- .Values.auth.existingSecret }}
{{- else }}
{{- printf "%s-auth" (include "fusion.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Name of the secret holding the PostgreSQL credentials.
- `postgresql.enabled`  -> the secret managed by the olake chart
- external + existingSecret -> the user provided secret
- external, no secret   -> the secret this chart creates from external.properties
*/}}
{{- define "fusion.postgresql.secretName" -}}
{{- if .Values.postgresql.enabled }}
{{- .Values.postgresql.existingSecret | default (printf "%s-postgresql-secret" (include "fusion.postgresql.base" .)) }}
{{- else if .Values.postgresql.external.existingSecret }}
{{- tpl .Values.postgresql.external.existingSecret . }}
{{- else }}
{{- printf "%s-external-postgresql" (include "fusion.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Key inside the PostgreSQL secret for a given field.
Usage: {{ include "fusion.postgresql.secretKey" (dict "key" "username" "context" $) }}
*/}}
{{- define "fusion.postgresql.secretKey" -}}
{{- $ctx := .context -}}
{{- if $ctx.Values.postgresql.enabled -}}
{{- index $ctx.Values.postgresql.secretKeys .key -}}
{{- else -}}
{{- index $ctx.Values.postgresql.external.secretKeys .key -}}
{{- end -}}
{{- end -}}

{{/*
JDBC URL for the Fusion metastore.
Rendered from values; when an external secret supplies host/port/database the
Deployment overrides this with the AMS_DATABASE_URL environment variable.
*/}}
{{/*
Base name the postgresql dependency builds its secret from. Mirrors
`postgresql.base` in that chart so this one can address what it creates.
*/}}
{{- define "fusion.postgresql.base" -}}
{{- $name := .Values.postgresql.nameOverride | default "fusion" -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "fusion.postgresql.host" -}}
{{- printf "%s.%s.svc.%s" (.Values.postgresql.serviceName | default "fusion-postgresql") (include "fusion.namespace" .) .Values.clusterDomain -}}
{{- end -}}

{{- define "fusion.postgresql.jdbcUrl" -}}
{{- if .Values.postgresql.enabled -}}
{{- printf "jdbc:postgresql://%s:5432/%s" (include "fusion.postgresql.host" .) .Values.postgresql.auth.database -}}
{{- else -}}
{{- $pg := .Values.postgresql.external.properties -}}
{{- printf "jdbc:postgresql://%s:%v/%s" $pg.host $pg.port $pg.fusion_database -}}
{{- end -}}
{{- end -}}

{{/*
Name of the shared storage PVC used for Fusion and optimizer logs
*/}}
{{- define "fusion.sharedStoragePVC" -}}
{{- if .Values.sharedStorage.create -}}
{{- .Values.nfsServer.claimName | default (printf "%s-shared-storage" (include "fusion.fullname" .)) -}}
{{- else -}}
{{- .Values.sharedStorage.existingClaim | default "olake-shared-storage" -}}
{{- end -}}
{{- end -}}

{{/*
Pod scheduling constraint for an optimizer role, falling back to the Fusion
pod's own constraint when the role does not define one.
Usage: {{ include "fusion.optimizer.scheduling" (dict "role" .Values.optimizer.master "field" "nodeSelector" "context" $) }}
*/}}
{{- define "fusion.optimizer.scheduling" -}}
{{- $roleValue := index .role .field -}}
{{- if empty $roleValue -}}
{{- $fallback := index .context.Values .field -}}
{{- if empty $fallback -}}
{{- if eq .field "tolerations" }}[]{{ else }}{}{{ end -}}
{{- else -}}
{{ toYaml $fallback }}
{{- end -}}
{{- else -}}
{{ toYaml $roleValue }}
{{- end -}}
{{- end -}}
