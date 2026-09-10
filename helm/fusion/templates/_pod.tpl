{{/*
Init container that copies the Spark distribution out of the optimizer image so
Fusion can submit Spark applications from its own pod.
*/}}
{{- define "fusion.pod.initContainer.spark" -}}
- name: install-spark
  image: {{ include "fusion.optimizer.spark.image" . }}
  imagePullPolicy: {{ .Values.optimizer.spark.image.pullPolicy }}
  command: ["sh", "-c", "cp -r /opt/spark/* /target/"]
  volumeMounts:
    - name: spark-binaries
      mountPath: /target
{{- end -}}

{{/*
Init container that waits for PostgreSQL and creates the Fusion database if it
does not exist yet.
*/}}
{{- define "fusion.pod.initContainer.database" -}}
- name: create-fusion-database
  image: {{ include "fusion.postgresql.image" . }}
  imagePullPolicy: {{ .Values.postgresql.image.pullPolicy }}
  command: ["bash", "-c"]
  args:
    - |
      set -e
      echo "Waiting for PostgreSQL service to be resolvable..."
      until nslookup "${POSTGRES_HOST}" > /dev/null 2>&1; do
        echo "PostgreSQL service not yet resolvable. Waiting..."
        sleep 2
      done
      echo "PostgreSQL service is resolvable!"

      echo "Waiting for PostgreSQL to be ready (port check)..."
      until nc -z "${POSTGRES_HOST}" "${POSTGRES_PORT}"; do
        echo "PostgreSQL port not ready yet. Waiting..."
        sleep 2
      done
      echo "PostgreSQL port is ready!"

      export PGPASSWORD=${POSTGRES_PASSWORD}
      echo "Attempting to create database '${POSTGRES_DATABASE}'..."
      if ! psql -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" -U "${POSTGRES_USER}" -lqt | cut -d \| -f 1 | grep -qw "${POSTGRES_DATABASE}"; then
        psql -h "${POSTGRES_HOST}" -p "${POSTGRES_PORT}" -U "${POSTGRES_USER}" -d postgres -c "CREATE DATABASE ${POSTGRES_DATABASE}"
        echo "Database '${POSTGRES_DATABASE}' created."
      else
        echo "Database '${POSTGRES_DATABASE}' already exists. Skipping creation."
      fi
  env:
    {{- $secret := include "fusion.postgresql.secretName" . }}
    - name: POSTGRES_HOST
      valueFrom:
        secretKeyRef:
          name: {{ $secret }}
          key: {{ include "fusion.postgresql.secretKey" (dict "key" "host" "context" $) }}
    - name: POSTGRES_PORT
      valueFrom:
        secretKeyRef:
          name: {{ $secret }}
          key: {{ include "fusion.postgresql.secretKey" (dict "key" "port" "context" $) }}
    - name: POSTGRES_USER
      valueFrom:
        secretKeyRef:
          name: {{ $secret }}
          key: {{ include "fusion.postgresql.secretKey" (dict "key" "username" "context" $) }}
    - name: POSTGRES_PASSWORD
      valueFrom:
        secretKeyRef:
          name: {{ $secret }}
          key: {{ include "fusion.postgresql.secretKey" (dict "key" "password" "context" $) }}
    - name: POSTGRES_DATABASE
      valueFrom:
        secretKeyRef:
          name: {{ $secret }}
          key: {{ include "fusion.postgresql.secretKey" (dict "key" "fusion_database" "context" $) }}
{{- end -}}

{{/*
All init containers for the Fusion pod
*/}}
{{- define "fusion.pod.initContainers" -}}
{{- if .Values.postgresql.createDatabase.enabled }}
{{- include "fusion.pod.initContainer.database" . }}
{{ end }}
{{- include "fusion.pod.initContainer.spark" . }}
{{- end -}}

{{/*
Ports exposed by the Fusion container
*/}}
{{- define "fusion.pod.container.ports" -}}
- name: rest
  containerPort: {{ .Values.server.rest.port }}
- name: table
  containerPort: {{ .Values.server.table.port }}
- name: optimizing
  containerPort: {{ .Values.server.optimizing.port }}
{{- if .Values.plugin.metricReporters }}
{{- if .Values.plugin.metricReporters.prometheusExporter.enabled }}
- name: prometheus
  containerPort: {{ .Values.plugin.metricReporters.prometheusExporter.properties.port }}
{{- end }}
{{- end }}
{{- end -}}

{{/*
Volume mounts for the Fusion container
*/}}
{{- define "fusion.pod.container.mounts" -}}
{{- $home := include "fusion.home" . -}}
- name: fusion-config
  mountPath: {{ $home }}/conf/config.yaml
  readOnly: true
  subPath: config.yaml
- name: fusion-config
  mountPath: {{ $home }}/conf/master-pod-template.yaml
  readOnly: true
  subPath: master-pod-template.yaml
- name: fusion-config
  mountPath: {{ $home }}/conf/worker-pod-template.yaml
  readOnly: true
  subPath: worker-pod-template.yaml
{{- if .Values.plugin.metricReporters }}
- name: fusion-config
  mountPath: {{ $home }}/conf/plugins/metric-reporters.yaml
  readOnly: true
  subPath: metric-reporters.yaml
{{- end }}
{{- if .Values.plugin.tableRuntimeFactories }}
- name: fusion-config
  mountPath: {{ $home }}/conf/plugins/table-runtime-factories.yaml
  readOnly: true
  subPath: table-runtime-factories.yaml
{{- end }}
- name: spark-binaries
  mountPath: /opt/spark
{{- if .Values.sharedStorage.enabled }}
- name: shared-storage
  mountPath: {{ .Values.sharedStorage.mountPath }}
  subPath: {{ .Values.sharedStorage.subPath }}
{{- end }}
{{- with .Values.volumeMounts }}
{{- tpl (toYaml .) $ | nindent 0 }}
{{- end }}
{{- end -}}

{{/*
Volumes for the Fusion pod
*/}}
{{- define "fusion.pod.volumes" -}}
- name: fusion-config
  configMap:
    name: {{ include "fusion.fullname" . }}-config
- name: spark-binaries
  emptyDir: {}
{{- if .Values.sharedStorage.enabled }}
- name: shared-storage
  persistentVolumeClaim:
    claimName: {{ include "fusion.sharedStoragePVC" . }}
{{- end }}
{{- with .Values.volumes }}
{{- tpl (toYaml .) $ | nindent 0 }}
{{- end }}
{{- end -}}

{{/*
Environment for the Fusion container
*/}}
{{- define "fusion.pod.container.env" -}}
- name: AMORO_CONF_DIR
  value: {{ printf "%s/conf" (include "fusion.home" .) | quote }}
{{- if .Values.sharedStorage.enabled }}
- name: LOG_DIR
  value: {{ .Values.sharedStorage.mountPath | quote }}
{{- end }}
{{- range $key, $value := .Values.global.env }}
- name: {{ $key }}
  value: {{ $value | quote }}
{{- end }}
{{- range $key, $value := .Values.env }}
- name: {{ $key }}
  value: {{ $value | quote }}
{{- end }}
{{- $authSecret := include "fusion.auth.secretName" . }}
- name: AMS_ADMIN__USERNAME
  valueFrom:
    secretKeyRef:
      name: {{ $authSecret }}
      key: {{ required "auth.secretKeys.username must be set" .Values.auth.secretKeys.username }}
- name: AMS_ADMIN__PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ $authSecret }}
      key: {{ required "auth.secretKeys.password must be set" .Values.auth.secretKeys.password }}
{{- $pgSecret := include "fusion.postgresql.secretName" . }}
- name: AMS_DATABASE_USERNAME
  valueFrom:
    secretKeyRef:
      name: {{ $pgSecret }}
      key: {{ include "fusion.postgresql.secretKey" (dict "key" "username" "context" $) }}
- name: AMS_DATABASE_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ $pgSecret }}
      key: {{ include "fusion.postgresql.secretKey" (dict "key" "password" "context" $) }}
{{- /* An external secret owns host/port/database, so build the JDBC URL at runtime. */}}
{{- if and (not .Values.postgresql.enabled) .Values.postgresql.external.existingSecret }}
- name: _AMS_PG_HOST
  valueFrom:
    secretKeyRef:
      name: {{ $pgSecret }}
      key: {{ include "fusion.postgresql.secretKey" (dict "key" "host" "context" $) }}
- name: _AMS_PG_PORT
  valueFrom:
    secretKeyRef:
      name: {{ $pgSecret }}
      key: {{ include "fusion.postgresql.secretKey" (dict "key" "port" "context" $) }}
- name: _AMS_PG_DB
  valueFrom:
    secretKeyRef:
      name: {{ $pgSecret }}
      key: {{ include "fusion.postgresql.secretKey" (dict "key" "fusion_database" "context" $) }}
- name: AMS_DATABASE_URL
  value: "jdbc:postgresql://$(_AMS_PG_HOST):$(_AMS_PG_PORT)/$(_AMS_PG_DB)"
{{- end }}
{{- end -}}
