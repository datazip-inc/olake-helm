{{/*
Optimizer container definition rendered into Fusion's config.yaml.

Fusion submits each optimizer as a Spark application on Kubernetes:
  * the optimizer MASTER is the Spark driver pod
  * the optimizer WORKERS are the Spark executor pods
Both are labelled with `olake.io/workload` so they can be found and cleaned up.
*/}}
{{- define "fusion.optimizer.container.spark" -}}
container-impl: org.apache.amoro.server.manager.SparkOptimizerContainer
properties:
  job-uri: {{ .Values.optimizer.spark.jobUri | quote }}
  ams-optimizing-uri: {{ include "fusion.svc.optimizing.uri" . | quote }}
  spark-conf.spark.kubernetes.container.image: {{ include "fusion.optimizer.spark.image" . | quote }}
  spark-conf.spark.kubernetes.container.image.pullPolicy: {{ .Values.optimizer.spark.image.pullPolicy | quote }}
  spark-conf.spark.kubernetes.namespace: {{ include "fusion.namespace" . | quote }}
  spark-conf.spark.kubernetes.authenticate.driver.serviceAccountName: {{ include "fusion.serviceAccountName" . | quote }}
  spark-conf.spark.kubernetes.authenticate.executor.serviceAccountName: {{ include "fusion.serviceAccountName" . | quote }}
  spark-conf.spark.kubernetes.driver.label.olake.io/workload: {{ .Values.optimizer.master.workloadLabel | quote }}
  spark-conf.spark.kubernetes.executor.label.olake.io/workload: {{ .Values.optimizer.worker.workloadLabel | quote }}
  spark-conf.spark.kubernetes.driver.podTemplateFile: "{{ include "fusion.home" . }}/conf/master-pod-template.yaml"
  spark-conf.spark.kubernetes.executor.podTemplateFile: "{{ include "fusion.home" . }}/conf/worker-pod-template.yaml"
  {{- if .Values.global.imagePullSecrets }}
  spark-conf.spark.kubernetes.container.image.pullSecrets: {{ include "fusion.optimizer.spark.pullSecrets" . | quote }}
  {{- end }}
  # Keep the Spark context alive across executor failures.
  # Number of retries by spark = spark-conf.spark.task.maxFailures - 1
  spark-conf.spark.executor.maxNumFailures: "2147483647"
  spark-conf.spark.executor.failuresValidityInterval: "10ms"
  spark-conf.spark.task.maxFailures: "4"
  {{- if .Values.sharedStorage.enabled }}
  # Shared storage so master and worker logs land in one place
  {{- $claim := include "fusion.sharedStoragePVC" . }}
  {{- $mountPath := .Values.sharedStorage.mountPath }}
  {{- $subPath := .Values.sharedStorage.subPath }}
  spark-conf.spark.kubernetes.driver.volumes.persistentVolumeClaim.fusion-logs.mount.path: {{ $mountPath | quote }}
  spark-conf.spark.kubernetes.driver.volumes.persistentVolumeClaim.fusion-logs.mount.readOnly: "false"
  spark-conf.spark.kubernetes.driver.volumes.persistentVolumeClaim.fusion-logs.mount.subPath: {{ $subPath | quote }}
  spark-conf.spark.kubernetes.driver.volumes.persistentVolumeClaim.fusion-logs.options.claimName: {{ $claim | quote }}
  spark-conf.spark.kubernetes.executor.volumes.persistentVolumeClaim.fusion-logs.mount.path: {{ $mountPath | quote }}
  spark-conf.spark.kubernetes.executor.volumes.persistentVolumeClaim.fusion-logs.mount.readOnly: "false"
  spark-conf.spark.kubernetes.executor.volumes.persistentVolumeClaim.fusion-logs.mount.subPath: {{ $subPath | quote }}
  spark-conf.spark.kubernetes.executor.volumes.persistentVolumeClaim.fusion-logs.options.claimName: {{ $claim | quote }}
  spark-conf.spark.kubernetes.driverEnv.LOG_DIR: {{ $mountPath | quote }}
  spark-conf.spark.executorEnv.LOG_DIR: {{ $mountPath | quote }}
  {{- end }}
  # Force Log4j2 to use the routing config (belt-and-suspenders with the Dockerfile COPY)
  spark-conf.spark.driver.extraJavaOptions: "-Dlog4j2.configurationFile=file:///opt/spark/conf/log4j2.xml"
  spark-conf.spark.executor.extraJavaOptions: "-Dlog4j2.configurationFile=file:///opt/spark/conf/log4j2.xml"
  {{- range $key, $value := .Values.optimizer.spark.extraConfig }}
  {{ printf "spark-conf.%s" $key }}: {{ $value | quote }}
  {{- end }}
  {{- range $key, $value := .Values.optimizer.spark.properties }}
  {{ $key }}: {{ $value | quote }}
  {{- end }}
{{- end -}}

{{/*
Image pull secrets for the optimizer master/worker pods, in the comma separated
form Spark expects.
*/}}
{{- define "fusion.optimizer.spark.pullSecrets" -}}
{{- $names := list -}}
{{- range .Values.global.imagePullSecrets -}}
{{- $names = append $names .name -}}
{{- end -}}
{{- join "," $names -}}
{{- end -}}

{{/*
Pod template for one optimizer role, mounted into the Fusion pod and handed to
Spark through spark.kubernetes.{driver,executor}.podTemplateFile.
Usage: {{ include "fusion.optimizer.podTemplate" (dict "role" .Values.optimizer.master "context" $) }}
*/}}
{{- define "fusion.optimizer.podTemplate" -}}
{{- $ctx := .context -}}
{{- $role := .role -}}
apiVersion: v1
kind: Pod
{{- /* the olake.io/workload label is set by Spark itself, see above */}}
{{- if $ctx.Values.global.podAnnotations }}
metadata:
  annotations:
    {{- toYaml $ctx.Values.global.podAnnotations | nindent 4 }}
{{- else }}
metadata: {}
{{- end }}
spec:
  nodeSelector:
    {{- include "fusion.optimizer.scheduling" (dict "role" $role "field" "nodeSelector" "context" $ctx) | nindent 4 }}
  tolerations:
    {{- include "fusion.optimizer.scheduling" (dict "role" $role "field" "tolerations" "context" $ctx) | nindent 4 }}
  affinity:
    {{- include "fusion.optimizer.scheduling" (dict "role" $role "field" "affinity" "context" $ctx) | nindent 4 }}
{{- end -}}
