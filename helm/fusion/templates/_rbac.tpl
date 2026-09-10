{{/*
Permissions Fusion needs to schedule and manage the Spark optimizer master and
worker pods in its own namespace.
*/}}
{{- define "fusion.rbac.rules" -}}
# Pod management - required for optimizer master/worker pods
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["get", "watch", "list", "create", "delete", "deletecollection", "patch"]
- apiGroups: [""]
  resources: ["pods/exec"]
  verbs: ["create"]
- apiGroups: [""]
  resources: ["pods/log"]
  verbs: ["get", "list"]
- apiGroups: [""]
  resources: ["pods/status"]
  verbs: ["get", "list", "watch"]
# Services - required for optimizer master service discovery
- apiGroups: [""]
  resources: ["services"]
  verbs: ["get", "list", "watch", "create", "update", "delete", "patch"]
# ConfigMaps - required for Spark configuration
- apiGroups: [""]
  resources: ["configmaps"]
  verbs: ["get", "list", "watch", "create", "update", "delete", "patch", "deletecollection"]
# PVCs - required if using dynamic volume provisioning
- apiGroups: [""]
  resources: ["persistentvolumeclaims"]
  verbs: ["get", "list", "watch", "create", "delete"]
# Secrets - required for pulling images from private registries
- apiGroups: [""]
  resources: ["secrets"]
  verbs: ["get", "list", "watch"]
# Events - helpful for debugging
- apiGroups: [""]
  resources: ["events"]
  verbs: ["get", "list", "watch"]
{{- end -}}
