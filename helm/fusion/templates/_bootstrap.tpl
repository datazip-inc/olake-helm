{{/*
Lifecycle hooks that register and release the optimizer through Fusion's REST
API. Fusion has no declarative way to describe an optimizer group, so the pod
does it for itself on start-up and cleans up on shutdown.
*/}}

{{/*
postStart: drop optimizer master pods left over from a previous Fusion pod,
then (re-)create the optimizer group and its optimizers.
*/}}
{{- define "fusion.bootstrap.postStart" -}}
{{- $group := .Values.optimizer.group.name -}}
{{- $container := .Values.optimizer.group.container -}}
{{- $parallelism := .Values.optimizer.parallelism -}}
{{- $masterLabel := .Values.optimizer.master.workloadLabel -}}
exec > /proc/1/fd/1 2>/proc/1/fd/2
set -e

echo "Deleting optimizer master pods with labelSelector=olake.io/workload={{ $masterLabel }}..."
K8S_TOKEN=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)
K8S_NAMESPACE=$(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace)
set +e
DELETE_PODS_RESP=$(curl -sS --cacert /var/run/secrets/kubernetes.io/serviceaccount/ca.crt -X DELETE \
  -H "Authorization: Bearer ${K8S_TOKEN}" \
  "https://kubernetes.default.svc/api/v1/namespaces/${K8S_NAMESPACE}/pods?labelSelector=olake.io%2Fworkload%3D{{ $masterLabel }}")
set -e

BASE_URL="http://127.0.0.1:{{ .Values.server.rest.port }}"
echo "Running Fusion optimizer bootstrap via postStart at ${BASE_URL}..."

echo "Logging into Fusion..."
LOGIN_RC=0
for i in $(seq 1 60); do
  set +e
  LOGIN_RESP=$(curl -sS -f -c /tmp/cookies.txt \
    -X POST "${BASE_URL}/api/ams/v1/login" \
    -H "Content-Type: application/json" \
    -H "X-Request-Source: Web" \
    -d "{\"user\":\"${AMS_ADMIN__USERNAME}\",\"password\":\"${AMS_ADMIN__PASSWORD}\"}" 2>&1)
  LOGIN_RC=$?
  set -e
  if [ "$LOGIN_RC" -eq 0 ]; then
    echo ""
    echo "----- Login response (${i}/60) -----"
    echo "${LOGIN_RESP}"
    echo "------------------------------------"
    echo ""
    break
  fi
  echo ""
  echo "----- Login response (${i}/60, failed rc=${LOGIN_RC}) -----"
  echo "${LOGIN_RESP}"
  echo "-----------------------------------------------------------"
  echo ""
  sleep 1
done
if [ "$LOGIN_RC" -ne 0 ]; then
  echo "Login failed after retries (rc=${LOGIN_RC})."
  exit "$LOGIN_RC"
fi

# ---- Optimizer Group ----
echo "Checking optimizer group '{{ $group }}'..."
GROUPS=$(curl -s -b /tmp/cookies.txt \
  -H "X-Request-Source: Web" \
  "${BASE_URL}/api/ams/v1/optimize/resourceGroups")
echo ""
echo "----- Resource groups response -----"
echo "$GROUPS"
echo "-----------------------------------"
echo ""

if echo "$GROUPS" | grep -q '"name":"{{ $group }}"'; then
  echo "Optimizer group '{{ $group }}' already exists."
else
  echo "Creating optimizer group '{{ $group }}'..."
  CREATE_GROUP_RESP=$(curl -s -b /tmp/cookies.txt \
    -X POST "${BASE_URL}/api/ams/v1/optimize/resourceGroups" \
    -H "Content-Type: application/json" \
    -H "X-Request-Source: Web" \
    -d '{"name":"{{ $group }}","container":"{{ $container }}","properties":{}}'
  )
  echo ""
  echo "----- Create group response -----"
  echo "$CREATE_GROUP_RESP"
  echo "---------------------------------"
  echo ""
  sleep 5
fi

# ---- Optimizers ----
echo "Checking optimizers for group '{{ $group }}'..."
OPTIMIZERS=$(curl -s -b /tmp/cookies.txt \
  -H "X-Request-Source: Web" \
  "${BASE_URL}/api/ams/v1/optimize/optimizerGroups/{{ $group }}/optimizers?page=1&pageSize=50")
echo ""
echo "----- Optimizers response -----"
echo "$OPTIMIZERS"
echo "-------------------------------"
echo ""

JOB_IDS=$(echo "$OPTIMIZERS" | grep -oE '"jobId":"[^"]+"' | cut -d':' -f2 | tr -d '"')

if [ -n "$JOB_IDS" ]; then
  echo "Releasing existing optimizers..."
  for jobId in $JOB_IDS; do
    DELETE_RESP=$(curl -s -b /tmp/cookies.txt \
      -X DELETE "${BASE_URL}/api/ams/v1/optimize/optimizers/${jobId}" \
      -H "X-Request-Source: Web")
    echo ""
    echo "----- Delete optimizer ${jobId} response -----"
    echo "$DELETE_RESP"
    echo "-----------------------------------------------"
    echo ""
  done
fi

echo "Creating optimizer parallelism={{ $parallelism }}..."
CREATE_OPT_RESP=$(curl -s -b /tmp/cookies.txt \
  -X POST "${BASE_URL}/api/ams/v1/optimize/optimizerGroups/{{ $group }}/optimizers" \
  -H "Content-Type: application/json" \
  -H "X-Request-Source: Web" \
  -d '{"parallelism":{{ $parallelism }}}')
echo ""
echo "----- Create optimizers response -----"
echo "$CREATE_OPT_RESP"
echo "--------------------------------------"
echo ""

echo ""
echo "============================================"
echo "  Fusion is ready!"
echo "  Login  : ${AMS_ADMIN__USERNAME} / ${AMS_ADMIN__PASSWORD}"
echo "============================================"
{{- end -}}

{{/*
preStop: release the optimizers so their master/worker pods are torn down with
the Fusion pod instead of being orphaned.
*/}}
{{- define "fusion.bootstrap.preStop" -}}
{{- $group := .Values.optimizer.group.name -}}
exec > /proc/1/fd/1 2>/proc/1/fd/2
BASE_URL="http://127.0.0.1:{{ .Values.server.rest.port }}"
echo "Running Fusion optimizer cleanup via preStop at ${BASE_URL}..."

echo "Logging into Fusion..."
LOGIN_RC=0
for i in $(seq 1 10); do
  set +e
  LOGIN_RESP=$(curl -sS -f -c /tmp/prestop-cookies.txt \
    -X POST "${BASE_URL}/api/ams/v1/login" \
    -H "Content-Type: application/json" \
    -H "X-Request-Source: Web" \
    -d "{\"user\":\"${AMS_ADMIN__USERNAME}\",\"password\":\"${AMS_ADMIN__PASSWORD}\"}" 2>&1)
  LOGIN_RC=$?
  set -e
  if [ "$LOGIN_RC" -eq 0 ]; then
    echo "Login succeeded (${i}/10)"
    break
  fi
  echo "Login failed (${i}/10, rc=${LOGIN_RC}), retrying..."
  sleep 2
done
if [ "$LOGIN_RC" -ne 0 ]; then
  echo "Login failed after retries, skipping optimizer cleanup."
  exit 0
fi

echo "Releasing existing optimizers..."
OPTIMIZERS=$(curl -s -b /tmp/prestop-cookies.txt \
  -H "X-Request-Source: Web" \
  "${BASE_URL}/api/ams/v1/optimize/optimizerGroups/{{ $group }}/optimizers?page=1&pageSize=50")
echo "Optimizers: ${OPTIMIZERS}"
JOB_IDS=$(echo "$OPTIMIZERS" | grep -oE '"jobId":"[^"]+"' | cut -d':' -f2 | tr -d '"')
if [ -n "$JOB_IDS" ]; then
  for jobId in $JOB_IDS; do
    DELETE_RESP=$(curl -s -b /tmp/prestop-cookies.txt \
      -X DELETE "${BASE_URL}/api/ams/v1/optimize/optimizers/${jobId}" \
      -H "X-Request-Source: Web")
    echo "Released optimizer ${jobId}: ${DELETE_RESP}"
  done
else
  echo "No optimizers found to release."
fi
echo "Optimizer cleanup complete."
{{- end -}}
