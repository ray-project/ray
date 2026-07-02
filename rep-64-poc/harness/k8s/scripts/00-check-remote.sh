#!/usr/bin/env bash
# 00-check-remote.sh — read-only preflight for the remote tier.
# Verifies kubectl can talk to $KUBE_CONTEXT, the namespace exists, RBAC
# permits the resources we touch, KubeRay CRDs + operator are present, and
# the configured StorageClass(es) exist. Never mutates cluster state.
#
# Exit 0 if all REQUIRED checks pass; exit 1 otherwise. OPTIONAL checks
# (substrate sweep, fast-NVMe) print warnings but don't fail.

set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
source "$HERE/../lib.sh"
load_env

required_fail=0
optional_warn=0

# check <required|optional> <label> <cmd...>
# Runs the cmd silently. Emits ✓ on success, ✗ on required failure, ! on
# optional failure. Updates required_fail / optional_warn. Always returns 0
# so `set -e` (when callers opt in) doesn't abort on optional warnings.
check() {
  local kind="$1"; shift
  local label="$1"; shift
  local err
  if err=$("$@" 2>&1 >/dev/null); then
    log "  ✓ $label"
    return 0
  fi
  if [[ "$kind" == required ]]; then
    log "  ✗ $label"
    [[ -n "$err" ]] && log "    └─ $(printf '%s' "$err" | head -n1)"
    required_fail=1
  else
    log "  ! $label (optional)"
    optional_warn=1
  fi
  return 0
}

log "remote-tier preflight: context=$KUBE_CONTEXT ns=$NAMESPACE image=$IMAGE"

# 1. kubectl reach.
check required "kubectl reaches '$KUBE_CONTEXT'" \
  kubectl --context="$KUBE_CONTEXT" version

# 2. namespace exists.
check required "namespace '$NAMESPACE' exists" \
  kubectl --context="$KUBE_CONTEXT" get ns "$NAMESPACE"

# 3. RBAC for resources the harness creates / inspects.
for vr in \
  "get rayclusters.ray.io" \
  "create rayclusters.ray.io" \
  "delete rayclusters.ray.io" \
  "get persistentvolumeclaims" \
  "create persistentvolumeclaims" \
  "delete persistentvolumeclaims" \
  "get configmaps" \
  "create configmaps" \
  "delete configmaps" \
  "get jobs.batch" \
  "create jobs.batch" \
  "delete jobs.batch" \
  "get pods" \
  "delete pods" \
  "create pods/exec" \
  "get pods/log"
do
  v="${vr%% *}"; r="${vr##* }"
  check required "RBAC: can $v $r in $NAMESPACE" \
    kubectl --context="$KUBE_CONTEXT" auth can-i "$v" "$r" -n "$NAMESPACE"
done

# 4. KubeRay CRDs.
for crd in rayclusters.ray.io rayjobs.ray.io rayservices.ray.io; do
  check required "CRD $crd installed" \
    kubectl --context="$KUBE_CONTEXT" get crd "$crd"
done

# 5. KubeRay operator deployed somewhere — best-effort, optional.
# This requires cluster-wide list permission, which many namespaced SAs
# lack; on RBAC-restricted clusters the CRDs check above is the meaningful
# signal that the operator was deployed.
if op_out=$(kubectl --context="$KUBE_CONTEXT" get deploy -A \
    -l app.kubernetes.io/name=kuberay-operator -o name 2>&1); then
  operator_count=$(printf '%s\n' "$op_out" | grep -c . || true)
  if (( operator_count >= 1 )); then
    log "  ✓ KubeRay operator deployment found (${operator_count} instance(s))"
  else
    log "  ! no KubeRay operator deployment found cluster-wide (optional — CRDs above are the load-bearing check)"
    optional_warn=1
  fi
else
  log "  ! could not list deployments cluster-wide (RBAC?); trusting CRD check above"
  optional_warn=1
fi

# 6. STORAGE_CLASS set + exists.
if [[ -z "${STORAGE_CLASS:-}" ]]; then
  log "  ✗ STORAGE_CLASS is empty in $HARNESS_ENV_FILE — set it to a class your cluster offers"
  required_fail=1
else
  check required "StorageClass '$STORAGE_CLASS' exists" \
    kubectl --context="$KUBE_CONTEXT" get sc "$STORAGE_CLASS"
fi

# 7. Optional: substrate-sweep classes.
if [[ -n "${SUBSTRATE_SWEEP_CLASSES:-}" ]]; then
  IFS=',' read -ra sweep <<<"$SUBSTRATE_SWEEP_CLASSES"
  for sc in "${sweep[@]}"; do
    sc_trimmed="${sc// /}"
    [[ -z "$sc_trimmed" ]] && continue
    check optional "sweep StorageClass '$sc_trimmed' exists" \
      kubectl --context="$KUBE_CONTEXT" get sc "$sc_trimmed"
  done
else
  log "  - SUBSTRATE_SWEEP_CLASSES empty — 40-substrate-sweep.sh will skip"
fi

# 8. Optional: fast-NVMe class.
if [[ -n "${FAST_NVME_CLASS:-}" ]]; then
  check optional "fast-NVMe StorageClass '$FAST_NVME_CLASS' exists" \
    kubectl --context="$KUBE_CONTEXT" get sc "$FAST_NVME_CLASS"
else
  log "  - FAST_NVME_CLASS empty — 50-fast-storage.sh will skip"
fi

if (( required_fail )); then
  log ""
  log "FAIL — fix the ✗ items above; required preflight gates 10-deploy-cluster.sh"
  exit 1
fi

if (( optional_warn )); then
  log ""
  log "OK — required checks pass (with optional warnings)"
else
  log ""
  log "OK — all preflight checks pass"
fi
exit 0
