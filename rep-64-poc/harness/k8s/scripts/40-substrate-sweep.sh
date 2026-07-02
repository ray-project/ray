#!/usr/bin/env bash
# 40-substrate-sweep.sh — for each StorageClass in SUBSTRATE_SWEEP_CLASSES,
# run probe_fsync.py inside a K8s Job and report the per-class verdict.
#
# Why a K8s Job (not a RayJob, not kubectl exec into ray): the fsync probe
# needs a clean PVC backed by a specific StorageClass, no Ray dependency,
# and the substrate IS the variable under test.  RayCluster-shaped flows
# all share the rep64-gcs-data PVC; a Job lets each class stand alone.
#
# probe_fsync.py ships via a per-class ConfigMap created at run time from
# rep-64-poc/harness/durability/probe_fsync.py — single source of truth.
#
# Output: result envelope with metrics.by_storage_class[sc] = {verdict,
# fsync_p50_us, fsync_p99_us}.  Pass = every probe Job ran to completion;
# fail = at least one Job failed or produced no data.  Honesty verdicts
# (honest / suspicious / lying) are findings, surfaced via aggregate.py.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
source "$HERE/../lib.sh"
load_env

START=$(date +%s)

if [[ -z "${SUBSTRATE_SWEEP_CLASSES:-}" ]]; then
  log "SUBSTRATE_SWEEP_CLASSES empty — skipping sweep"
  write_result "40-substrate-sweep" skipped 0 '{}' \
    "SUBSTRATE_SWEEP_CLASSES not configured"
  exit 0
fi

REPO_ROOT="$(git -C "$(dirname "$HARNESS_ENV_FILE")/../../.." rev-parse --show-toplevel 2>/dev/null)"
[[ -n "$REPO_ROOT" ]] || abort "could not locate repo root from \$HARNESS_ENV_FILE"
PROBE_SCRIPT="$REPO_ROOT/rep-64-poc/harness/durability/probe_fsync.py"
[[ -f "$PROBE_SCRIPT" ]] || abort "probe_fsync.py not found at $PROBE_SCRIPT"

JOB_TIMEOUT_S="${PROBE_JOB_TIMEOUT_S:-180}"

# slugify <text>: lowercase, replace non-alnum with -, collapse runs.
slug() { echo "$1" | tr 'A-Z' 'a-z' | sed -E 's/[^a-z0-9]+/-/g; s/^-+|-+$//g'; }

# probe_one <storage-class>: runs the probe in a Job, prints
# "verdict|p50|p99" on stdout and returns 0 on Job=Complete, 1 otherwise.
probe_one() {
  local sc="$1"
  local sc_slug job_name pod probe_out succeeded failed
  sc_slug="$(slug "$sc")"
  job_name="rep64-probe-${sc_slug}"

  # Idempotent cleanup first — previous run may have left state behind.
  kubectl delete job "$job_name" -n "$NAMESPACE" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  kubectl delete pvc "${job_name}-data" -n "$NAMESPACE" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  kubectl delete configmap "${job_name}-script" -n "$NAMESPACE" --ignore-not-found >/dev/null 2>&1 || true

  kubectl create configmap "${job_name}-script" -n "$NAMESPACE" \
    --from-file=probe_fsync.py="$PROBE_SCRIPT" >/dev/null

  JOB_NAME="$job_name" STORAGE_CLASS="$sc" \
    render_manifest "$HERE/../manifests/job-substrate-probe.yaml.tmpl" \
    | kubectl apply -f - >/dev/null

  # Poll Job status — kubectl wait --for=condition=Complete blocks on
  # success only, but we also want to surface Failed cleanly.
  local poll_start now
  poll_start=$(date +%s)
  while :; do
    succeeded="$(kubectl get job "$job_name" -n "$NAMESPACE" \
      -o jsonpath='{.status.succeeded}' 2>/dev/null || echo 0)"
    failed="$(kubectl get job "$job_name" -n "$NAMESPACE" \
      -o jsonpath='{.status.failed}' 2>/dev/null || echo 0)"
    if [[ "${succeeded:-0}" -ge 1 ]]; then
      break
    fi
    if [[ "${failed:-0}" -ge 1 ]]; then
      log "  Job $job_name FAILED"
      _probe_one_emit "$job_name" "" 1
      return 1
    fi
    now=$(date +%s)
    if (( now - poll_start > JOB_TIMEOUT_S )); then
      log "  Job $job_name did not complete in ${JOB_TIMEOUT_S}s"
      _probe_one_emit "$job_name" "" 1
      return 1
    fi
    sleep 3
  done

  pod="$(kubectl get pod -n "$NAMESPACE" \
    -l job-name="$job_name" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -z "$pod" ]]; then
    log "  WARN: no pod found for Job $job_name"
    _probe_one_emit "$job_name" "" 1
    return 1
  fi

  probe_out="$(kubectl logs -n "$NAMESPACE" "$pod" 2>/dev/null || true)"
  _probe_one_emit "$job_name" "$probe_out" 0
}

# _probe_one_emit <job_name> <probe_out> <is_failure>: prints the
# pipe-delimited result (verdict|p50|p99) on stdout and runs cleanup.
_probe_one_emit() {
  local job_name="$1" probe_out="$2" is_failure="$3"
  if (( is_failure )) || [[ -z "$probe_out" ]]; then
    echo "no_data|null|null"
  else
    local verdict p50 p99
    verdict="$(echo "$probe_out" | jq -r '.label.verdict // "no_data"' 2>/dev/null || echo no_data)"
    p50="$(echo "$probe_out" | jq -r '.results.fsync.p50_us // null' 2>/dev/null || echo null)"
    p99="$(echo "$probe_out" | jq -r '.results.fsync.p99_us // null' 2>/dev/null || echo null)"
    echo "${verdict}|${p50}|${p99}"
  fi
  kubectl delete job "$job_name" -n "$NAMESPACE" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  kubectl delete pvc "${job_name}-data" -n "$NAMESPACE" --ignore-not-found --wait=true >/dev/null 2>&1 || true
  kubectl delete configmap "${job_name}-script" -n "$NAMESPACE" --ignore-not-found >/dev/null 2>&1 || true
}

# Iterate the comma-separated class list.  IFS-scoped read avoids leaking IFS.
IFS=',' read -ra SC_ARRAY <<< "$SUBSTRATE_SWEEP_CLASSES"

declare -A SC_RESULTS=()
all_complete=1
for raw_sc in "${SC_ARRAY[@]}"; do
  sc="$(echo "$raw_sc" | xargs)"  # trim
  [[ -n "$sc" ]] || continue
  log "=== probing StorageClass: $sc ==="
  if probe_result="$(probe_one "$sc")"; then
    log "  ok: $probe_result"
  else
    log "  failed: $probe_result"
    all_complete=0
  fi
  SC_RESULTS[$sc]="$probe_result"
done

# Build by_storage_class map via jq.
METRICS_JSON='{"by_storage_class": {}}'
for sc in "${!SC_RESULTS[@]}"; do
  IFS='|' read -r verdict p50 p99 <<< "${SC_RESULTS[$sc]}"
  METRICS_JSON="$(echo "$METRICS_JSON" | jq \
    --arg sc "$sc" \
    --arg v "$verdict" \
    --argjson p50 "$p50" \
    --argjson p99 "$p99" \
    '.by_storage_class[$sc] = {verdict: $v, fsync_p50_us: $p50, fsync_p99_us: $p99}')"
done
METRICS_JSON="$(echo "$METRICS_JSON" | jq \
  --argjson c "${#SC_RESULTS[@]}" \
  '. + {classes_probed_count: $c}')"

DURATION=$(( $(date +%s) - START ))
if (( all_complete )); then
  STATUS=pass
else
  STATUS=fail
fi
write_result "40-substrate-sweep" "$STATUS" "$DURATION" "$METRICS_JSON"
log "40-substrate-sweep done: status=$STATUS, ${#SC_RESULTS[@]} class(es) probed"
