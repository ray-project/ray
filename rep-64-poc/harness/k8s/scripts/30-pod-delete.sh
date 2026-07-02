#!/usr/bin/env bash
# 30-pod-delete.sh — COLLABORATORS.md item #5.
# Tests GCS state persistence across a forced head-pod deletion.
#
# For each backend (rocksdb, and optionally inmem when BASELINE_INMEM=1):
#   1. Redeploy the RayCluster with the given backend.
#   2. Run pod_delete_workload.py PHASE=setup to create detached actors.
#   3. Force-delete the head pod (kubectl delete --force --grace-period=0).
#   4. Wait for the new head pod to become Ready; measure restart latency.
#   5. Run pod_delete_workload.py PHASE=verify to check state preservation.
#   6. Write a JSON result file.
#
# Uses kubectl cp + kubectl exec (B3-style) — no RayJob CR needed.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
source "$HERE/../lib.sh"
load_env
require_env ACTOR_COUNT POD_RESTART_TIMEOUT_S DEPLOY_TIMEOUT_S

WORKLOAD_SRC="$HERE/../python/pod_delete_workload.py"
[[ -f "$WORKLOAD_SRC" ]] || abort "pod_delete_workload.py not found at $WORKLOAD_SRC"

CLUSTER_NAME=rep64-rocksdb
HEAD_LABEL="ray.io/cluster=${CLUSTER_NAME},ray.io/node-type=head"

# ---------------------------------------------------------------------------
# run_backend <backend> <test_name>
# ---------------------------------------------------------------------------
run_backend() {
  local backend="$1" test_name="$2"
  local RUN_ID="${backend}-$$-$(date +%s)"
  log "=== starting run: backend=$backend test=$test_name run_id=$RUN_ID ==="

  # 1. Redeploy with the requested backend.
  # We apply the manifest directly rather than calling 10-deploy-cluster.sh because
  # that script sources load_env (which re-sources k3d.env and overwrites GCS_STORAGE).
  # Instead, temporarily set GCS_STORAGE for envsubst and apply, then verify.
  export GCS_STORAGE="$backend"
  log "deploying RayCluster with GCS_STORAGE=$backend"
  render_manifest "$HERE/../manifests/raycluster.yaml.tmpl" | kubectl apply -f -
  wait_ray_ready "$NAMESPACE" "$CLUSTER_NAME" "$DEPLOY_TIMEOUT_S" \
    || abort "RayCluster did not reach Ready in ${DEPLOY_TIMEOUT_S}s (backend=$backend)"

  # 2. Locate head pod (before deletion).
  local OLD_HEAD
  OLD_HEAD="$(kubectl get pod -n "$NAMESPACE" \
    -l "$HEAD_LABEL" \
    -o jsonpath='{.items[0].metadata.name}')"
  [[ -n "$OLD_HEAD" ]] || abort "no head pod found in ns=$NAMESPACE before setup"
  log "head pod (pre-delete): $OLD_HEAD"

  # 3. Copy workload to head pod and run PHASE=setup.
  local REMOTE_SETUP="/tmp/pod_delete_workload_${RUN_ID}.py"
  kubectl cp "$WORKLOAD_SRC" "$NAMESPACE/$OLD_HEAD:$REMOTE_SETUP"
  log "running PHASE=setup on $OLD_HEAD (ACTOR_COUNT=$ACTOR_COUNT)..."
  local SETUP_LOGS
  SETUP_LOGS="$(kubectl exec -n "$NAMESPACE" "$OLD_HEAD" -- \
    env PHASE=setup ACTOR_COUNT="$ACTOR_COUNT" \
    python "$REMOTE_SETUP" 2>&1)" || true
  echo "$SETUP_LOGS" | tail -20 >&2

  local SNAPSHOT_LINE SNAPSHOT_JSON
  SNAPSHOT_LINE="$(echo "$SETUP_LOGS" | grep '^SNAPSHOT_JSON ' | tail -1 || true)"
  [[ -n "$SNAPSHOT_LINE" ]] || abort "no SNAPSHOT_JSON in setup output (crashed before emitting?)"
  SNAPSHOT_JSON="${SNAPSHOT_LINE#SNAPSHOT_JSON }"
  log "snapshot captured: $SNAPSHOT_JSON"

  # Cleanup setup script on old pod (best-effort, pod is about to die anyway).
  kubectl exec -n "$NAMESPACE" "$OLD_HEAD" -- rm -f "$REMOTE_SETUP" 2>/dev/null || true

  # No flush sleep needed: AsyncPut uses wo.sync=true so each write
  # fsyncs the WAL before its callback fires; the GCS actor manager's
  # RegisterActor/OnActorCreationSuccess chains are gated on those
  # callbacks, and the driver's method-call dispatch is gated on the
  # post-fsync ALIVE pubsub. By the time PHASE=setup's `ray.get` returns,
  # every actor's ALIVE row IS durable. See PR-64-poc analysis for the
  # full trace.

  # 4. Record T1 and force-delete the head pod.
  local T1
  T1=$(date +%s%N)
  log "force-deleting head pod $OLD_HEAD at T1=$T1"
  kubectl delete pod "$OLD_HEAD" -n "$NAMESPACE" --grace-period=0 --force 2>/dev/null || true

  # 5. Wait for a NEW head pod to be Ready.
  # First, give KubeRay a moment to evict the old pod object and schedule the new one,
  # then poll until a pod with a different name is Ready. kubectl wait on a label selector
  # can match the old pod while it is still in Terminating state, so we use a loop instead.
  log "waiting for new head pod to become Ready (timeout=${POD_RESTART_TIMEOUT_S}s)..."
  local wait_start; wait_start=$(date +%s)
  local NEW_HEAD_READY=""
  while :; do
    local candidate
    candidate="$(kubectl get pod -n "$NAMESPACE" -l "$HEAD_LABEL" \
      --field-selector=status.phase=Running \
      -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
    if [[ -n "$candidate" && "$candidate" != "$OLD_HEAD" ]]; then
      # Check that it's actually Ready.
      local is_ready
      is_ready="$(kubectl get pod -n "$NAMESPACE" "$candidate" \
        -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)"
      if [[ "$is_ready" == "True" ]]; then
        NEW_HEAD_READY="$candidate"
        break
      fi
    fi
    local now; now=$(date +%s)
    if (( now - wait_start > POD_RESTART_TIMEOUT_S )); then
      kubectl get pod -n "$NAMESPACE" -l "$HEAD_LABEL" -o wide >&2
      abort "new head pod did not become Ready within ${POD_RESTART_TIMEOUT_S}s"
    fi
    sleep 5
  done
  local T2
  T2=$(date +%s%N)
  log "new head pod ${NEW_HEAD_READY} is Ready"

  local POD_RESTART_S
  POD_RESTART_S=$(awk -v t1="$T1" -v t2="$T2" 'BEGIN{printf "%.2f", (t2-t1)/1e9}')
  log "new head pod Ready — pod_restart_s=$POD_RESTART_S"

  # 6. Wait for RayCluster itself to report ready.
  wait_ray_ready "$NAMESPACE" "$CLUSTER_NAME" "$POD_RESTART_TIMEOUT_S" \
    || abort "RayCluster $CLUSTER_NAME did not reach Ready after pod restart"

  # 7. Use the NEW head pod identified in the wait loop above.
  local NEW_HEAD="$NEW_HEAD_READY"
  log "new head pod: $NEW_HEAD"

  # 8. Copy workload to new head pod and run PHASE=verify.
  local REMOTE_VERIFY="/tmp/pod_delete_workload_${RUN_ID}_v.py"
  kubectl cp "$WORKLOAD_SRC" "$NAMESPACE/$NEW_HEAD:$REMOTE_VERIFY"
  log "running PHASE=verify on $NEW_HEAD..."
  local VERIFY_LOGS VERIFY_RC
  # Capture verify-phase exit code so pass/fail logic below reflects whether
  # the workload met its spec (pod_delete_workload.py exits 1 when
  # state_preserved_pct < 95 or new_tasks_ok is false).
  set +e
  VERIFY_LOGS="$(kubectl exec -n "$NAMESPACE" "$NEW_HEAD" -- \
    env PHASE=verify ACTOR_COUNT="$ACTOR_COUNT" SNAPSHOT="$SNAPSHOT_JSON" \
    python "$REMOTE_VERIFY" 2>&1)"
  VERIFY_RC=$?
  set -e
  echo "$VERIFY_LOGS" | tail -20 >&2

  local METRICS_LINE METRICS_JSON
  METRICS_LINE="$(echo "$VERIFY_LOGS" | grep '^METRICS_JSON ' | tail -1 || true)"
  [[ -n "$METRICS_LINE" ]] || abort "no METRICS_JSON in verify output (script crashed before emitting?)"
  METRICS_JSON="${METRICS_LINE#METRICS_JSON }"
  log "raw metrics: $METRICS_JSON"

  # 9. Augment metrics with pod_restart_s, backend, and verify_rc.
  METRICS_JSON="$(echo "$METRICS_JSON" | jq \
    --arg b "$backend" \
    --argjson prs "$POD_RESTART_S" \
    --argjson rc "$VERIFY_RC" \
    '. + {backend: $b, pod_restart_s: $prs, verify_rc: $rc}')"
  log "augmented metrics: $METRICS_JSON"

  # 10. Determine pass/fail.  Same threshold for every backend so the inmem
  # baseline correctly reports status=fail when it loses state — which is the
  # whole point of running the baseline.  Aggregate.py highlights the gap.
  local PCT NEW_OK STATUS
  PCT="$(echo "$METRICS_JSON" | jq -r '.state_preserved_pct')"
  NEW_OK="$(echo "$METRICS_JSON" | jq -r '.new_tasks_ok')"
  local START_S DURATION
  START_S="$(echo "scale=0; $T1 / 1000000000" | bc)"
  DURATION=$(( $(date +%s) - START_S ))

  if (( VERIFY_RC == 0 )) && awk "BEGIN{exit ($PCT >= 95) ? 0 : 1}" && [[ "$NEW_OK" == "true" ]]; then
    STATUS=pass
  else
    STATUS=fail
  fi

  write_result "$test_name" "$STATUS" "$DURATION" "$METRICS_JSON"
  log "backend=$backend status=$STATUS state_preserved_pct=$PCT pod_restart_s=$POD_RESTART_S verify_rc=$VERIFY_RC"

  # 11. Cleanup verify script on new pod (best-effort).
  kubectl exec -n "$NAMESPACE" "$NEW_HEAD" -- rm -f "$REMOTE_VERIFY" 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# Teardown helper: remove RayCluster + PVC so the next backend starts clean.
# ---------------------------------------------------------------------------
teardown_cluster() {
  log "tearing down RayCluster + PVC before switching backends..."
  kubectl delete raycluster "$CLUSTER_NAME" -n "$NAMESPACE" --ignore-not-found --wait=true
  kubectl delete pvc rep64-gcs-data -n "$NAMESPACE" --ignore-not-found --wait=true
  log "teardown complete"
}

# ---------------------------------------------------------------------------
# run_backend_with_fallback: subshell-wraps run_backend so an abort inside
# (e.g. head pod doesn't reach Ready in time) doesn't skip downstream
# backends.  Writes a fail-stub result envelope so aggregate.py still has
# a row for the failed test instead of silently missing it.
# ---------------------------------------------------------------------------
run_backend_with_fallback() {
  local backend="$1" test_name="$2"
  local start_ts duration
  start_ts=$(date +%s)
  if ( run_backend "$backend" "$test_name" ); then
    return 0
  fi
  duration=$(( $(date +%s) - start_ts ))
  log "WARN: run_backend $backend aborted; writing fail-stub result"
  write_result "$test_name" fail "$duration" \
    "$(jq -n --arg b "$backend" \
      '{backend: $b, aborted: true, reason: "run_backend aborted before write_result"}')"
  return 1
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

# rocksdb and memory backends are independent measurements — failure of
# one does not block the other from running.  || true keeps the script
# alive past either backend's failure.
run_backend_with_fallback rocksdb 30-pod-delete || true

# Optionally run the inmem baseline.
if [[ "${BASELINE_INMEM:-0}" == "1" ]]; then
  log "BASELINE_INMEM=1 — running inmem (memory) baseline"
  teardown_cluster
  # GCS_STORAGE value must be "memory" (not "inmem") to match the C++ kInMemoryStorage constant.
  run_backend_with_fallback memory 30-pod-delete.inmem || true

  # Restore rocksdb cluster so subsequent tasks (D1+) have a working cluster.
  # KubeRay does NOT recreate head pods on PodTemplate changes alone — it only
  # reconciles the spec.  Apply-only would leave the running pod on the memory
  # backend, so we teardown_cluster first to force KubeRay to recreate pods
  # against the new (rocksdb) spec.
  log "restoring rocksdb cluster after inmem baseline..."
  teardown_cluster
  export GCS_STORAGE=rocksdb
  render_manifest "$HERE/../manifests/raycluster.yaml.tmpl" | kubectl apply -f -
  wait_ray_ready "$NAMESPACE" "$CLUSTER_NAME" "$DEPLOY_TIMEOUT_S" \
    || log "WARN: RayCluster did not reach Ready after rocksdb restore (downstream tests may fail)"
  log "rocksdb cluster restored"
fi

log "30-pod-delete done"
