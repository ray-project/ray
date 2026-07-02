#!/usr/bin/env bash
# 20-actor-survival.sh — COLLABORATORS.md item #6.
# Runs python/actor_survival.py inside the Ray cluster head pod, parses the
# METRICS_JSON line from its stdout, and emits a JSON result file.
#
# Uses kubectl cp + kubectl exec instead of a RayJob CR so that we don't
# depend on the branch being pushed to a public git remote (works on local k3d
# AND remote clusters) and avoid KubeRay's job-ID-tracking constraints when
# overriding the submitter command. The actor-survival behaviour under test
# (detached actors surviving a disconnect/reconnect cycle against RocksDB GCS)
# is identical regardless of how the Python is delivered to the cluster.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
source "$HERE/../lib.sh"
load_env
require_env ACTOR_COUNT

START=$(date +%s)

# 1. Locate the head pod.
HEAD_POD="$(kubectl get pod -n "$NAMESPACE" \
  -l ray.io/cluster=rep64-rocksdb,ray.io/node-type=head \
  -o jsonpath='{.items[0].metadata.name}')"
[[ -n "$HEAD_POD" ]] || abort "no head pod found in ns=$NAMESPACE"
log "head pod: $HEAD_POD"

# 2. Copy the Python file into the head pod.
REMOTE_SCRIPT="/tmp/actor_survival_$$.py"
kubectl cp \
  "$HERE/../python/actor_survival.py" \
  "$NAMESPACE/$HEAD_POD:$REMOTE_SCRIPT"
log "copied actor_survival.py to $HEAD_POD:$REMOTE_SCRIPT"

# 3. Run the script inside the head pod and capture output.
log "running actor_survival.py on head pod (ACTOR_COUNT=$ACTOR_COUNT)..."
LOGS="$(kubectl exec -n "$NAMESPACE" "$HEAD_POD" -- \
  env ACTOR_COUNT="$ACTOR_COUNT" \
  python "$REMOTE_SCRIPT" 2>&1)" || true
echo "$LOGS" | tail -20 >&2

# 4. Parse the METRICS_JSON line.
METRICS_LINE="$(echo "$LOGS" | grep '^METRICS_JSON ' | tail -1 || true)"
[[ -n "$METRICS_LINE" ]] || abort "no METRICS_JSON line in output (script crashed before emitting metrics)"
METRICS_JSON="${METRICS_LINE#METRICS_JSON }"

# 5. Determine pass/fail from the detached_survived field.
DETACHED_SURVIVED="$(echo "$METRICS_JSON" | jq -r '.detached_survived')"
STATE_MATCH="$(echo "$METRICS_JSON" | jq -r '.state_match_pct')"
log "detached_survived=$DETACHED_SURVIVED state_match_pct=$STATE_MATCH"

DURATION=$(( $(date +%s) - START ))
if [[ "$DETACHED_SURVIVED" == "true" ]]; then
  write_result 20-actor-survival pass "$DURATION" "$METRICS_JSON"
  log "PASS in ${DURATION}s"
  RC=0
else
  write_result 20-actor-survival fail "$DURATION" "$METRICS_JSON"
  log "FAIL in ${DURATION}s (detached_survived=$DETACHED_SURVIVED)"
  RC=1
fi

# 6. Cleanup the remote script.
kubectl exec -n "$NAMESPACE" "$HEAD_POD" -- rm -f "$REMOTE_SCRIPT" 2>/dev/null || true

exit $RC
