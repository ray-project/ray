#!/usr/bin/env bash
# 10-deploy-cluster.sh — apply the RayCluster manifest, wait for Ready.
# Idempotent. Backend is controlled by GCS_STORAGE env var (rocksdb|inmem).
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
source "$HERE/../lib.sh"
load_env
require_env GCS_STORAGE DEPLOY_TIMEOUT_S

log "deploying RayCluster (backend=$GCS_STORAGE) to ns=$NAMESPACE"
render_manifest "$HERE/../manifests/raycluster.yaml.tmpl" \
  | kubectl apply -f -

wait_ray_ready "$NAMESPACE" rep64-rocksdb "$DEPLOY_TIMEOUT_S" \
  || abort "RayCluster did not reach Ready in ${DEPLOY_TIMEOUT_S}s"

# Sanity: head pod is up, env vars are set.
HEAD_POD="$(kubectl get pod -n "$NAMESPACE" \
  -l ray.io/cluster=rep64-rocksdb,ray.io/node-type=head \
  -o jsonpath='{.items[0].metadata.name}')"
log "head pod: $HEAD_POD"

actual_storage="$(kubectl exec -n "$NAMESPACE" "$HEAD_POD" -- \
  printenv RAY_gcs_storage 2>/dev/null || echo unset)"
[[ "$actual_storage" == "$GCS_STORAGE" ]] \
  || abort "head pod has RAY_gcs_storage=$actual_storage, expected $GCS_STORAGE"

log "RayCluster deployed and verified"
