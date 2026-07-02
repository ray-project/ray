#!/usr/bin/env bash
# 99-teardown.sh — clean up. For k3d: deletes the cluster. For remote: deletes
# only the RayCluster + PVC (never the namespace), with a confirmation prompt.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
source "$HERE/../lib.sh"
load_env

case "$KUBE_CONTEXT" in
  k3d-*)
    require_env K3D_CLUSTER_NAME
    log "deleting k3d cluster $K3D_CLUSTER_NAME"
    k3d cluster delete "$K3D_CLUSTER_NAME"
    ;;
  *)
    if [[ "${ASSUME_YES:-0}" != "1" ]]; then
      read -rp "Delete RayCluster + PVC in $NAMESPACE on $KUBE_CONTEXT? [y/N] " ans
      [[ "$ans" =~ ^[Yy]$ ]] || abort "aborted by user"
    fi
    kubectl delete raycluster --all -n "$NAMESPACE" --ignore-not-found
    kubectl delete pvc --all -n "$NAMESPACE" --ignore-not-found
    log "deleted RayClusters and PVCs in ns=$NAMESPACE; namespace itself preserved"
    ;;
esac
