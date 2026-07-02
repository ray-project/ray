#!/usr/bin/env bash
# 00-setup-k3d.sh — create a local k3d cluster and install KubeRay v1.6.0.
# Idempotent. Reads HARNESS_ENV_FILE for KUBE_CONTEXT, NAMESPACE, K3D_CLUSTER_NAME, IMAGE.
#
# Why k3d and not kind: on Mariner cgroup-v1 hosts the standard kind node images can't
# create the nested systemd slices kubelet expects. k3d (k3s in Docker) has a smaller
# cgroup footprint and works on the same hosts. Verified 2026-05-08.

set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
source "$HERE/../lib.sh"
load_env
require_env K3D_CLUSTER_NAME

# 1. Ensure k3d is installed (single-binary download via the upstream installer).
if ! command -v k3d >/dev/null && ! [[ -x "$HOME/.local/bin/k3d" ]]; then
  log "installing k3d v5.7.4 to ~/.local/bin/k3d"
  mkdir -p "$HOME/.local/bin"
  curl -sfL https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh \
    | TAG=v5.7.4 USE_SUDO=false K3D_INSTALL_DIR="$HOME/.local/bin" bash
fi
export PATH="$HOME/.local/bin:$PATH"

# 2. Create the k3d cluster if it doesn't exist (1 server + 1 agent — multi-node so head
#    and workers can land on different nodes; matters for 30-pod-delete.sh authenticity).
if k3d cluster get "$K3D_CLUSTER_NAME" >/dev/null 2>&1; then
  log "k3d cluster $K3D_CLUSTER_NAME already exists"
else
  log "creating k3d cluster $K3D_CLUSTER_NAME (1 server + 1 agent)"
  k3d cluster create "$K3D_CLUSTER_NAME" \
    --servers 1 --agents 1 \
    --wait --timeout 180s
fi

# 3. kubectl context is set by k3d; be explicit.
kubectl config use-context "$KUBE_CONTEXT" >/dev/null

# 4. Wait for kube-system to be Healthy. k3s ships traefik + helm-install jobs that
#    finish as Completed (not Running) — we wait specifically for the long-running
#    components.
log "waiting for kube-system long-running pods to be Ready"
kubectl wait --for=condition=Ready pod -n kube-system \
  -l 'k8s-app in (kube-dns, metrics-server, traefik)' --timeout=120s \
  >/dev/null 2>&1 || log "(some kube-system labels may differ; continuing)"

# 5. Namespace (idempotent).
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# 6. Install KubeRay operator pinned to v1.6.0 (matches prod-ltx1-k8s-1).
#    KubeRay v1.x distributes via Helm only — no single-yaml release asset on GitHub.
if ! command -v helm >/dev/null && ! [[ -x "$HOME/.local/bin/helm" ]]; then
  log "installing helm v3.16.1 to ~/.local/bin/helm"
  TMPHELM="$(mktemp -d)"
  curl -sfL -o "$TMPHELM/helm.tar.gz" \
    https://get.helm.sh/helm-v3.16.1-linux-amd64.tar.gz
  tar -xzf "$TMPHELM/helm.tar.gz" -C "$TMPHELM"
  install -m 0755 "$TMPHELM/linux-amd64/helm" "$HOME/.local/bin/helm"
  rm -rf "$TMPHELM"
fi
export PATH="$HOME/.local/bin:$PATH"

if ! kubectl get deploy -n default kuberay-operator >/dev/null 2>&1; then
  log "installing KubeRay operator v1.6.0 via helm"
  helm repo add kuberay https://ray-project.github.io/kuberay-helm/ 2>/dev/null || true
  helm repo update kuberay >/dev/null
  helm upgrade --install kuberay-operator kuberay/kuberay-operator \
    --version 1.6.0 \
    --namespace default \
    --wait --timeout 5m
fi
kubectl rollout status deploy/kuberay-operator -n default --timeout=180s

# 7. Import the image into k3d nodes.
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  abort "$IMAGE not found in local docker daemon — build it first via build-image.sh"
fi
log "importing $IMAGE into k3d cluster (this takes ~1-2 min for the 2.17 GB image)"
k3d image import "$IMAGE" -c "$K3D_CLUSTER_NAME"

# 8. StorageClass sanity. k3s ships local-path-provisioner; the default SC is `local-path`.
kubectl get storageclass "$STORAGE_CLASS" >/dev/null \
  || abort "StorageClass $STORAGE_CLASS not found in cluster"

log "k3d cluster ready: ctx=$KUBE_CONTEXT ns=$NAMESPACE img=$IMAGE"
