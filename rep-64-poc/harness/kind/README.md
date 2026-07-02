# Phase 8 K8s reproducer (REP-64 POC)

## What's here

- **`ray-head-rocksdb.yaml`** — reference Kubernetes manifest deploying a Ray head pod with the RocksDB GCS backend. StatefulSet + headless Service + PVC-on-RWO. Annotated with the failure-mode scenarios it's designed to test.

## What's NOT here yet

- A Ray container image built from this branch. Until that exists, the manifest's `image:` field is a placeholder. Building one requires:
  1. `bazel build //:ray_pkg` and the rest of the Python wheel pipeline against the REP-64 branch.
  2. Packaging it into a container image based on `rayproject/ray`.
- The `make kind-up` driver script the PLAN flags for one-command repro. Trivial wrapper around `kind create cluster` + `kubectl apply -f`; deferred to the Phase 8 follow-on once the image exists.
- Recovery-time measurement scripts that wrap `kubectl delete pod / wait / read-actor` and time the cycle end-to-end. The C++ `recovery_bench` (this repo, `harness/recovery/`) constrains the lower bound at the storage layer; the K8s-level number adds container-restart + worker-reconnect time on top.

## How to run on kind once the image is available

```bash
# 1. Build a Ray image from this branch (out of scope for this POC iteration).
docker build -t rayproject/ray:rep64-poc-local .

# 2. Bring up kind and load the image.
kind create cluster --name rep64-poc
kind load docker-image rayproject/ray:rep64-poc-local --name rep64-poc

# 3. Apply manifest.
sed 's|rayproject/ray:rep64-poc-PLACEHOLDER|rayproject/ray:rep64-poc-local|' \
    rep-64-poc/harness/kind/ray-head-rocksdb.yaml | kubectl apply -f -
kubectl wait --for=condition=ready pod/ray-head-0 --timeout=120s

# 4. Run the actor-survival test from a Ray client pointed at the cluster.
RAY_REP64_RUN_E2E=1 \
    pytest rep-64-poc/harness/integration/test_rocksdb_recovery.py -s

# 5. Failure-mode probe: simulate head-pod crash, watch recovery.
kubectl delete pod ray-head-0
time kubectl wait --for=condition=ready pod/ray-head-0 --timeout=60s

# 6. Tear down.
kind delete cluster --name rep64-poc
```

## Failure modes this scaffold targets

| REP-named failure mode | How this manifest exercises it |
|---|---|
| Head pod crashes; PVC re-mounts on restart | StatefulSet replica replacement; ext4 PVC re-attaches; RocksDB cold-opens at the same path; the `recovery_bench` numbers in `phase-8-recovery.md` constrain lower bound. |
| Pod re-scheduled to a different node | RWO PVC must release cleanly; documented gotcha for RWX SSD / fileshare scenarios. |
| Different cluster's PVC re-mounted (stale-state risk) | REP "Stale data protection" case. Manifest annotates where the external authoritative `cluster_id` (K8s downward API) would plumb in once the cluster-ID-mismatch fail-fast lands; see `phase-3-skeleton.md` for the deferral rationale. |
| Storage-class fsync semantics violate durability | Out of scope for kind (kind uses local-path provisioner). Cloud reproducer in `harness/cloud/` is the right home for this on real EBS / GCE PD. |
