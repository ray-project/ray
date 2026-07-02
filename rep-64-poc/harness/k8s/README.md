# rep-64 POC — Kubernetes harness

End-to-end test harness for the rep-64 RocksDB-backed GCS POC, running on
KubeRay against either a local k3d cluster or a remote Kubernetes cluster.
Covers the K8s-shaped items from `rep-64-poc/COLLABORATORS.md`: pod-delete
recovery (#5), actor-survival E2E (#6), substrate sweep (#4 partial), and
fast-NVMe throughput (#8, stubbed).

## Quickstart (local k3d)

```bash
# Prerequisites: docker, k3d v5+, kubectl, jq, envsubst, helm, python3.
# A pre-built rep-64 image at cr.ray.io/rayproject/ray:nightly-rep64-fix
# (run rep-64-poc/build-image.sh from repo root if you don't have one yet).

cd rep-64-poc/harness/k8s
export HARNESS_ENV_FILE=env/k3d.env

scripts/run-all.sh                  # full sweep + aggregate
cat results/k3d/summary.md          # human-readable findings

scripts/run-all.sh --teardown       # full sweep + tear down k3d cluster
```

That's it. Individual scripts (`scripts/00-setup-k3d.sh`,
`scripts/30-pod-delete.sh`, etc.) can be run directly for iterative
debugging.

## Cleaning up

Three levels of cleanup, pick whichever matches what you want to
preserve:

```bash
# 1. Soft reset — keep the cluster + KubeRay operator, drop test state.
#    Useful between iterations when you're debugging a single test.
kubectl delete raycluster --all -n rep64-poc
kubectl delete pvc        --all -n rep64-poc

# 2. Full teardown — delete the whole k3d cluster (containers, networks,
#    images imported via `k3d image import`, kubeconfig context).
scripts/99-teardown.sh
# or, equivalently, run a sweep then nuke:
scripts/run-all.sh --teardown

# 3. Manual fallback if the script can't reach the cluster (e.g. docker
#    restart left k3d in a half state).
k3d cluster delete rep64
kubectl config delete-context k3d-rep64 || true
```

`99-teardown.sh` reads `KUBE_CONTEXT` from the env file and behaves
differently per tier: for k3d contexts it `k3d cluster delete`s; for
remote contexts it deletes only the RayCluster + PVCs in `$NAMESPACE`
(prompts for confirmation; bypass with `ASSUME_YES=1`) and never
touches the namespace itself.

## Tier model

The harness is designed to run identical scripts against two tiers:

| Tier   | Env file                                                          | Cluster bring-up                                                                | Status                       |
|--------|-------------------------------------------------------------------|---------------------------------------------------------------------------------|------------------------------|
| k3d    | `env/k3d.env`                                                     | `00-setup-k3d.sh`                                                               | implemented; runs end-to-end |
| remote | `env/remote.env` (gitignored; copy from `env/remote.env.example`) | external (operator-managed); `00-check-remote.sh` verifies preflight            | implemented; runs end-to-end |

`KUBE_CONTEXT` in the env file selects which kubectl context the run
attaches to.  Remote-tier operations never create or destroy clusters
— bring-up and tear-down stay the operator's responsibility.

### Switching between tiers

The harness picks a tier purely from `HARNESS_ENV_FILE`; there is no
global "current tier" state.  Switch by exporting a different env file
before running any script:

```bash
# Local k3d (default)
export HARNESS_ENV_FILE=rep-64-poc/harness/k8s/env/k3d.env

# Remote (e.g. prod-ltx1-k8s-1, EKS, GKE) — author env/remote.env first
export HARNESS_ENV_FILE=rep-64-poc/harness/k8s/env/remote.env
```

Every script sources `lib.sh::load_env`, which calls
`kubectl config use-context "$KUBE_CONTEXT"` as a side effect.  That
means running any harness script silently flips your shell's kubectl
context to whatever the env file specifies — convenient for the
harness, surprising if you have other kubectl work in flight.  Switch
back explicitly when done:

```bash
kubectl config use-context prod-ltx1-k8s-1   # or whatever you were on
kubectl config current-context               # confirm
```

For full remote-tier workflow (preflight + deploy + tests +
aggregate), see "Reproducing on a different cluster" below.

### Why k3d, not kind

The local tier was originally kind, but kind v0.25 fails on Mariner
cgroup-v1 hosts: kubelet can't create the nested systemd slices it
expects (`cgroup [kubelet kubepods] has some missing paths`).  k3d
(k3s in Docker) has a much smaller cgroup footprint and works on the
same hosts.  All scripts were written tier-agnostic — switching to
remote (EKS, GKE, or any managed K8s) is a one-line edit to
`env/remote.env`.

## Scripts catalog

| Script                              | Purpose                                          |
|-------------------------------------|--------------------------------------------------|
| `00-setup-k3d.sh`                   | Create k3d cluster + KubeRay operator (k3d only) |
| `10-deploy-cluster.sh`              | Apply RayCluster CR + PVC                        |
| `20-actor-survival.sh`              | COLLAB #6 — detached actors survive disconnect   |
| `30-pod-delete.sh`                  | COLLAB #5 — head pod kill + recovery; runs both rocksdb and inmem baselines |
| `40-substrate-sweep.sh`             | COLLAB #4 — fsync honesty per StorageClass       |
| `50-fast-storage.sh` (stub)         | COLLAB #8 — pipelined throughput (deferred)      |
| `99-teardown.sh`                    | Delete RayCluster, PVC, k3d cluster              |
| `run-all.sh`                        | Orchestrator — runs everything, then aggregates  |

Tests collect a JSON envelope per run into `$RESULTS_DIR/<test>-<ts>.json`
(see `lib.sh::write_result`); `python/aggregate.py` reads the latest
JSON per test and emits `summary.md`.

## Interpreting results

`summary.md` has two sections:

- **Tests** — one row per test with status, duration, and a key metric.
- **Findings** — explicit list of metric thresholds that were missed
  *or* tests that didn't complete.  Empty Findings = clean run.

Spec thresholds (from `python/aggregate.py:THRESHOLDS`):

| Metric                | Spec target | What it gates                          |
|-----------------------|-------------|----------------------------------------|
| `pod_restart_s`       | ≤ 30 s      | head pod recovers within 30 s          |
| `state_preserved_pct` | ≥ 95 %      | ≥ 95 % of actor state preserved        |
| `new_tasks_ok`        | == true     | cluster accepts new tasks after restart |

A finding looks like:

```
| 30-pod-delete.inmem | memory | state_preserved_pct | >= 95 (actor state ≥95% preserved) | 0 | 30-pod-delete.inmem.committed.json |
```

(That row is the expected baseline finding: the in-memory GCS backend
loses all actor state on pod kill — that's the contrast against
rocksdb's 100 %, exactly the point of running both.)

The `source` column always points back to the raw JSON for full context.

## Image story

The deployed image is `cr.ray.io/rayproject/ray:nightly-rep64-fix` —
a locally-built variant of the upstream Ray nightly image that includes
two POC-required C++ patches not yet upstream:

- `gcs_server.cc`: split RocksDB into `/data/gcs/tables/` and
  `/data/gcs/kv/` subdirs (avoids RocksDB's same-process double-open
  ENOLCK).
- `rocksdb_store_client.cc`: mkdir loop for intermediate dirs.

Build via `rep-64-poc/build-image.sh ray` (run from repo root; ~3 min
warm cache, ~50 min cold).  k3d picks it up via `k3d image import`;
remote tiers need it pushed to a registry the cluster can reach.  For
external collaborators, the image is also published at
`ghcr.io/jhasm/ray-rocksdb-poc:rep-64-poc` (public).

### Image registry & pull secrets

For public registries (the default `ghcr.io/jhasm/ray-rocksdb-poc:rep-64-poc`
is public), no extra setup is needed.  For private registries, attach the
pull secret to the namespace's default ServiceAccount so KubeRay-generated
pods inherit it automatically:

```bash
kubectl -n "$NAMESPACE" create secret docker-registry rep64-pull \
  --docker-server=<registry> --docker-username=<user> --docker-password=<token>

kubectl -n "$NAMESPACE" patch sa default \
  -p '{"imagePullSecrets":[{"name":"rep64-pull"}]}'
```

The harness manifest doesn't reference `imagePullSecrets` directly to
keep the template stable across tiers; the ServiceAccount workaround
covers all KubeRay-generated pods (head, workers, and Job pods) in one
patch.

## Known issues

### Path footgun

Run scripts from the repo root, not from `rep-64-poc/harness/k8s/`.
`RESULTS_DIR` in `env/k3d.env` is a relative path; running from inside
the harness directory nests the output one level too deep.

### Resolved: rocksdb 0–50 % recovery was test-methodology, not a durability bug

An earlier baseline showed non-deterministic 0–50 % recovery on the
30-pod-delete test and attributed it to async-write durability gaps.
That attribution was wrong.  Investigation in May 2026 traced the full
write path (`AsyncPut` → `wo.sync = true` fsync → callback → publish
→ driver `ConnectActor` → method dispatch) and ran the substrate-fsync
probe against `/data/gcs` (`verdict=honest`, fsync p50 = 4015 µs).
Both confirmed the GCS state IS durably persisted before `ray.get()`
returns from an actor method.

The actual cause was K8s scheduler placement: Ray's head pod runs a
worker process by default with `num-cpus = HEAD_CPU`, so a fraction of
user actors landed on the head pod's worker.  When the test fired
`kubectl delete --grace-period=0 --force` on the head pod, those
co-located actor processes died with the pod — the variance in
recovery rate was just "what fraction of the 10 happened to be
co-located with the GCS this time".

**Fix**: `headGroupSpec.rayStartParams.num-cpus = 0` (knob `HEAD_RAY_CPU`
in env files).  The K8s container request/limit stays at `HEAD_CPU` so
gcs_server + raylet still get real CPU, but the head's raylet
advertises 0 CPUs to the scheduler, so user actors only land on
worker pods.  Post-fix the test reports a clean 100 % recovery on
rocksdb across runs; inmem baseline still correctly reports 0 %.

### Open finding 2: rocksdb head-pod cold-start exceeds liveness window

On a cold cluster (fresh PVC, first head-pod restart), rocksdb GCS
startup takes ~138 s — far longer than the kubelet liveness probe
allows.  The container is killed mid-startup, triggering a CrashLoop;
recovery eventually happens via repeated container restarts within the
300 s harness timeout.  On a warm cluster the same path takes ~10 s, so
this only manifests on the first restart of a freshly deployed cluster
— which is exactly the production-relevant scenario.  Liveness probe
config likely needs `initialDelaySeconds` raised, or KubeRay's default
probe schedule needs override at the RayCluster level.

## Canonical baseline

`results/canonical/` contains the committed reference baseline (`*.committed.json` per test, plus a `summary.md`).  This is the snapshot the project considers "current known state" — both findings above are visible
in it.  Re-run `run-all.sh` and copy the latest `results/k3d/*.json` over the canonical files to refresh the baseline once a fix lands.

### Substrate sweep

`SUBSTRATE_SWEEP_CLASSES` is empty in `env/k3d.env` because k3d ships
only `local-path`.  Set it on remote tier (e.g.
`SUBSTRATE_SWEEP_CLASSES=local-nvme,local-ssd,nfs-fast-provision-v3`)
to exercise `40-substrate-sweep.sh`.

## Customizing

Every cluster-specific value lives in the env file.  Common knobs:

| Var                       | Purpose                                       |
|---------------------------|-----------------------------------------------|
| `KUBE_CONTEXT`            | which kubectl context to attach to            |
| `NAMESPACE`               | k8s namespace for RayCluster + Jobs           |
| `IMAGE`                   | rep-64 image reference                        |
| `STORAGE_CLASS`           | default PVC class for RayCluster              |
| `SUBSTRATE_SWEEP_CLASSES` | csv list of classes for `40-substrate-sweep`  |
| `FAST_NVME_CLASS`         | NVMe class for `50-fast-storage` (stub)       |
| `BASELINE_INMEM`          | 1 = run inmem baseline alongside rocksdb in C3 |
| `ACTOR_COUNT`             | actors created in the survival/pod-delete tests |
| `POD_RESTART_TIMEOUT_S`   | how long to wait for head pod re-Ready        |
| `RESULTS_DIR`             | where JSONs and summary land                  |

`HARNESS_ENV_FILE` can be set explicitly to override the default lookup;
otherwise scripts expect `env/k3d.env`.

## Reproducing on a different cluster

```bash
cd rep-64-poc/harness/k8s
cp env/remote.env.example env/remote.env

# Edit env/remote.env — required knobs:
#   KUBE_CONTEXT     your kubectl context name
#   NAMESPACE        a namespace where you have RayCluster/PVC/Job rights
#   IMAGE            registry path your cluster can pull from
#                    (default: ghcr.io/jhasm/ray-rocksdb-poc:rep-64-poc)
#   STORAGE_CLASS    pick one from `kubectl get sc`

export HARNESS_ENV_FILE=env/remote.env
scripts/run-all.sh --tier=remote          # preflight + deploy + tests + aggregate
cat results/remote/summary.md             # findings
```

`run-all.sh --tier=remote` first runs `scripts/00-check-remote.sh`,
which verifies kubectl reach, namespace existence, RBAC for the
resources the harness creates, KubeRay CRDs + operator presence, and
the configured StorageClass(es).  It is read-only — it never mutates
the cluster.  A failed preflight aborts the run before
`10-deploy-cluster.sh`.

`scripts/run-all.sh --tier=remote --skip-setup` skips the preflight
when you already know your cluster is properly configured.
`scripts/99-teardown.sh` against a remote context only deletes the
RayCluster + PVCs in `$NAMESPACE` (with a `y/N` prompt unless
`ASSUME_YES=1`); it never removes the namespace itself or touches
unrelated workloads.
