# Phase 8 — Recovery time + K8s/cloud validation

**Status:** storage-layer recovery-time numbers captured on this VM's ext4. Reference K8s manifests + cloud sanity wrapper shipped. Full GCS-process recovery + actor-survival deferred (needs Ray Python wheel built from this branch); cloud-volume runs deferred (no cloud VM access, collaborator-driven).
**Branch:** `jhasm/rep-64-poc-1`

## Claim addressed

> "Recovery time is equal to or better than Redis-based FT" + the K8s deployment story (PVC re-attach, head-pod restart) + cloud fsync sanity.

## Method

This phase splits the claim into three independently-verifiable pieces:

1. **Storage-layer recovery time** (cold-open + scan): the lower bound on full GCS recovery. C++ benchmark `recovery_bench` populates a fresh DB in one process, then a *separate* process opens it cold and runs `AsyncGetAll`. Two-process structure ensures we measure cold-open behavior, not handle re-attachment.
2. **K8s deployment scaffolding**: a reference StatefulSet + PVC + headless Service manifest at `rep-64-poc/harness/kind/ray-head-rocksdb.yaml`, annotated with the failure modes it's designed to test. Building the Ray container image from this branch (the missing piece for end-to-end K8s runs) is captured as Phase 8's primary follow-on.
3. **Cloud-volume sanity wrapper**: `rep-64-poc/harness/cloud/cloud_fsync_check.sh` that runs Phase 1 fsync probe + Phase 4 kill-9 harness + Phase 7 microbench + Phase 8 recovery against a cloud-mounted volume. Designed to be handed to a collaborator with EBS / GCE PD access.

## Result

### Storage-layer recovery on probe-verified honest ext4 (this VM)

`rep-64-poc/harness/recovery/results/recovery_ext4.json` (bench binary built `-c opt`):

| State size | Cold open | `AsyncGetAll` scan | First lookup | **Total recovery** |
|---|---|---|---|---|
| 100 entries | 31.9 ms | 1.2 ms | 1.1 ms | **34.2 ms** |
| 1,000 entries | 35.8 ms | 1.9 ms | 1.1 ms | **38.8 ms** |
| 10,000 entries | 72.6 ms | 7.6 ms | 1.1 ms | **81.4 ms** |

Populate-side wall-clocks (informational, measures the writer's sync=true throughput rather than recovery): 0.52 s / 4.27 s / 41.7 s respectively — consistent with Phase 7's 200-ish ops/s sync-write rate.

#### What this tells us

- **Cold open dominates recovery time, not GetAll.** At 10k entries, opening RocksDB takes 73 ms; the full table scan after that is only 7.6 ms. RocksDB's CF discovery + memtable rebuild is the load-bearing cost.
- **Recovery scales sub-linearly with state size.** From 100 → 10,000 entries (100×) the recovery time grows from 34 ms → 81 ms (2.4×). The scan portion grows linearly (1.2 ms → 7.6 ms ≈ 6×), but the open portion is dominated by per-CF + per-WAL fixed costs that don't depend on the data size.
- **Sub-100 ms recovery at 10,000-entry state.** A "very large cluster" recovery scenario in the storage layer is well under the worker-reconnect timeout (default 60 s, set in `RAY_gcs_rpc_server_reconnect_timeout_s`). The full GCS-process recovery time will be **storage-layer-recovery + container-startup + worker-reconnect**, and the storage-layer bound is provably tiny.

### K8s manifest (scaffolding)

`rep-64-poc/harness/kind/ray-head-rocksdb.yaml` — StatefulSet (1 replica) + headless Service + 10Gi PVC mounted at `/data/gcs-state`. Annotated with the four failure-mode scenarios it's designed to test (pod crash + PVC re-attach, node drain, PVC swap detection, fsync-class violation). The `image:` field is a placeholder pending a Ray container image built from this branch — see "What's NOT verified" below.

### Cloud sanity wrapper

`rep-64-poc/harness/cloud/cloud_fsync_check.sh` — runs Phase 1 fsync probe + Phase 4 kill-9 (positive case + ghost-writes negative control) + Phase 7 microbench + Phase 8 recovery against a caller-specified mount point. Substrate-aware: warns if the mount looks like tmpfs / overlay. Outputs JSON files in a layout that maps directly into this repo's `harness/.../results/` dirs once the run is complete. Designed to be runnable by anyone with shell + bazel-built binaries on a cloud VM.

## Recovery-time vs. Redis claim

The REP claims RocksDB recovery is "equal to or better than Redis-based FT." Direct comparison would need a Redis-backed run with the same state shape and workload, which is the Docker Compose harness in Phase 7's release-test tier. We can however bound:

- **Redis-based FT recovery** has to:
  1. Start the Redis container (or wait for it to come back if it crashed too).
  2. The new GCS reads the entire state out of Redis over the network (RPC per get, batched).
  3. Process it into in-memory representations.
- **RocksDB recovery** has to:
  1. Start the GCS process (the same step as above).
  2. Cold-open the local RocksDB (~33–80 ms storage-layer; bounded by Phase 8's numbers).
  3. Process state into in-memory representations (same step as Redis).

The storage-layer cold-open is *strictly faster* than Redis's "wait for Redis to come back + RPC fetch the whole state": no network, no separate process to wait for, no per-table RPC round-trip. The REP's claim is plausibly correct; the head-to-head measurement to confirm sits with the Phase 7 follow-on (Docker Compose harness).

## Skepticism

### What this phase does NOT prove

- **Full GCS-process recovery time end-to-end.** The C++ recovery_bench measures the storage layer, not the full GCS process. Real recovery time = container-restart + GCS process init + storage layer + worker reconnect. The biggest component is most likely the container-restart, which is K8s-substrate-dependent (kind, EKS, GKE all differ).
- **Actor-survival across GCS restart.** The Python harness `rep-64-poc/harness/integration/test_rocksdb_recovery.py` covers this scenario but requires a Ray Python wheel built from this branch. **Recommendation:** track as the highest-priority Phase 8 follow-on. Building the wheel is mechanical (Ray's existing wheel pipeline) but takes time and CI capacity we don't have inline.
- **Cloud-volume substrate.** No cloud VM access here. The `cloud_fsync_check.sh` wrapper is the testing artifact; the data needs a collaborator with EBS / GCE PD + sudo (some of the harnesses' ideal future variants want `drop_caches`).
- **NFS loopback substrate.** Same wrapper would work; we just haven't set up a local NFS server in this iteration. Trivial follow-on.
- **Recovery latency under cold OS page cache.** Phase 8 numbers are warm-cache: the populate phase ran moments before the recover phase, so SST files and WAL are in the page cache. A real head-pod restart on a fresh node has cold cache. Adding a `drop_caches` step between populate and recover would surface SSD-read latency. Needs `sudo`.
- **Memtable size effects.** Recovery here doesn't include memtable-flush + WAL-replay-after-flush dynamics for state larger than the default memtable (~64 MB). 10k × 256 B = 2.5 MB is well under that threshold.
- **`--config=asan-clang` recovery_bench run.** Same toolchain caveat as earlier phases.

### What would invalidate the result

- A real K8s recovery run showing >10 s wall-clock from `kubectl delete pod` to the recovered cluster being usable. That would mean the storage-layer bound here is OK but the system has a different tail-latency dragon (image pull, init container, worker reconnect, actor revival ordering).
- A recovery_bench run on cloud PV showing >1 s storage-layer cold-open at 10k entries. That would mean cloud PV cold-read latency dominates, and the Phase 8 sub-100 ms number is laptop-only.
- A `kubectl describe pod` showing the readiness probe fails for tens of seconds even though the gcs_server log says "recovered." That would mean the readiness probe (defined in this phase's manifest as a TCP socket check) is not actually a ready signal.

### What R-register status changes

- **R7 (Recovery time scales poorly — cold RocksDB open is slow).** **Closed at the storage layer.** Cold open + GetAll for 10k actors is sub-100 ms. Not closed for the full K8s pod-restart path; that requires the wheel-build-pending follow-on.
- **R4 (fsync semantics on K8s persistent volumes).** Unchanged from Phase 4: ext4 verified honest; cloud PV substrates still pending the `cloud_fsync_check.sh` runs.

## Reproducer

Storage-layer recovery numbers (this VM, ext4):

```bash
mkdir -p $HOME/.cache/rep64-recovery rep-64-poc/harness/recovery/results
bazel build --config=ci -c opt //rep-64-poc/harness/recovery:recovery_bench

python3 rep-64-poc/harness/recovery/run_recovery.py \
  --bin bazel-bin/rep-64-poc/harness/recovery/recovery_bench \
  --db-root $HOME/.cache/rep64-recovery \
  --sizes 100,1000,10000 \
  --output rep-64-poc/harness/recovery/results/recovery_ext4.json
```

K8s reproducer (once the Ray image from this branch is available):

```bash
# See rep-64-poc/harness/kind/README.md for the full sequence.
kind create cluster --name rep64-poc
kind load docker-image rayproject/ray:rep64-poc-local --name rep64-poc
sed 's|rayproject/ray:rep64-poc-PLACEHOLDER|rayproject/ray:rep64-poc-local|' \
    rep-64-poc/harness/kind/ray-head-rocksdb.yaml | kubectl apply -f -
```

Cloud reproducer (on a cloud VM with a mounted volume):

```bash
# Build the bazel binaries first on the cloud VM, then:
bash rep-64-poc/harness/cloud/cloud_fsync_check.sh /mnt/data
```

## Pivot decision

**Proceed.** Storage-layer recovery is fast enough that the REP's "recovery time equal to or better than Redis" claim is plausibly correct — formally proving it needs Phase 7's Docker Compose Redis-vs-RocksDB comparison and Phase 8's full-GCS-process variant. The K8s manifest and cloud script ship as scaffolding for the next iteration.

## Next concrete actions

1. **Build a Ray container image from this branch** to unblock the actor-survival end-to-end test. Highest-impact follow-on; the image lets every other Phase 8 deliverable run.
2. **Run `cloud_fsync_check.sh` on EBS or GCE PD** via collaborator. Captures durability + latency on cloud PVs.
3. **`drop_caches`-equipped variant** of recovery_bench — runs on a host with `sudo`, drops the page cache between populate and recover. Captures cold-cache cold-open behavior.
4. **NFS loopback substrate** as a quick local test before cloud runs land.
5. **End-to-end pod-delete recovery timing** once #1 lands. `kubectl delete pod` + `kubectl wait` + `kubectl get actors` cycle, end-to-end wall-clock.
