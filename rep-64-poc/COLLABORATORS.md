# Helping with the REP-64 POC

If you want to help validate this PoC, this doc is the punch list. Each item
is self-contained: what to test, why it matters, what environment you need,
exact commands, what success looks like, and where to put the results.

The POC was developed on a single Linux VM (ext4, x86_64). The work that
needs help is everything that can't be done from that one box.

**How to share results.** Pick an item, grab the commands, run them, drop
the output JSON or a short writeup in a comment on
[PR #63032](https://github.com/ray-project/ray/pull/63032) — or, if you have
push access to the branch (`jhasm/rep-64-poc-1` on `github.com/jhasm/ray`),
add the result file under the relevant `rep-64-poc/harness/*/results/`
directory and ping me. Either is fine.

For each item, "good" output is described inline. If you see something
different, that's also valuable — please report it.

---

## Triage: pick one based on what you have access to

| Need | Items |
|---|---|
| A Linux box on a different filesystem (xfs, btrfs, zfs, NFS) | [#1](#1-different-filesystem-fsync-probe--microbench) |
| A non-x86 Linux box (arm64) | [#2](#2-arm64-substrate) |
| A macOS dev box | [#3](#3-macos-build-only) |
| Cloud VM with attached PV (EBS, GCE PD, Azure Disk, OpenEBS, etc.) | [#4](#4-cloud-volume-substrate-sweep) |
| K8s cluster (kind / minikube / EKS / GKE) + Docker | [#5](#5-k8s-pod-delete-recovery), [#6](#6-end-to-end-actor-survival) |
| LLVM/Clang toolchain (clang ≥ 14 with ASAN/TSAN runtimes) | [#7](#7-asantsan-runs-of-existing-tests) |
| A heavy Linux box (≥ 16 cores, real NVMe SSD) | [#8](#8-pipelined-throughput-on-fast-storage) |
| A second pair of eyes on the design | [#9](#9-architectural-review-no-running-required) |

---

## 1. Different-filesystem fsync probe + microbench

**Why it matters.** Phase 7's "3.81 ms p50 sync write" is from one
substrate (probe-verified ext4 on a single VM). Phase 1's verdict was
"honest"; on tmpfs we know it lies. We don't know what xfs / btrfs /
zfs / NFS / cloud-PV / encrypted-LUKS / RAID look like in this regime,
and the REP's perf claims need that signal.

**What you need.** Any Linux box with a non-default filesystem mounted
(or, easier: any Linux box where you can `mkfs.xfs` or `mkfs.btrfs` on a
loopback file), Bazel via `bazelisk`, ~10 GB free disk for the cache.

**Run it.**

```bash
# 1. Probe the substrate first — confirm it's not lying about fsync.
mkdir -p rep-64-poc/harness/durability/results
python3 rep-64-poc/harness/durability/probe_fsync.py \
  --target-dir /your/test/mount \
  --output rep-64-poc/harness/durability/results/$(date +%Y-%m-%d)-$(hostname -s)-FSTYPE.json

# 2. Run the microbench on the same path.
bazel build --config=ci -c opt //rep-64-poc/harness/microbench:storage_microbench
mkdir -p /your/test/mount/rep64-microbench rep-64-poc/harness/microbench/results
bazel-bin/rep-64-poc/harness/microbench/storage_microbench \
  --db-dir /your/test/mount/rep64-microbench \
  --output rep-64-poc/harness/microbench/results/$(date +%Y-%m-%d)-$(hostname -s)-FSTYPE.json
```

**What "good" looks like.** Probe verdict `honest` (p50 fsync ≥ ~500 µs).
Microbench: read p50 ~1 µs class on any FS; sync-write p50 should track
the substrate's fsync probe within ~30%.

**What's also useful.** A substrate that says `verdict: lying` and reports
sub-microsecond fsync is *exactly* what we want documented — it tells
operators which mounts will silently lose acked writes.

---

## 2. arm64 substrate

**Why it matters.** RocksDB and `boost::asio` build on arm64, but we
haven't run any of the bench harnesses on aarch64. Recovery and bench
numbers might differ; bazel/RocksDB build flags might need tweaks.

**What you need.** An arm64 Linux box (Graviton, Ampere, Apple Silicon
Linux VM, RPi 4 with 4 GB+ RAM).

**Run it.** The "Quick sanity sweep" from
[`reproducers/README.md`](./reproducers/README.md), then Phase 7 microbench

+ Phase 8 recovery_bench. Results go to
`rep-64-poc/harness/microbench/results/<date>-<host>-arm64.json` and
`rep-64-poc/harness/recovery/results/<date>-<host>-arm64.json`.

**What "good" looks like.** Build green, four sanity tests PASS, microbench
sync-write p50 within an order of magnitude of the ext4 number, recovery
sub-100 ms at 10k entries.

---

## 3. macOS build-only

**Why it matters.** The store-client targets are
`target_compatible_with = ["@platforms//os:linux"]` — they're skipped on
macOS by design (RocksDB on macOS works, but Ray's GCS doesn't run on
macOS in production). What we don't know is whether macOS folks doing a
full Ray build hit any other regression because of our `bazel/BUILD.rocksdb`
or the `@boost` URL change. A pure build-success signal is enough.

**Run it.**

```bash
# A normal Ray build from this branch on macOS — confirms we didn't break it.
bazel build --config=ci //:gcs_server  # or whatever your usual smoke target is
```

**What "good" looks like.** Builds green. If you hit a `@boost` SHA-mismatch
or a `rules_foreign_cc` error on macOS, file it on the PR with the full
error.

---

## 4. Cloud-volume substrate sweep

**Why it matters.** REP-64's whole point is K8s PVs. We've verified ext4
on a non-cloud VM. The interesting question is: does an EBS gp3 / GCE
PD-balanced / Azure Premium SSD honor fsync, and what's the sync-write
p50 on each? Cloud volumes are where ghost writes would actually bite
ops in production.

**What you need.** A cloud VM (any size; t3.medium is fine) with an
attached block volume — EBS, GCE PD, Azure Managed Disk, DO volume,
Hetzner Cloud Volume, etc. Mount it at, say, `/mnt/data`. Build deps
already installed.

**Run it.** A single wrapper script does Phase 1 + 4 + 7 + 8:

```bash
git clone --depth=1 -b jhasm/rep-64-poc-1 https://github.com/jhasm/ray.git
cd ray
# ~10 minutes of cold bazel build the first time.
bash rep-64-poc/harness/cloud/cloud_fsync_check.sh /mnt/data
```

**What "good" looks like.** Probe verdict `honest`, kill-9 positive case
PASS (0 acked-but-missing), kill-9 negative control FAIL with exactly
50 acked-but-missing (fault-sensitivity), microbench numbers in the
1–20 ms sync-write range. Numbers > 100 ms or < 100 µs both warrant a
flag (former = bad PV, latter = lying about fsync).

**What we'd love most.** EBS gp3 + EBS io2 (because they're popular), GCE
PD-balanced (default for GKE), and *any* substrate that lies about fsync
(so we can document it as unsafe in the K8s manifest comments).

---

## 5. K8s pod-delete recovery

**Why it matters.** Phase 8 has the storage-layer recovery time (sub-100 ms
at 10k entries on local ext4). What's missing is the *real* metric: how
long from `kubectl delete pod ray-head` to "Ray cluster serving requests
again" with PV reattach + GCS reload. That number is what operators
actually care about.

**What you need.** A K8s cluster (kind locally is fine, or any real
cluster you have access to), Docker to build a Ray image from this
branch, kubectl. About 20 GB of disk for the image build.

**Run it.**

```bash
# 1. Build a Ray container image from this branch (slow — ~30–60 min cold).
#    Follow https://docs.ray.io/en/latest/ray-contribute/development.html#building-ray
#    on this branch tip.

# 2. Use the manifest at rep-64-poc/harness/kind/ray-head-rocksdb.yaml,
#    update image: to point at your image, then:
kubectl apply -f rep-64-poc/harness/kind/ray-head-rocksdb.yaml

# 3. Submit a small workload (a few actors + tasks). Note time T0.
# 4. kubectl delete pod ray-head. Note time T1.
# 5. When the pod is back and the workload is serving, note time T2.
# 6. Report (T2 - T1) and what state was preserved (T2 - T0 view of the
#    cluster matches T0 view).
```

**What "good" looks like.** Pod restart latency is dominated by image
pull + PV reattach (typically 5–30 s); GCS itself should reload in
< 1 s. Critical signal: actors created before kill must still be there
after restart.

**Bonus.** Try the same with the existing Redis-backed FT setup for
head-to-head numbers — REP-64 claim 9 ("recovery time ≥ Redis-based FT")
is currently 🟡 provisional pending exactly this comparison.

---

## 6. End-to-end actor survival

**Why it matters.** Same Ray container image as #5, but a Python-level
test (not bench-level). Confirms the GCS state (actors, jobs, runtime
envs, etc.) round-trips through `RocksDbStoreClient` without
serialization quirks.

**What you need.** Ray container from #5, ability to run a Python
client against it.

**Run it.**

```bash
# Reproducer skeleton lives at rep-64-poc/harness/integration/test_rocksdb_recovery.py.
# Adapt the IMAGE/HOST values, then:
python3 rep-64-poc/harness/integration/test_rocksdb_recovery.py
```

**What "good" looks like.** All assertions pass: actors created
pre-kill are still alive post-kill, named-actor lookups still work,
detached actors survive.

---

## 7. ASAN/TSAN runs of existing tests

**Why it matters.** The POC's tests pass under default sanitizers but
haven't been run under AddressSanitizer or ThreadSanitizer. ASAN catches
heap UAF / leaks in `RocksDbStoreClient`'s ctor/dtor; TSAN catches data
races we might have missed in the offload path's pool-thread / io_service
handoff.

**What you need.** A Linux box with a Clang toolchain that has matching
ASAN/TSAN runtimes (Ray's CI uses Clang ≥ 14).

**Run it.**

```bash
# Targets to cover:
TARGETS=(
  //src/ray/gcs/store_client/tests:rocksdb_smoke_test
  //src/ray/gcs/store_client/tests:rocksdb_store_client_test
  //rep-64-poc/harness/concurrency:concurrency_test
  //rep-64-poc/harness/store_client_parity:rocksdb_parity_test
)

# ASAN — heap bugs, leaks
bazel test --config=asan-clang "${TARGETS[@]}" --test_output=errors

# TSAN — data races. The concurrency test is the highest-yield one here.
bazel test --config=tsan "${TARGETS[@]}" --test_output=errors
```

**What "good" looks like.** All four targets PASS under both configs.
Any ASAN heap report or TSAN race report is a real bug we want to know
about — please paste the full sanitizer report.

**Known.** I haven't run these because the dev VM doesn't have a working
Clang toolchain with sanitizer runtimes. Would unblock a "ready for review"
signal.

---

## 8. Pipelined throughput on fast storage

**Why it matters.** Phase 7 addendum showed 215 → 535 ops/s (inline →
offload) on commodity ext4 on one VM. The 2.5× ratio comes from
RocksDB group-commit aggregating fsyncs. On real NVMe with deeper
queues that ratio could be 5–10× — and that's the single most important
production-defensibility signal for the offload path. (See Open Question
6 in the PR body.)

**What you need.** A Linux box with a fast local SSD (real NVMe, ideally
not a cloud VM where the hypervisor mediates fsync) and ≥ 16 cores so
the pool-vs-issuer scheduling doesn't bottleneck on context switches.

**Run it.**

```bash
bazel build --config=ci -c opt //rep-64-poc/harness/microbench:storage_microbench

# Sequential: per-call latency comparison (should be ~equal).
bazel-bin/rep-64-poc/harness/microbench/storage_microbench \
  --db-dir /fast/nvme/rep64-microbench \
  --include-offload --io-pool-size 4 --sequential \
  --output rep-64-poc/harness/microbench/results/$(date +%Y-%m-%d)-$(hostname -s)-fast-nvme-sequential.json

# Pipelined: aggregate-throughput comparison (offload should be much higher).
bazel-bin/rep-64-poc/harness/microbench/storage_microbench \
  --db-dir /fast/nvme/rep64-microbench \
  --include-offload --io-pool-size 4 \
  --output rep-64-poc/harness/microbench/results/$(date +%Y-%m-%d)-$(hostname -s)-fast-nvme-pipelined.json

# Try --io-pool-size {1,2,4,8,16} — that's the production-tuning sweep.
```

**What "good" looks like.** Sequential: inline ≈ offload (within 5%).
Pipelined: offload throughput substantially higher than inline; ratio
scales with `--io-pool-size` until either CPU or fsync bandwidth saturates.
The interesting number for the PR is "what's the inflection point on
real NVMe".

---

## 9. Architectural review (no running required)

**Why it matters.** Several decisions in the PoC are deliberately punted
to maintainers; this is where someone who knows Ray internals can move
the needle without touching a build.

**What to look at.**

+ [`rep-64-poc/EVIDENCE.md`](./EVIDENCE.md) — claim-by-claim verdict
  table. Is each verdict's "method" sufficient evidence for the claim?
+ [`rep-64-poc/RISKS.md`](./RISKS.md) — risk register. Anything missing,
  or any row's "current status" stale?
+ [`rep-64-poc/reports/phase-3-skeleton.md`](./reports/phase-3-skeleton.md)
  — the cluster-id-fail-fast deferral writeup. Is the "external authoritative
  source" path the right one, or is there an in-Ray signal we missed?
+ [`rep-64-poc/reports/phase-7-microbench.md`](./reports/phase-7-microbench.md)
  — Phase 7 addendum on inline vs offload. Open Question 6 in the PR body
  is the headline — is keeping `gcs_rocksdb_async_offload = false` the right
  default for a feature merge?
+ The five other open questions in the PR body. Maintainer judgment calls,
  no testing required.

**What "good" looks like.** Comments on the PR (or directly on the relevant
files) saying "this evidence is/isn't sufficient", "this risk row is wrong
in this way", "use this Ray API instead of the K8s downward API".

---

## What's *not* on this list

Things I'd love help with but aren't actionable yet:

+ **Production hardening of the offload path.** The pool currently has no
  backpressure; under saturation the queue-depth latency explodes (the
  bench saw 9.2 s p50 in pipelined mode at queue-fill). That's a real
  follow-on, not a POC test.
+ **Multi-AZ K8s recovery.** Needs a real cluster spanning AZs and a
  StorageClass that allows cross-AZ reattach. KubeRay-operator territory.
+ **`chaos_rocksdb_store_client_test.cc`.** A chaos test mirroring
  `chaos_redis_store_client_test.cc` — needs the chaos test framework
  set up locally; pendable but not yet started.

If you want to take any of these on as a follow-on PR, please ping me
on the main PR — happy to coordinate.
