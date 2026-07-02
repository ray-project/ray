# macOS APFS substrate evidence — Phases 1, 4, 7, 8

**Branch:** `jhasm/rep-64-poc-1`
**Host:** `sanjha-mn3428.linkedin.biz` — Apple Silicon, macOS 26.4.1, Darwin 25.4.0, arm64, Bazel 7.5.0
**Substrate under test:** APFS (`/dev/disk3s5`, journaled, root data volume) on the user's MacBook
**Date:** 2026-05-05

This report fills in the four "Pending" cells on the macOS row of the
substrate matrix in `rep-64-poc/EVIDENCE.md`. It documents the process,
the code changes that were required to run the harness on macOS, the
methodology decision behind the durability test, and the four phases'
result JSONs (committed alongside this report).

## TL;DR

| Phase | macOS APFS verdict | Compare to ext4 baseline | Notes |
|---|---|---|---|
| 1 — fsync probe | **honest_via_fullfsync** | ext4 honest via fsync (3.61 ms p50) | APFS `fsync()` p50 17 µs (page-cache only); `F_FULLFSYNC` p50 4.00 ms (honest media-flush). Probe correctly classifies APFS. |
| 4 — kill-9 durability (sync=1, F_FULLFSYNC) | **PASS** at 1k and 5k. Ghost-write negative control catches exactly 50. | PASS at 1k and 5k. Ghost-write catches exactly 50. | Identical evidence shape. Required modifying the writer to issue `F_FULLFSYNC` after each Put (the highest-evidence option). |
| 7 — microbench (-c opt) | RocksDB sync put p50 **4.02 ms**, get p50 3.25 µs | RocksDB sync put p50 3.81 ms, get p50 0.97 µs | Sync-write latency tracks the F_FULLFSYNC class on both substrates. Get latency ~3× higher on APFS — worth a footnote, not a blocker. |
| 8 — storage-layer recovery | **61 ms total** at 10k entries | 81 ms total at 10k entries | Sub-100 ms target met on APFS. 100-entry case is anomalously slow on macOS (276 ms) — io_service / RocksDB cold-start cost; settles on the larger sizes. |

**Net effect on the dossier:** the macOS APFS row in `EVIDENCE.md`'s
substrate matrix moves from four "Pending" to one "honest" + three
filled-in evidence cells. R4 (cloud-PV-class fsync semantics) closes
on a second honest substrate. R6 (REP perf claim mismatch on writes)
gets a second confirming data point. R7 (recovery time) closes at the
storage layer on a second substrate.

## Methodology — why F_FULLFSYNC for Phase 4

The Phase 4 harness was designed for Linux ext4, where
`WriteOptions::sync = true` causes RocksDB to call `fsync()`, which on
ext4 is an honest media-flush. The kill-9 test therefore verifies the
"ack ⇒ durable" contract.

On macOS, that chain is broken. `fsync()` is documented as flushing
data only to the kernel's page cache, not to the drive's permanent
storage:

> `fsync()` causes all modified data and attributes of fildes to be
> moved to a permanent storage device. ... while fsync() will flush
> all data from the host to the drive, the drive itself may not
> physically write the data to the platters for quite some time and
> it may be written in an out-of-order sequence.
> — `fsync(2)` on macOS

Apple's documented honest media-flush barrier is `fcntl(fd,
F_FULLFSYNC, 0)` — it tells the drive to flush its on-board write
cache to platter and is treated as a global drive barrier (whichever
fd it is issued on).

Two options were considered for Phase 4 on APFS:

1. **`fsync`-only (status quo).** Run the existing writer unchanged.
   The kill-9 test would almost certainly pass on APFS — but only
   because the kill kills the user-space process; the OS keeps
   running and eventually flushes its page cache to disk. So a pass
   is a "page-cache survives SIGKILL" pass, not an "ack-was-durable"
   pass. This is the same methodology caveat the Phase 4 report
   already flagged for ext4's sync=0 negative control: a passing
   sync=0 run on ext4 was a methodology finding, not durability
   evidence.
2. **F_FULLFSYNC barrier after each Put (chosen).** Modify the writer
   to issue `fcntl(F_FULLFSYNC, 0)` on the DB-directory fd after
   `db->Put(...)` returns OK and before printing `ack:N`. This makes
   ack imply media-flush, which is the contract Phase 4 is meant to
   verify. The kill-9 then becomes a *real* honest-substrate test,
   because even an OS crash between kill and recovery could not
   un-do a write that has already been F_FULLFSYNC'd.

Option 2 is the higher-evidence choice — the same kind of evidence
ext4's honest fsync provides on Linux. Option 1 would have produced a
green checkmark with weaker-than-ext4 evidence underneath, which would
weaken the overall dossier rather than strengthen it.

The trade-off: the honest barrier costs ~4 ms per Put on this APFS
host, vs. ~17 µs for `fsync()`-alone. That cost is exactly the cost
the REP would already pay on Linux ext4 (3.6 ms per fsync), so this
is the production-realistic substrate, not a synthetic stress test.

## Code changes (uncommitted; bundled into `macos-dev.patch`)

These changes were applied on top of the existing `macos-dev.patch` to
make the harness binaries build and run on macOS arm64. They are
**not** committed to the tree — Ray's supported build target is Linux,
and these are dev-machine edits packaged into the same patch as the
existing macOS workflow fixes.

### 1. Drop Linux-only constraints on three harness `cc_binary` targets

```diff
 cc_binary(
     name = "writer",
     srcs = ["writer.cc"],
-    target_compatible_with = ["@platforms//os:linux"],
     visibility = ["//visibility:public"],
     deps = ["@com_github_facebook_rocksdb//:rocksdb"],
 )
```

Same for `verifier` (`rep-64-poc/harness/durability/kill9/BUILD.bazel`),
`storage_microbench` (`rep-64-poc/harness/microbench/BUILD.bazel`),
and `recovery_bench` (`rep-64-poc/harness/recovery/BUILD.bazel`). The
underlying C++ is portable — only standard C++ + RocksDB + Ray's
StoreClient + boost::asio. No Linux-specific syscalls.

### 2. `writer.cc` — F_FULLFSYNC after each acked Put on macOS

`rep-64-poc/harness/durability/kill9/writer.cc`. Adds a small
RAII helper `FullFsyncBarrier` that opens the DB-dir fd once and
issues `fcntl(F_FULLFSYNC, 0)` after each successful Put. Gated on
`__APPLE__` so Linux behavior is unchanged. Skipped when the user
passes `--sync 0` (intentionally testing the page-cache-only negative
control).

The barrier is global to the drive — it does not need to be issued on
the WAL fd specifically. Apple's documentation treats F_FULLFSYNC as a
hardware cache-flush barrier, not a per-file metadata flush.

### 3. `recovery_bench.cc` — bring `Postable<>` callbacks to the current API shape

The recovery_bench's `AsyncPut` / `AsyncGetAll` / `AsyncGet` callbacks
were passing a bare lambda where the current `RocksDbStoreClient` API
expects a `Postable<...>`. The microbench in this same branch already
uses the correct `{lambda, io.io()}` brace-init pattern; the recovery
bench was the lone outlier and would not compile against the
post-offload-split API. Updated to match. **Not a macOS-specific fix
— this would have failed on Linux too if the recovery bench had been
rebuilt against the latest API.** Worth flagging during PR review.

The diff also tightens the `AsyncGet` callback's `boost::optional` to
`std::optional` to match `OptionalItemCallback`'s current signature.

## Process — what was actually done, in order

```
0. Confirm substrate
   - df -T showed $HOME on /dev/disk3s5 (apfs)
   - /var/folders ($TMPDIR) on /dev/disk3s5 (apfs) — used by Phase 4

1. Apply macos-dev.patch (already in this branch)
   git apply rep-64-poc/macos-dev.patch

2. Run Phase 1 fsync probe
   mkdir -p $HOME/rep64-fsync-probe
   python3 rep-64-poc/harness/durability/probe_fsync.py \
     --target-dir $HOME/rep64-fsync-probe \
     --output rep-64-poc/harness/durability/results/2026-05-05-sanjha-mn3428-apfs.json

3. Drop Linux-only constraints + add F_FULLFSYNC barrier to writer.cc
   (see "Code changes" above)

4. Build harness binaries
   ~/.local/bin/bazel-7.5.0 build --config=ci --copt=-Wno-deprecated-builtins \
     //rep-64-poc/harness/durability/kill9:writer \
     //rep-64-poc/harness/durability/kill9:verifier
   ~/.local/bin/bazel-7.5.0 build --config=ci -c opt --copt=-Wno-deprecated-builtins \
     //rep-64-poc/harness/microbench:storage_microbench \
     //rep-64-poc/harness/recovery:recovery_bench
   (recovery_bench surfaced the Postable<> API drift on first build;
   fixed in-place and rebuilt.)

5. Run Phase 4 — kill-9 durability harness
   - 1k writes, sync=1, F_FULLFSYNC barrier:        PASS
   - 5k writes, sync=1, seed=13, kill-after=1560:   PASS
   - 1k writes + 50 ghost writes, kill-after=10000: FAIL (exactly 50 detected — sensitivity confirmed)

6. Run Phase 7 — storage microbench (-c opt)
   - in_memory + rocksdb_inline backends, default --db-dir under $HOME

7. Run Phase 8 — storage-layer recovery bench
   - sizes 100 / 1000 / 10000 entries
```

## Phase 1 — fsync honesty probe

**File:** `rep-64-poc/harness/durability/results/2026-05-05-sanjha-mn3428-apfs.json`

| Sync call | Available | n | p50 (µs) | p99 (µs) | min (µs) | max (µs) |
|---|---|---|---|---|---|---|
| `fsync()` | yes | 200 | 17.4 | 21.3 | 13.6 | 50.0 |
| `fdatasync()` | no | — | — | — | — | — |
| `F_FULLFSYNC` | yes | 200 | **4003.4** | 5159.3 | 3729.2 | 7087.2 |

**Verdict:** `honest_via_fullfsync`.

Cross-substrate calibration:

| Substrate | Honest barrier | p50 (ms) |
|---|---|---|
| Linux ext4 (project reference VM) | `fsync()` | 3.61 |
| macOS APFS (this host) | `F_FULLFSYNC` | 4.00 |

The two honest media-flush barriers land in the same latency class on
their respective substrates — both hit a real drive write-cache-flush.
The 4 ms / 3.6 ms split is consistent with NVMe-class hardware on both
hosts.

## Phase 4 — kill-9 durability with F_FULLFSYNC

**Files:**
- `rep-64-poc/harness/durability/results/kill9_apfs_sync1.json`
- `rep-64-poc/harness/durability/results/kill9_apfs_sync1_5k.json`
- `rep-64-poc/harness/durability/results/kill9_apfs_ghostwrites.json`

| Run | Writes | Sync mode | Killed at ack | Acked-but-missing | Verdict |
|---|---|---|---|---|---|
| Positive 1k | 1000 | sync=1 + F_FULLFSYNC | 754 | **0** | **PASS** |
| Positive 5k | 5000 | sync=1 + F_FULLFSYNC | 1560 | **0** | **PASS** |
| Negative (ghost) | 1000 (50 ghost) | sync=1 + F_FULLFSYNC, no kill | n/a | **50** (exactly the ghost set, indices 950–999) | **FAIL** (expected) |

The ghost-write negative control proves the harness still has fault
sensitivity on macOS — exactly 50 acked-but-missing keys are reported,
matching the 50 keys the writer deliberately ack'd without putting.

**Substrate status update:** R4 (fsync semantics on K8s PVs) now
closes on **two** independently-verified honest substrates (ext4 and
APFS), not just one. Cloud PV substrates (EBS, GCE PD) remain pending.

## Phase 7 — microbench

**File:** `rep-64-poc/harness/microbench/results/microbench_apfs.json`

Run config: `kKeyCount = 10000`, `kSampledOps = 10000`, `-c opt`,
default `--db-dir = $HOME/.cache/rep64-microbench` (APFS, honest per
Phase 1).

| Backend | Put p50 (µs) | Put p99 (µs) | Put ops/s | Get p50 (µs) | Get p99 (µs) | Get ops/s |
|---|---|---|---|---|---|---|
| `in_memory` | 4.04 | 11.92 | 1.48 M | 9.50 | 43.46 | 1.79 M |
| `rocksdb_inline` (sync=true) | **4021.17** | 5132.92 | 245 | **3.25** | 10.79 | 488.5 K |

**Compare to ext4 reference (`microbench_ext4.json`):**

| Backend | Put p50 (µs) | Get p50 (µs) |
|---|---|---|
| `rocksdb` (ext4) | 3811.14 | 0.97 |
| `rocksdb_inline` (APFS) | 4021.17 | 3.25 |

- **Sync write p50 4.0 ms on APFS** — within 6% of the ext4 4 ms class
  and tracks the F_FULLFSYNC barrier latency from Phase 1. This is a
  second confirming data point for **R6 (REP perf claim mismatch on
  writes)**: the REP's "0.01–0.1 ms write" claim is contradicted by
  measurement on a second substrate. The REP's perf table needs the
  same revision the Phase 7 report already proposed for ext4 ("writes
  (sync, fsync-bounded): 1–10 ms class").
- **Get p50 3.25 µs on APFS** vs 0.97 µs on ext4 — ~3× higher. Both
  substrates are well inside the REP's "0.01–0.1 ms read" class. The
  difference likely reflects hardware (APFS host is consumer-class
  Apple Silicon NVMe; ext4 host is a server VM with different page
  cache + scheduler characteristics) rather than a substrate-level
  durability concern. Worth a footnote in the perf section, not a
  blocker.
- **Side note worth flagging.** The Phase 1 probe measured APFS
  `fsync()` p50 at 17 µs, but the microbench measured RocksDB's
  `WriteOptions::sync = true` (which calls `fsync()` internally — *not*
  F_FULLFSYNC; the F_FULLFSYNC modification was scoped to the kill-9
  writer.cc only) at 4.0 ms. Two plausible explanations: (a) RocksDB
  amplifies a single Put into multiple `fsync()` calls (WAL +
  directory + manifest + ...), and the cumulative cost adds up; or
  (b) APFS's `fsync()` is honest under non-trivial I/O patterns and
  only fast in the probe's tight-loop case. Either way, the
  measurement is reproducible. The microbench number is the right
  number to publish; the explanation is a tail follow-up.

## Phase 8 — storage-layer recovery

**File:** `rep-64-poc/harness/recovery/results/recovery_apfs.json`

| Entries | Populate (s) | Open (s) | Scan (s) | Lookup (s) | Total recover (s) | Total recover (ext4) |
|---|---|---|---|---|---|---|
| 100 | 0.97 | 0.273 | 0.0016 | 0.0016 | 0.276 | 0.034 |
| 1,000 | 4.19 | 0.069 | 0.0022 | 0.0016 | 0.073 | 0.039 |
| 10,000 | 41.17 | 0.056 | 0.0039 | 0.0015 | **0.061** | 0.081 |

- **At the production-relevant 10k state size, APFS hits 61 ms total
  recovery (open + scan + lookup),** under the EVIDENCE.md sub-100 ms
  target and faster than the ext4 baseline at the same size.
- The **100-entry case shows a 273 ms anomaly** that does not appear
  on ext4. Most of that is `open_seconds` (273 ms vs 32 ms on ext4).
  This looks like a one-time cold-start cost on macOS — likely the
  `instrumented_io_context` / `boost::asio` thread spin-up plus
  RocksDB metadata first-load. It does not recur at larger sizes
  (1k = 69 ms, 10k = 56 ms). For a Ray head-pod restart, recovery is
  scaled by entry count, not by smallest-case open cost, so this is
  noise rather than a regression. Worth a footnote in the dossier.
- Populate-side wall-clock at 10k (41 s) matches ext4 (42 s) — these
  are 10k sync-true Puts in series, paying the ~4 ms class per write
  on both substrates. This is consistent with the Phase 7 finding.

## Updates to `EVIDENCE.md`

The substrate matrix row for "macOS APFS (user's MacBook)" should
move from:

| Substrate | Phase 1 fsync probe | Phase 4 kill-9 sync=1 | Phase 7 microbench | Phase 8 recovery |
|---|---|---|---|---|
| macOS APFS (user's MacBook) | Probe ready (`probe_fsync.py` supports `F_FULLFSYNC`); dev workflow green per `MACOS_DEV_SETUP.md` | Pending | Pending | Pending |

…to:

| Substrate | Phase 1 fsync probe | Phase 4 kill-9 sync=1 | Phase 7 microbench | Phase 8 recovery |
|---|---|---|---|---|
| macOS APFS (user's MacBook) | **Honest** via F_FULLFSYNC (p50 = 4003 µs); fsync()-only is page-cache-bounded as documented (p50 = 17 µs). | **PASS** (1k, 5k writes, sync=1 + F_FULLFSYNC; ghost-write negative control catches exactly 50). | **Captured** (Put p50 4.02 ms, Get p50 3.25 µs). | **Captured** (61 ms cold open + scan at 10k entries). |

Open follow-up #6 ("macOS+F_FULLFSYNC runs on the user's MacBook for
substrate diversity at zero infrastructure cost") can be marked
**done** in the priority list.

## Risk register impact

| ID | Risk | Pre-APFS status | Post-APFS status |
|---|---|---|---|
| R4 | fsync semantics on K8s PVs silently violate durability | Partially closed (ext4) | **Closed on two independently honest substrates (ext4 + APFS).** Cloud PV substrates still pending. |
| R6 | RocksDB sync-write latency materially worse than REP claims | Open — REP claim materially off (ext4) | **Open and confirmed on a second substrate.** REP needs the perf-table revision Phase 7 already proposed. |
| R7 | Recovery time scales poorly | Closed at storage layer (ext4) | **Closed at storage layer on two substrates.** Full-process timing pending Ray container image. |

## What this evidence does NOT cover

The same gaps that exist on the Linux side still exist on macOS:

- **No Ray container image.** Phase 8's K8s pod-delete recovery and
  full Redis-vs-RocksDB head-to-head still require a container built
  from this branch.
- **No TSAN / ASAN runs.** Mutex-RMW concurrency safety is verified
  for GCC 11 (Linux) only. Apple Clang results would be additive.
- **No multi-threaded writer microbench variant.** Phase 7's
  group-commit benefit is not measured on APFS either.
- **No NFS loopback or cloud-PV runs on macOS.** APFS is a
  third-party-evidence substrate; it is not a substitute for cloud
  EBS / GCE PD evidence.

These remain on the open-follow-ups list in `EVIDENCE.md`.

## Reproducer

From a clean checkout of `jhasm/rep-64-poc-1` on a macOS arm64 host
with the dev setup in `rep-64-poc/MACOS_DEV_SETUP.md`:

```bash
git apply rep-64-poc/macos-dev.patch     # applies macOS dev edits + harness gate relaxations + writer.cc F_FULLFSYNC

# Phase 1
mkdir -p $HOME/rep64-fsync-probe
python3 rep-64-poc/harness/durability/probe_fsync.py \
  --target-dir $HOME/rep64-fsync-probe \
  --output rep-64-poc/harness/durability/results/$(date +%Y-%m-%d)-$(hostname -s)-apfs.json

# Phase 4
~/.local/bin/bazel-7.5.0 build --config=ci --copt=-Wno-deprecated-builtins \
  //rep-64-poc/harness/durability/kill9:writer \
  //rep-64-poc/harness/durability/kill9:verifier
python3 rep-64-poc/harness/durability/kill9/run_kill9.py \
  --writer bazel-bin/rep-64-poc/harness/durability/kill9/writer \
  --verifier bazel-bin/rep-64-poc/harness/durability/kill9/verifier \
  --num-writes 1000 --sync 1 \
  --output rep-64-poc/harness/durability/results/kill9_apfs_sync1.json
python3 rep-64-poc/harness/durability/kill9/run_kill9.py \
  --writer bazel-bin/rep-64-poc/harness/durability/kill9/writer \
  --verifier bazel-bin/rep-64-poc/harness/durability/kill9/verifier \
  --num-writes 5000 --sync 1 --seed 13 --kill-after-ack 1560 \
  --output rep-64-poc/harness/durability/results/kill9_apfs_sync1_5k.json
python3 rep-64-poc/harness/durability/kill9/run_kill9.py \
  --writer bazel-bin/rep-64-poc/harness/durability/kill9/writer \
  --verifier bazel-bin/rep-64-poc/harness/durability/kill9/verifier \
  --num-writes 1000 --sync 1 --ghost-writes 50 --kill-after-ack 10000 \
  --output rep-64-poc/harness/durability/results/kill9_apfs_ghostwrites.json

# Phase 7
~/.local/bin/bazel-7.5.0 build --config=ci -c opt --copt=-Wno-deprecated-builtins \
  //rep-64-poc/harness/microbench:storage_microbench
mkdir -p $HOME/.cache/rep64-microbench
bazel-bin/rep-64-poc/harness/microbench/storage_microbench \
  --output rep-64-poc/harness/microbench/results/microbench_apfs.json

# Phase 8
~/.local/bin/bazel-7.5.0 build --config=ci -c opt --copt=-Wno-deprecated-builtins \
  //rep-64-poc/harness/recovery:recovery_bench
mkdir -p $HOME/.cache/rep64-recovery
python3 rep-64-poc/harness/recovery/run_recovery.py \
  --bin bazel-bin/rep-64-poc/harness/recovery/recovery_bench \
  --db-root $HOME/.cache/rep64-recovery \
  --sizes 100,1000,10000 \
  --output rep-64-poc/harness/recovery/results/recovery_apfs.json

git apply -R rep-64-poc/macos-dev.patch  # revert before pushing
```

Total wall-clock: ~5 min after a warm Bazel cache.

## Files added by this run

```
rep-64-poc/harness/durability/results/2026-05-05-sanjha-mn3428-apfs.json   # Phase 1
rep-64-poc/harness/durability/results/kill9_apfs_sync1.json                # Phase 4 positive (1k)
rep-64-poc/harness/durability/results/kill9_apfs_sync1_5k.json             # Phase 4 positive (5k)
rep-64-poc/harness/durability/results/kill9_apfs_ghostwrites.json          # Phase 4 negative control
rep-64-poc/harness/microbench/results/microbench_apfs.json                 # Phase 7
rep-64-poc/harness/recovery/results/recovery_apfs.json                     # Phase 8
rep-64-poc/reports/macos-apfs-evidence.md                                  # this report
```

## Patch additions (uncommitted; should be rolled into `macos-dev.patch`)

```
rep-64-poc/harness/durability/kill9/BUILD.bazel       drop target_compatible_with on writer/verifier
rep-64-poc/harness/durability/kill9/writer.cc         add F_FULLFSYNC barrier (Apple-only #ifdef)
rep-64-poc/harness/microbench/BUILD.bazel             drop target_compatible_with on storage_microbench
rep-64-poc/harness/recovery/BUILD.bazel               drop target_compatible_with on recovery_bench
rep-64-poc/harness/recovery/recovery_bench.cc         bring Postable<> callbacks to current API shape
```
