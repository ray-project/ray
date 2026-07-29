# REP-64 provenance, expanded: the RocksDB delay surface

Companion to `REP64.md` (PR #64187). That document proved a node-death lost-wakeup
and recommended **F3** (publish node death before persist, node channel only),
explicitly declining to extend it to the actor channel. PR #64702 shipped that
recommendation together with the removal of **F4** (the `sync=false`
"soft-durability" workaround on the `NODE` and `ACTOR` tables).

CI then failed: under the real RocksDB backend,
`test_dynamic_generator_reconstruction_nondeterministic[None-False]` and
`[None-True]` hung deterministically (TIMEOUT 2/2 @ 915.1s, `dev = 0.0s`,
`:ray: core: rocksdb tests` with `--test-env=TEST_GCS_ROCKSDB=1`).

This document records the expanded experiment built to explain that, because the
original provenance had a **design gap**, not merely a wrong answer.

## 1. The gap in the original provenance

Every arm in `REP64.md` injected delay into **one logical path** while the rest of
the GCS stayed fast, and every arm ran on the **in-memory GCS**. Consequently:

1. The actor channel's self-heal (fetch-on-subscribe read of the synchronously-set
   in-memory DEAD) has a hidden precondition — *that read must be fast*. No arm
   ever slowed reads, so no arm could violate it.
2. Real RocksDB slows **every write at once**; each arm slowed exactly one path.
3. Reads and writes share **one bounded I/O pool**
   (`gcs_rocksdb_io_pool_size`, default 4; `AsyncGet` and `AsyncPut` both route
   through `RunIoForKey` → `io_pool_`). Queueing/contention was never modelled.
4. The **shipped combination** — F4 removed + F3 node-only, on the real backend —
   was never an arm; all arms were in-memory, where F4 is a no-op.
5. The harness only modelled `num_returns -= 1`, i.e. `[None-False]`.

## 2. What was built

A single instrumented build off `upstream/master` in which every variable is an
env knob, so one binary expresses every arm.

New instrumentation (all inert unless the env var is set):

| Knob | Where | Purpose |
|---|---|---|
| `RAY_TESTING_GCS_SOFT_DURABLE_TABLES` | `rocksdb_store_client.cc` | override the F4 set; empty string = F4 fully off |
| `RAY_TESTING_GCS_STORE_WRITE_DELAY_MS` | `delay_injecting_store_client` | per-write latency (fsync model) |
| `RAY_TESTING_GCS_STORE_READ_DELAY_MS` | `delay_injecting_store_client` | per-read latency |
| `RAY_TESTING_GCS_STORE_DELAY_TABLES` | `delay_injecting_store_client` | scope the above to specific tables |
| `RAY_TESTING_GCS_STORE_IO_CONCURRENCY` | `delay_injecting_store_client` | bounded pool; a delayed op *occupies* a thread, so reads queue behind writes |
| `RAY_TESTING_GCS_STORE_DELAY_TRIGGER_FILE` | `delay_injecting_store_client` | arm injection only for the death→recovery phase |
| `RAY_TESTING_GCS_ACTOR_PUBLISH_BEFORE_PERSIST` | `gcs_actor_manager.cc` | **new**: "F3 for the actor channel" |

`DelayInjectingStoreClient` injects at the **store-client layer** — exactly where
RocksDB introduces latency — so one component models all four mechanisms.

The trigger file matters: without it, an arm that stalls a table forever also
stalls node *registration*, so the cluster never boots and the arm proves nothing.
(Observed directly: the first matrix run failed with "The current node timed out
during startup" until injection was gated to the post-startup phase.)

## 3. Results (44 runs: 22 arms x both `too_many_returns` values)

Verdicts were identical for `[None-False]` and `[None-True]` in every arm.

| Tier | Arm | Verdict |
|---|---|---|
| A | `A1_control` | FAST (~1.6s, n=9/11) |
| A | `A2_node_writes_stalled` | **PERMANENT-HANG** |
| A | `A3_node_writes_stalled_f3` | FAST |
| B | `B1_actor_writes_stalled` | FAST |
| B | `B2_actor_reads_stalled` | FAST |
| B | `B3_actor_rw_stalled` | FAST |
| B | `B4_actor_rw_stalled_f3actor` | FAST |
| C | `C1_all_writes_slow` (50ms, every table) | FAST |
| C | `C2/C3/C4` (+F3 node / +F3 both / +F5) | FAST |
| D | `D1_pool4_writes_slow` | FAST |
| D | `D2/D3/D5` (pool 4, +F3/+F5 combinations) | FAST |
| D | `D4_pool1_writes_slow_f3node` (pool of 1) | FAST |
| E | `E1_rocksdb_f4_none` (real RocksDB, F4 off, no F3) | FAST |
| E | `E2_rocksdb_f4_node_actor` (master's behavior) | FAST |
| E | `E3_rocksdb_f4_none_f3node` (**exactly what #64702 ships**) | FAST |
| E | `E4/E5/E6` (+F3 actor / +F5 combinations) | FAST |

Tier A reproduces `REP64.md`'s findings at the new injection point, which
validates the instrument.

## 4. What this establishes

**Refuted — the actor channel is not the trigger.** Stalling actor writes, actor
reads, or both *forever*, with F4 off, is FAST (B1–B3). The hypothesis that a
saturated I/O pool defeats the actor fetch-on-subscribe self-heal is **wrong**;
`REP64.md`'s original conclusion stands. Adding F3-for-actor changes nothing
(B4, C3, D3, E4). **Do not ship an actor-channel fix on these grounds.**

**Refuted — plain storage latency is not sufficient.** 50 ms on every write, with
or without pool contention down to a single I/O thread, is FAST (C1–C4, D1–D5).

**Confirmed — the node channel is still the only reproducible trigger, and F3
still fixes it.** A2 hangs; A3 (same stall + F3) is FAST.

**Unreproduced — the CI failure itself.** No arm reproduces it, including the
exact shipped configuration (E3). Two further checks, both on real RocksDB with
F4 off and F3 on:

- the four `..._reconstruction_nondeterministic` variants in isolation —
  **4 passed** (271s);
- the entire `test_generators.py` file (46 tests, replicating the accumulated
  load of the CI job, including `test_generator_oom`'s multi-GB spill) —
  **45 passed, 1 failed**, and the single failure is `test_generator_oom`
  (an environment-specific OOM), *not* a reconstruction test (1520s).

## 5. Tiers F and G: execution-environment pressure

Because the delay surface did not reproduce, pressure was varied instead while
the GCS configuration was held at exactly what #64702 ships. Pressure is applied
only during the death -> recovery phase (same trigger-file gate).

| Arm | Pressure | Verdict (`[None-False]` / `[None-True]`) |
|---|---|---|
| `F1_shipped_cpu_load` | 8 CPU burners | FAST 3.8s / 7.2s |
| `F2_shipped_disk_load` | 3 write+fsync burners | FAST 1.9s / 1.8s |
| `F3_shipped_cpu_disk_load` | 8 CPU + 3 disk | FAST 6.4s / 11.0s |
| `F4_shipped_disk_load_heavy` | 8 disk | FAST 10.1s / 4.5s |
| `F5_f4on_cpu_disk_load` | same as F3, **F4 ON** | FAST 15.8s / 8.5s |
| `F6_shipped_cpu_disk_load_f5` | same as F3, **+F5** | FAST 14.9s / 8.7s |
| `G1_shipped_mem_load` | 12 GB pinned | FAST 1.8s / 1.9s |
| `G2_shipped_mem_cpu_load` | 12 GB + 8 CPU | FAST 4.1s / 5.5s |
| `G3_shipped_mem_cpu_disk_load` | 12 GB + 8 CPU + 3 disk | FAST 13.2s / 13.4s |
| `G4_f4on_mem_cpu_disk_load` | same as G3, **F4 ON** | FAST 13.5s / 14.2s |

Findings:

- Pressure makes the scenario **slower but never wedged**: 1.6s baseline -> at
  worst ~16s, against a 90s arm timeout and the 180s pytest timeout the CI tests
  exceeded. Slowness and the CI hang are not the same phenomenon.
- **CPU is the dominant lever**; disk fsync contention alone barely registers
  (F2 ~1.9s), which further weakens any story that rests on WAL fsync cost.
- **F4 is not protective.** With identical pressure, F4-on was no better than
  F4-off (F5 vs F3, G4 vs G3) — and occasionally slower. This independently
  undercuts keeping the soft-durability workaround for any table.

Two further whole-file replications, both real RocksDB in the shipped config:
running `test_generators.py` alone, and running it concurrently with two other
core test files to emulate `--parallelism-per-worker 3`. Both times the
reconstruction tests **passed**; the only failure was `test_generator_oom`, an
environment-specific OOM on this 31 GB box.

## 6. Conclusion and next step

The RocksDB *delay surface* — per-write fsync on every table, read latency,
bounded-pool contention, per-key strands — **does not explain the CI hang**. The
governing variable is therefore not modelled by delay injection at all.

The untested difference is the CI **execution environment**, not GCS timing:

- `--parallelism-per-worker 3`: three test targets sharing one container's CPU
  and disk, so fsync latency is contended rather than merely present;
- container CPU/memory limits and a slower, possibly network-backed disk;
- the CI job had already spilled ~16.5 GB in `test_generator_oom` before reaching
  the reconstruction tests.

That load dimension has now been added (tiers F and G) and it does **not**
reproduce the hang either. Across seven tiers and ~60 process-isolated runs, the
only configuration that hangs is still A2 — the node channel with its publish
gated behind a stalled write — and F3 still fixes it.

**Next step: stop trying to reproduce, and measure where it actually happens.**
Since the failure only manifests in CI, `RAY_TESTING_REP64_TRACE=1` stamps each
hop of the death path with a microsecond timestamp:

| Stamp | Site |
|---|---|
| `REP64_TRACE gcs_inmemory_dead` | `gcs_node_manager.cc`, in-memory DEAD transition |
| `REP64_TRACE gcs_publish_running` | `gcs_node_manager.cc`, publish handler actually running |
| `REP64_TRACE owner_received_node_dead` | `core_worker.cc`, owner-side `on_node_change` DEAD |

On a healthy local run the whole chain takes **~0.5 ms**
(`gcs_inmemory_dead` -> `gcs_publish_running` +46 us -> `owner_received_node_dead`
+503 us). A CI run of the failing test with this flag set will show which hop is
slow, or which stamp never appears at all:

- no `gcs_inmemory_dead` => the GCS never detected the death (health-check side);
- `gcs_inmemory_dead` but no `gcs_publish_running` => the GCS io_context never ran
  the posted publish (F3's assumption violated);
- both present but no `owner_received_node_dead` => the notification was dropped
  or never delivered, which F3 cannot fix and which F5 (owner-side re-poll) is
  designed for;
- all three present and fast => the hang is downstream of node death entirely,
  and the whole node-channel framing is wrong.

That is a decisive, single-run experiment, whereas further local reproduction
attempts have a demonstrated ~0/60 hit rate.

## 7. Reproducing

```bash
bazel build //:ray_pkg
./python/ray/tests/run_rep64_rocksdb.sh        # all arms
./python/ray/tests/run_rep64_rocksdb.sh E      # ground-truth RocksDB arms only
```

Each arm runs in its own process because the knobs are read via `std::getenv` at
GCS startup and cached in function-local statics.
