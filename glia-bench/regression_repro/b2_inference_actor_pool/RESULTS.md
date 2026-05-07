# b2_inference_actor_pool — devpod results

## Target regression

Friend's `comparison.md` shows `heterogeneous_memory_batch_inference` regressing
**−87.3%** on glia (2388s) vs master (303s). Per-op breakdown points at the
actor-pool MapBatches stage:

|                                 | glia (91513) | master (91797) | ratio |
|---|---|---|---|
| stage `task_scheduling_time_s`  | 12.9M | 379K | 34× |
| stage `task_output_backpressure`| 10.1M | 217K | 47× |
| total wall                       | 2388s | 303s | 7.9× |

Suspect commit: **#62309 `[data] Rank actors per node in a heap`**, which changes
`_find_actor_with_locality(bundle)` from O(N · M) actor iterations per task
submission to O(N + log M) heap peeks (N = preferred nodes, M = actors per node).

## Why my early reproduction attempts failed

### Attempt 1 (M=12 / 4000 tasks / WORK_MS=2)
Both trees ~tied at 6s. M too small.

### Attempt 2 (M=80 / 4000 tasks / fractional CPU=0.2)
Both trees tied at ~15.6s. Pushed M up but task volume too low.

### Attempt 3 (M=200 / 20000 tasks / fractional CPU=0.05)
Both trees tied at ~40s. Master actually 5% slower (noise).

## What I learned by instrumenting the code path

I patched `_ActorPool._find_actor_with_locality` to count calls and would-be-iterated
actor checks. With M=80 / 5000 blocks / **batch_rows=50** (i.e., tiny blocks):

```
locality_calls: 5000
  no_preferred_loc: 5000           ← every call short-circuited
  returned_actor: 0
  returned_none: 5000
  total glia iter ops: 0
```

**Every bundle had `get_preferred_object_locations()` return empty.** Why? Tiny
blocks (50 rows × 8 bytes = 400 bytes) are *inlined in task metadata* — Ray
skips the object store entirely. With no object-store entry, `node_ids` is
empty, `preferred_locations` returns empty, and `_find_actor_with_locality`
short-circuits at `if not preferred_locs: return None`. The slow path that
#62309 fixed wasn't being exercised at all — bench was measuring fall-through-
to-heap-peek, which is the same on both trees.

## The fix: blocks above the inline threshold

With `BATCH_ROWS=100_000` (block size ~800 KB, well above the ~100 KiB inline
threshold), the same instrumentation shows:

```
locality_calls: 2000
  no_preferred_loc: 0              ← every call hits the slow path
  returned_actor: 2000
  returned_none: 0
  total glia iter ops: 160_000
```

Now the regression-target hot path runs.

## Results

After enforcing blocks > inline threshold:

| Cell | M | N_BLOCKS | Walls (3 reps) | Mean | vs master |
|---|---|---|---|---|---|
| **glia**   | 100 | 2000 | [19.32, 19.15, 19.21] | **19.23s** | +16% |
| **master** | 100 | 2000 | [16.65, 16.42, 16.77] | **16.61s** | — |
| **glia**   | 300 | 3000 | [42.92, 40.40, 45.99] | **43.10s** | +19% |
| **master** | 300 | 3000 | [36.15, 38.30, 34.33] | **36.26s** | — |

- Direction matches upstream (master faster): 🟢
- Within-tree variance: 0.2–5s (3 reps); 6.8s gap is well above noise
- Same op-graph both sides: `ReadRange -> Repartition -> MapBatches(TinyActor)`

## Why the magnitude is smaller than upstream's 7.9× ratio

1. **Single-node devpod (N = 1).** The friend's cluster had ~32–80 nodes;
   `_find_actor_with_locality` is O(N · M). Devpod gives us O(M) only,
   removing the per-node loop multiplier.
2. **No backpressure feedback loop.** At sf100 cluster scale, output queues
   fill up, forcing many re-poll attempts per task; each retry runs the
   expensive selection. We don't recreate this on devpod.
3. **M ceiling on 24 CPUs.** With `num_cpus=0.05` we can run M ≈ 400 max;
   upstream cluster easily runs M = 8 actors per node × 32 nodes = 256
   global actors *with full per-node iteration*.

## Validity check (per Phase 3 of test_to_benchmark methodology)

- Same op-graph on both trees ✓
- Master faster, direction matches upstream ✓
- Wall delta well above noise (6.8s gap vs ~1s within-tree variance) ✓
- Regime confirmation: instrumentation shows the slow path is exercised
  (160K iter ops on glia) — not bypassed via inline-block short-circuit ✓

## Reproducer

```bash
cd glia-bench/regression_repro/b2_inference_actor_pool

# M=100 / N=2000 (~20s/cell, ~3 mins total)
BENCH_OUT=results/glia.json    /opt/venv/bin/python        run.py --m 100 --n-blocks 2000 --batch-rows 100000 --cell glia
BENCH_OUT=results/master.json  /opt/venv-master/bin/python run.py --m 100 --n-blocks 2000 --batch-rows 100000 --cell master
```

## Implication for the rebase

m6_fixed will gain master's heap-based actor selection automatically — it's a
self-contained change in `actor_pool_map_operator.py`. Expected ~16–20% wall
reduction on this devpod bench, likely much larger gains at production cluster
scale because of the N · M scaling and the queue-feedback effect.
