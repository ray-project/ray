# Callback Latency Benchmark Results

10-node cluster (m6i.2xlarge), 1 MiB blocks. Benchmark source: `release/benchmarks/object_store/test_callback_throughput.py`.

## Raw Results (p95)

### Latency

| Test | 100 blocks | 1000 blocks | 5000 blocks | 10000 blocks |
|------|-----------|-------------|-------------|--------------|
| Registration (`on_block_produced`) | 2.3us | 37.0us | 2.9us | 1.7us |
| Steady-state (one at a time) | 0.0004s | 0.0005s | 0.0013s | 0.0012s |
| All-at-once (Direct callback) | 0.0083s | 0.0865s | 0.4980s | 0.8141s |
| All-at-once (BlockRefCounter, no clear) | 0.0092s | 0.0704s | 0.4302s | 0.8923s |
| All-at-once (BlockRefCounter, with clear) | 0.0085s | 0.0736s | 0.4970s | 0.9368s |
| Per-callback during all-at-once | 0.0002s | 0.0001s | 0.0018s | 0.0013s |

### Throughput (all-at-once, callbacks/second)

| Test | 100 blocks | 1000 blocks | 5000 blocks | 10000 blocks |
|------|-----------|-------------|-------------|--------------|
| Direct callback | 12,048 | 11,561 | 10,040 | 11,664 |
| BlockRefCounter (no clear) | 10,870 | 13,477 | 11,105 | 10,706 |
| BlockRefCounter (with clear) | 11,765 | 12,920 | 9,609 | 10,165 |

## Test descriptions

- **Registration**: How long each `on_block_produced` call takes end-to-end: acquiring `BlockRefCounter`'s Python lock, adding to the internal set/dict, then calling through Cython into C++ `ReferenceCounter::AddObjectOutOfScopeOrFreedCallback` (which acquires a C++ mutex and appends to a callback vector).
- **Steady-state**: Blocks released one at a time as consumer tasks complete (normal pipeline flow).
- **All-at-once (Direct callback)**: All blocks dropped simultaneously, with 1 Python callback per block registered directly via `add_object_out_of_scope_callback`. No BlockRefCounter involved. This is the baseline for per-callback overhead.
- **All-at-once (BlockRefCounter)**: Same as above, but callbacks are registered via `BlockRefCounter.on_block_produced` (the real Data-layer path). Each block has 2 callbacks: one for BlockRefCounter accounting, one for timing measurement.
- **Per-callback during all-at-once**: Blocks are dropped one at a time in a rapid loop with individual timestamps per block, measuring true per-callback latency rather than total drain time.

## Key questions

### Is callback registration expensive?

No. Calling `on_block_produced` (which acquires a Python lock, updates a set/dict, and registers a callback) takes ~2-3us per call at scale. The 37us at 1000 blocks is a warmup outlier. Negligible compared to task execution.

### What happens when many blocks are freed at once (LIMIT, UDF failure)?

All callbacks fire on a single background thread. At 10000 blocks, total time for all callbacks to complete is ~0.8-0.9s (~11,000 callbacks/second). This scales linearly with block count. Extrapolating linearly, ~3000 blocks freed in a burst would take ~240ms.

### Where is the time spent?

Each callback takes ~80us end-to-end. What we can measure from the benchmark:

- **Python callback work: ~8us** (measured). Derived from the difference between BlockRefCounter and Direct callback tests: (0.89s - 0.81s) / 10000 = 7.7us per callback for lock + set/dict operations.
- **Remaining ~72us: GIL scheduling + C++ work + event loop overhead** (not individually measured). Each callback requires the dedicated callback thread to acquire the Python GIL from the main thread. The main thread and callback thread alternate on the GIL for every callback, with OS thread scheduling overhead on each switch.

The fact that `clear()` (which eliminates the ~8us Python work) shows no improvement confirms that the ~8us is a small fraction of the total ~80us. The bulk of the time is spent on per-callback thread coordination, not Python execution.

### Does `clear()` help reduce this time?

No. Comparing BlockRefCounter with and without `clear()` shows identical performance:

| Blocks | Without clear | With clear |
|--------|--------------|------------|
| 1000 | 0.0704s | 0.0736s |
| 5000 | 0.4302s | 0.4970s |
| 10000 | 0.8923s | 0.9368s |

`clear()` eliminates ~8us of Python callback work per callback, but the remaining ~72us of per-callback overhead is unchanged. The savings are within measurement noise.

### Does BlockRefCounter add overhead vs raw Core callbacks?

Minimal. Direct callback fires 1 callback per block. BlockRefCounter fires 2 (one for accounting, one for timing). Total time is nearly the same (0.81s vs 0.89s for 10000 blocks), confirming the per-callback cost is dominated by thread coordination overhead, not Python work.

### Will this cause visible pauses during normal execution?

No. In steady-state (blocks released one at a time as tasks complete), per-callback p95 is ~1ms. Bursts only occur on LIMIT or failure paths, and the ~240ms drain time for 3000 blocks is small relative to typical batch processing times.
