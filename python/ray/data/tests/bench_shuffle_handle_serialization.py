"""Micro-benchmark: how expensive is (de)serializing shuffle handle dicts?

Motivation: the external-shuffle reduce path currently aggregates all mapper
handles into a single ``_shared_handles_ref`` (a plasma object holding
deep-copied dicts). Migrating to Ray-Core-lineage-based FT recovery
requires either (a) putting a list of ObjectRefs into ``_shared_handles_ref``,
or (b) passing the list of handle refs directly as reducer task args and
letting Ray track them. Both are equivalent w.r.t. lineage, but they differ
in dispatch bookkeeping and in how the reducer materializes the dicts.

This benchmark answers a bounded question: with N=256 handles, each
containing an index of P=500 partitions (path → [(offset, length), ...]),
how long does *sequential* pickle-based (de)serialization of all N dicts
take? If the answer is milliseconds, the "avoid N×M serialization" argument
for the aggregated shared_ref is not load-bearing — we can prefer whichever
option is cleaner for lineage.

Run::

    python python/ray/data/tests/bench_shuffle_handle_serialization.py
"""

import pickle
import time
from typing import Any, Dict


def _build_handle(
    map_id: int, num_partitions: int, ranges_per_partition: int = 3
) -> Dict[str, Any]:
    """Construct a realistic ShuffleHandle dict.

    Fields match ``external_hash_shuffle_map_task``'s return dict:
    ``shuffle_id``, ``node_id``, ``token``, ``path``, ``index`` (per-partition
    ranges), plus a handful of small scalars.
    """
    index = {
        pid: [
            (pid * 4096 + r * 128, 128) for r in range(ranges_per_partition)
        ]
        for pid in range(num_partitions)
    }
    return {
        "path": f"/tmp/ray_shuffle_external_deadbeef/map_{map_id}.shf",
        "index": index,
        "shuffle_id": "abcdef1234567890",
        "node_id": f"node-{map_id % 16:02d}-cafebabe" * 2,
        "token": "0" * 32,
        "num_partitions": num_partitions,
        "peak_inflight_bytes": 1024 * 1024,
        "total_bytes": 128 * 1024 * 1024,
        "compression": "lz4",
        "decoded_bytes": {pid: 512 * 1024 for pid in range(num_partitions)},
        "schema": None,
    }


def _time(label: str, fn, *, iters: int = 5) -> float:
    """Run fn iters times, report min / mean, return the min in seconds."""
    ts = []
    for _ in range(iters):
        t0 = time.perf_counter()
        fn()
        ts.append(time.perf_counter() - t0)
    ts.sort()
    ms_min = ts[0] * 1000
    ms_mean = (sum(ts) / len(ts)) * 1000
    print(f"  {label:<45s} min={ms_min:8.2f} ms   mean={ms_mean:8.2f} ms")
    return ts[0]


def _bench(num_handles: int, num_partitions: int) -> None:
    print(
        f"\n=== N handles = {num_handles}, partitions/handle = {num_partitions} ==="
    )
    handles = [_build_handle(i, num_partitions) for i in range(num_handles)]

    # rough sizes to sanity-check what we're measuring
    one_pickled = pickle.dumps(handles[0], protocol=pickle.HIGHEST_PROTOCOL)
    all_pickled = pickle.dumps(handles, protocol=pickle.HIGHEST_PROTOCOL)
    print(
        f"  per-handle pickle size : {len(one_pickled):>8d} bytes"
    )
    print(
        f"  aggregated list  size  : {len(all_pickled):>8d} bytes"
    )

    _time(
        "sequential pickle of N handles (one-by-one, distinct)",
        lambda: [
            pickle.dumps(h, protocol=pickle.HIGHEST_PROTOCOL) for h in handles
        ],
    )
    # Sanity check: same object N times. Should be close to the distinct-case
    # cost — pickle doesn't cache across calls, but cache locality helps.
    _one = handles[0]
    _time(
        "sequential pickle of the SAME handle N times",
        lambda: [
            pickle.dumps(_one, protocol=pickle.HIGHEST_PROTOCOL)
            for _ in range(num_handles)
        ],
    )
    _time(
        "aggregated pickle of list-of-N-handles",
        lambda: pickle.dumps(handles, protocol=pickle.HIGHEST_PROTOCOL),
    )

    # Round-trip: pickle + unpickle sequentially.
    def _seq_roundtrip():
        bufs = [
            pickle.dumps(h, protocol=pickle.HIGHEST_PROTOCOL) for h in handles
        ]
        [pickle.loads(b) for b in bufs]

    _time("sequential pickle + unpickle (N times each)", _seq_roundtrip)

    def _agg_roundtrip():
        buf = pickle.dumps(handles, protocol=pickle.HIGHEST_PROTOCOL)
        pickle.loads(buf)

    _time("aggregated pickle + unpickle (once)", _agg_roundtrip)


def _bench_real_dispatch_loop(num_mappers: int, num_reducers: int, num_partitions: int):
    """Simulate the actual shuffle dispatch pattern: build N handle refs,
    then dispatch M reducer tasks each carrying either
    (a) a single shared_ref (aggregated), or
    (b) the full list of N individual refs.

    Measures the wall time of the ``.remote(...)`` calls only (no ray.get) —
    that isolates *dispatch* cost (task-args pickle + Ray Core scheduling),
    which is what the design decision actually turns on.
    """
    import ray

    if not ray.is_initialized():
        ray.init(num_cpus=4, include_dashboard=False, ignore_reinit_error=True)

    @ray.remote
    def _noop_reducer(*args):
        return None

    print(
        f"\n=== Real Ray dispatch: N mappers = {num_mappers}, "
        f"M reducers = {num_reducers}, partitions/handle = {num_partitions} ==="
    )
    handles = [_build_handle(i, num_partitions) for i in range(num_mappers)]
    handle_refs = [ray.put(h) for h in handles]

    # --- Option A: current design (single aggregated shared_ref carrying dict copies)
    # Driver builds it once (cost NOT included in the dispatch loop).
    all_handles = ray.get(handle_refs)
    shared_ref_A = ray.put(all_handles)

    # --- Option B: shared_ref wrapping the list of refs (lineage-preserving)
    shared_ref_B = ray.put(list(handle_refs))

    def _dispatch_A():
        for pid in range(num_reducers):
            _noop_reducer.remote(shared_ref_A, pid)

    def _dispatch_B():
        for pid in range(num_reducers):
            _noop_reducer.remote(shared_ref_B, pid)

    def _dispatch_C():
        for pid in range(num_reducers):
            _noop_reducer.remote(*handle_refs, pid)

    def _dispatch_D():
        # Skip ObjectRef entirely: pickle the whole dict list into each task's
        # args. Same *values* as ``all_handles`` used to build shared_ref_A.
        for pid in range(num_reducers):
            _noop_reducer.remote(all_handles, pid)

    # Warm-up (first dispatch pays a JIT/init tax we don't want to include).
    _dispatch_A()
    _dispatch_B()
    _dispatch_C()
    _dispatch_D()

    _time("Option A: M .remote(shared_ref, pid)  -- dict-copy shared_ref", _dispatch_A, iters=3)
    _time("Option B: M .remote(shared_ref, pid)  -- refs-list shared_ref", _dispatch_B, iters=3)
    _time("Option C: M .remote(*N refs, pid)     -- direct list arg      ", _dispatch_C, iters=3)
    _time("Option D: M .remote(all_handles, pid) -- raw values (no ObjectRef!)", _dispatch_D, iters=3)


if __name__ == "__main__":
    # ---- Pure pickle micro-benchmark (no Ray) -----------------------------
    _bench(num_handles=256, num_partitions=500)
    for n, p in [(64, 500), (256, 100), (1024, 500)]:
        _bench(num_handles=n, num_partitions=p)

    # ---- Real Ray dispatch loop (matches the shuffle dispatch pattern) ---
    _bench_real_dispatch_loop(num_mappers=256, num_reducers=256, num_partitions=500)
    _bench_real_dispatch_loop(num_mappers=64, num_reducers=64, num_partitions=500)
    _bench_real_dispatch_loop(num_mappers=1024, num_reducers=256, num_partitions=500)
