"""Release test: track task:deserialize_arguments cost for actor-pool map_batches.

Synthesizes a wide pyarrow Table on the driver (no external data), pushes the
batches through ``from_arrow().map_batches(actor, batch_format='pyarrow',
zero_copy_batch=True)`` with an ``ActorPoolMapOperator``, then reads the Ray
timeline to report p50/p90/max of ``task:deserialize_arguments`` across the
actor pool tasks.

Reported metrics:
    deser_p50_ms, deser_p90_ms, deser_max_ms, deser_task_count
    (plus the auto-recorded RUNTIME from Benchmark.run_fn)
"""

import argparse
from typing import Any, Dict, List

import numpy as np
import pyarrow as pa

import ray
from benchmark import Benchmark
from ray._private.test_utils import wait_for_condition


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--num-batches", type=int, default=8)
    parser.add_argument("--rows-per-batch", type=int, default=256)
    parser.add_argument("--cols", type=int, default=400)
    parser.add_argument("--values-per-cell", type=int, default=128)
    parser.add_argument("--concurrency", type=int, default=2)
    return parser.parse_args()


def make_batch(rows: int, cols: int, values_per_cell: int, seed: int) -> pa.Table:
    """Build a wide pyarrow Table with list<float32> columns.

    Bytes per row ≈ cols * values_per_cell * 4. With defaults
    (400 * 128 * 4 = ~205 KB/row, 256 rows ⇒ ~52 MB/batch).
    """
    rng = np.random.default_rng(seed)
    arrays: Dict[str, pa.Array] = {"id": pa.array(np.arange(rows, dtype=np.int64))}
    offsets = pa.array(
        np.arange(0, (rows + 1) * values_per_cell, values_per_cell, dtype=np.int32)
    )
    for i in range(cols):
        flat = pa.array(
            rng.standard_normal(rows * values_per_cell).astype(np.float32),
            type=pa.float32(),
        )
        arrays[f"f{i:04d}"] = pa.ListArray.from_arrays(offsets, flat)
    return pa.table(arrays)


_UDF_CLASS_NAME = "MinimalActor"


def collect_deser_stats(events: List[Dict[str, Any]]) -> Dict[str, float]:
    """Extract task:deserialize_arguments durations (ms) for our MinimalActor.

    Two stable hooks make this robust:

    1. ``"task:deserialize_arguments"`` is a Cython literal in
       ``python/ray/_raylet.pyx:1804`` and is covered by
       ``src/ray/core_worker/tests/task_event_buffer_test.cc``.
    2. We scope to tids where the *UDF class name* (``MinimalActor``, defined
       in this file) appears in the event cat. Ray Data's actor wrapper
       embeds the UDF name in the cat as ``task::MapWorker(MapBatches(MinimalActor))...``
       (see ``actor_pool_map_operator.py``). Since ``MinimalActor`` is a
       symbol we own, this scoping survives Ray Data renaming its wrapper
       class (which has happened before: ``_MapWorker`` -> ``MapWorker(...)``).

    Unscoped matching (every ``task:deserialize_arguments`` event in the
    timeline) is wrong: StatsActor / AutoscalingCoordinator / ActorLocationTracker
    contribute many cheap deser events that distort p50.
    """
    udf_tids = {
        e["tid"]
        for e in events
        if isinstance(e, dict) and _UDF_CLASS_NAME in e.get("cat", "")
    }
    durs_ms = sorted(
        e["dur"] / 1000.0
        for e in events
        if isinstance(e, dict)
        and e.get("tid") in udf_tids
        and e.get("cat") == "task:deserialize_arguments"
        and e.get("dur", 0) > 0
    )
    if not durs_ms:
        return {"n": 0, "p50": -1.0, "p90": -1.0, "max": -1.0}
    n = len(durs_ms)
    return {
        "n": n,
        "p50": durs_ms[int((n - 1) * 0.5)],
        "p90": durs_ms[int((n - 1) * 0.9)],
        "max": durs_ms[-1],
    }


class MinimalActor:
    """No-op actor — we only want to measure argument deserialization cost."""

    def __call__(self, batch: pa.Table) -> Dict[str, np.ndarray]:
        return {"n": np.array([batch.num_rows])}


def run_deser_benchmark(args: argparse.Namespace) -> Dict[str, Any]:
    tables = [
        make_batch(args.rows_per_batch, args.cols, args.values_per_cell, seed=i)
        for i in range(args.num_batches)
    ]
    batch_mb = tables[0].nbytes / 1e6
    print(
        f"Synthesized {len(tables)} batches x {args.rows_per_batch} rows "
        f"x {args.cols} cols (~{batch_mb:.1f} MB each)"
    )

    ds = ray.data.from_arrow(tables)
    ds.map_batches(
        MinimalActor,
        batch_size=args.rows_per_batch,
        num_gpus=0,
        concurrency=args.concurrency,
        batch_format="pyarrow",
        zero_copy_batch=True,
    ).materialize()

    # Task profile events are flushed to GCS on a periodic interval
    # (``task_events_report_interval_ms``, default 1000 ms), so events from
    # the last tasks may not be in the timeline immediately after
    # ``materialize()`` returns. Retry until we have at least one
    # ``task:deserialize_arguments`` event per expected map_batches task.
    # If they never arrive, ``wait_for_condition`` raises and the test fails
    # loudly — that is also our guard against the timeline event name or
    # actor-class-name embedding changing under us.
    stats: Dict[str, float] = {}

    def _events_arrived() -> bool:
        stats.update(collect_deser_stats(ray.timeline()))
        return stats["n"] >= args.num_batches

    wait_for_condition(_events_arrived, timeout=20)
    print(f"Deser stats (ms): {stats}")

    return {
        "deser_p50_ms": stats["p50"],
        "deser_p90_ms": stats["p90"],
        "deser_max_ms": stats["max"],
        "deser_task_count": stats["n"],
        "batch_size_mb": batch_mb,
    }


def main() -> None:
    args = parse_args()
    ray.init()
    benchmark = Benchmark()
    benchmark.run_fn("from_arrow_map_batches_actor", run_deser_benchmark, args)
    benchmark.write_result()


if __name__ == "__main__":
    main()
