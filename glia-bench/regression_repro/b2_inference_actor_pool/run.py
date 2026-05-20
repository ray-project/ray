"""B2: actor-pool task→actor selection cost.

The regime fixed by master commit #62309 (PR "Rank actors per node in
a heap"):

- glia (pre-fix) `_find_actor_with_locality(bundle)` iterates ALL M
  alive actors on each preferred node, building a candidate list and
  taking min(). Per call: O(N * M) where N = preferred nodes, M = actors
  per node.
- master `_find_actor_with_locality` keeps a per-node heap and peeks
  the least-busy actor in O(log M) (effectively O(1) since peek).

This selection is invoked once per task submission whenever
`actor_locality_enabled=True` (the default in `ExecutionOptions`).
On a single-node devpod, N=1, so the per-call ratio is M / log(M).

To make the regression bite on devpod we amplify by:
  1. Pushing M (actor count) high — fractional CPU lets us run far
     more actors than physical CPU. M=200 with 0.05 CPU/actor uses 10
     CPUs.
  2. Pushing the number of selection invocations high — many small
     blocks → many task submissions. 20_000 blocks → 20_000 selections.
  3. Driving per-task work toward zero so wall is dispatcher-bound.

Predicted per-call cost ratio: M / log2(M). At M=200, ratio ≈ 26×.
Predicted dispatcher overhead delta: 20_000 × 200 × ~1 μs per actor
iteration = 4 s on glia, vs 20_000 × 8 × ~1 μs = 0.16 s on master. The
~3-4 s gap should be visible against ~5-15 s total wall.

Validity check: same op-graph on both trees; per-op
`task_scheduling_time_s` should differ by an order of magnitude.
"""

from __future__ import annotations

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from _common import run_bench, shutdown_ray  # noqa: E402

import ray  # noqa: E402


# Defaults; overridable via CLI for scaling experiments.
# IMPORTANT: BATCH_ROWS must be large enough that each block exceeds the
# Ray inline-storage threshold (~100 KiB). Below that, blocks are inlined
# in task metadata and have no node_ids, so RefBundle.preferred_locations()
# returns empty and `_find_actor_with_locality` short-circuits — bypassing
# the very code path #62309 fixed. With 1 int64 column, 100_000 rows ≈ 800 KB
# safely above the threshold.
N_BLOCKS_DEFAULT = 5_000
BATCH_ROWS_DEFAULT = 100_000
M_DEFAULT = 200
ACTOR_CPUS_DEFAULT = 0.05
WORK_MS_DEFAULT = 0
N_REPS_DEFAULT = 3


class TinyActor:
    """Pass-through actor; per-task work configured at module level."""

    def __init__(self, work_ms: int = 0) -> None:
        self._work_ms = work_ms
        # Small init delay so all actors are ready before tasks start.
        time.sleep(0.05)

    def __call__(self, batch):
        if self._work_ms > 0:
            time.sleep(self._work_ms / 1000.0)
        return batch


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--n-blocks", type=int, default=N_BLOCKS_DEFAULT)
    parser.add_argument("--batch-rows", type=int, default=BATCH_ROWS_DEFAULT)
    parser.add_argument("--m", type=int, default=M_DEFAULT)
    parser.add_argument("--actor-cpus", type=float, default=ACTOR_CPUS_DEFAULT)
    parser.add_argument("--work-ms", type=int, default=WORK_MS_DEFAULT)
    parser.add_argument("--n-reps", type=int, default=N_REPS_DEFAULT)
    parser.add_argument("--cell", default="default")
    args = parser.parse_args()

    n_blocks = args.n_blocks
    batch_rows = args.batch_rows
    m = args.m
    work_ms = args.work_ms
    actor_cpus = args.actor_cpus

    total_rows = n_blocks * batch_rows

    def workload():
        ds = ray.data.range(total_rows)
        # Force exactly n_blocks blocks → n_blocks downstream task submissions.
        ds = ds.repartition(n_blocks)
        ds = ds.map_batches(
            TinyActor,
            fn_constructor_kwargs={"work_ms": work_ms},
            batch_size=batch_rows,
            num_cpus=actor_cpus,
            concurrency=(m, m),
        )
        for _ in ds.iter_internal_ref_bundles():
            pass
        return ds

    init_kwargs = dict(
        num_cpus=24,
        log_to_driver=False,
        logging_level="WARNING",
        include_dashboard=False,
        object_store_memory=4 * 1024**3,
    )
    run_bench(
        name=f"b2_actor_pool[{args.cell} M={m} N={n_blocks} batch={batch_rows} work={work_ms}ms]",
        init_kwargs=init_kwargs,
        workload=workload,
        n_reps=args.n_reps,
        warmup=True,
    )
    shutdown_ray()


if __name__ == "__main__":
    main()
