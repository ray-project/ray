"""B4: sort_driver_refs.

Engages R3 (driver-side O(M·N) object-ref bookkeeping in the pull-based
shuffle scheduler — see `pull_based_shuffle_task_scheduler.py`'s
`caller_memory_usage = input_num_blocks * output_num_blocks *
CALLER_MEMORY_USAGE_PER_OBJECT_REF`).

Mechanism: plasma generous (no spill); run sort with two partition
counts in the same process — M_low (16) and M_high (128) — keeping the
per-partition size constant. M·N grows from 256 → 16384 (64×) while
data volume only grows 8×. Capture driver RSS deltas and driver maxrss
across reps, plus M, N from a signature-forwarding patch on
`PullBasedShuffleTaskScheduler.execute`.

Run:
    RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE=1 /opt/venv/bin/python run.py

Evaluation: compare driver_maxrss_gb_max between the two M groups
(printed in summaries) and confirm M=128 grows substantially over M=16.
The patch's MN field corroborates the regime intensity.
"""

from __future__ import annotations

import argparse
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))

import ray  # noqa: E402
from ray.data.context import DataContext  # noqa: E402

from _common import (  # noqa: E402
    DeterministicIntRowDatasource,
    driver_rss_bytes,
    install_pull_scheduler_patch,
    print_summary,
    run_reps,
    summarize,
)


def build_dataset(num_partitions: int, partition_bytes: int, row_bytes: int):
    rows_per_part = partition_bytes // (8 + row_bytes)
    src = DeterministicIntRowDatasource(seed=21)
    return ray.data.read_datasource(
        src,
        override_num_blocks=num_partitions,
        n=rows_per_part * num_partitions,
        row_size_bytes=row_bytes,
    )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--m-low", type=int, default=16)
    p.add_argument("--m-high", type=int, default=128)
    p.add_argument("--partition-bytes", type=int, default=8 * 1024 * 1024)
    p.add_argument("--row-bytes", type=int, default=100)
    p.add_argument("--object-store-gb", type=float, default=16.0)
    p.add_argument("--num-cpus", type=int, default=16)
    p.add_argument("--reps", type=int, default=3)
    p.add_argument("--warmup", type=int, default=1)
    args = p.parse_args()

    if not os.environ.get("RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE"):
        os.environ["RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE"] = "1"

    ray.init(
        num_cpus=args.num_cpus,
        object_store_memory=int(args.object_store_gb * 1024 ** 3),
        include_dashboard=False,
        log_to_driver=False,
        ignore_reinit_error=True,
    )
    try:
        DataContext.get_current().target_max_block_size = 1 * 1024 ** 3

        sched_state, restore = install_pull_scheduler_patch()
        try:
            for label, M in [("low", args.m_low), ("high", args.m_high)]:
                cfg = (
                    f"label={label} M={M} part={args.partition_bytes / 1e6:.0f}MB "
                    f"row={args.row_bytes}B store={args.object_store_gb}GiB "
                    f"cpus={args.num_cpus}"
                )
                print(f"[B4] config: {cfg}")
                sys.stdout.flush()

                def one_rep(M=M):
                    rss_pre = driver_rss_bytes()
                    ds = build_dataset(M, args.partition_bytes, args.row_bytes)
                    ds = ds.sort(key="c_0")
                    ds = ds.materialize()
                    rss_post = driver_rss_bytes()
                    return {
                        "rows": ds.count(),
                        "M": M,
                        "input_blocks_seen": sched_state.last_input_blocks,
                        "output_blocks_seen": sched_state.last_output_blocks,
                        "MN": sched_state.last_input_blocks
                        * sched_state.last_output_blocks,
                        "rss_delta_during_rep_mb": (rss_post - rss_pre) / 1e6,
                    }

                reps = run_reps(
                    f"B4.sort_driver_refs.M{M}",
                    one_rep,
                    n_reps=args.reps,
                    warmup=args.warmup,
                )
                print_summary(
                    f"B4.sort_driver_refs.M{M}",
                    summarize(reps, skip_warmup=args.warmup),
                )
        finally:
            restore()
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
