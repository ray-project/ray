"""B3: sort_merge_hot.

Engages R2 (reducer merge-sort dominates compute because rows have wide
binary payloads, so merge memcpy dominates over key compares).

Mechanism: plasma generous (32 GiB disk-backed via
`RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE=1` to bypass /dev/shm cap; no spill).
M=64 input partitions × 64 MiB each, with row_size_bytes=1024 so each
reducer's M-way merge over wide rows dominates the per-block sort.

Run:
    RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE=1 /opt/venv/bin/python run.py

Validation lever: pass --low-fanout to shrink M from 64 to 8 while
multiplying partition_bytes by 8 (total volume held constant). Reducer
fanout drops 8× → reduce/map ratio should drop substantially.
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
    parse_op_total_seconds,
    print_summary,
    run_reps,
    summarize,
)


def build_dataset(num_partitions: int, partition_bytes: int, row_bytes: int):
    rows_per_part = partition_bytes // (8 + row_bytes)
    src = DeterministicIntRowDatasource(seed=7)
    return ray.data.read_datasource(
        src,
        override_num_blocks=num_partitions,
        n=rows_per_part * num_partitions,
        row_size_bytes=row_bytes,
    )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--num-partitions", type=int, default=64)
    p.add_argument("--partition-bytes", type=int, default=64 * 1024 * 1024)
    p.add_argument("--row-bytes", type=int, default=1024)
    p.add_argument("--object-store-gb", type=float, default=32.0)
    p.add_argument("--num-cpus", type=int, default=16)
    p.add_argument("--reps", type=int, default=3)
    p.add_argument("--warmup", type=int, default=1)
    # Validation: M /= 8, partition_bytes *= 8 → volume constant, fanout 8×
    # smaller.
    p.add_argument("--low-fanout", action="store_true")
    args = p.parse_args()

    if args.low_fanout:
        num_partitions = max(8, args.num_partitions // 8)
        partition_bytes = args.partition_bytes * (args.num_partitions // num_partitions)
    else:
        num_partitions = args.num_partitions
        partition_bytes = args.partition_bytes

    if not os.environ.get("RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE"):
        print(
            "[B3] note: setting RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE=1 because "
            "the configured object store exceeds /dev/shm; this disk-backs "
            "plasma, which is what we want for the no-spill regime."
        )
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

        cfg = (
            f"M={num_partitions} part={partition_bytes / 1e6:.0f}MB "
            f"row={args.row_bytes}B store={args.object_store_gb}GiB "
            f"cpus={args.num_cpus} low_fanout={args.low_fanout}"
        )
        print(f"[B3] config: {cfg}")
        sys.stdout.flush()

        def one_rep():
            ds = build_dataset(num_partitions, partition_bytes, args.row_bytes)
            ds = ds.sort(key="c_0")
            ds = ds.materialize()
            stats = ds.stats()
            map_total = parse_op_total_seconds(stats, r"Suboperator .* SortMap")
            reduce_total = parse_op_total_seconds(stats, r"Suboperator .* SortReduce")
            return {
                "rows": ds.count(),
                "map_total_s": map_total,
                "reduce_total_s": reduce_total,
                "ratio_reduce_over_map": (
                    round(reduce_total / map_total, 2) if map_total > 0 else None
                ),
            }

        reps = run_reps(
            "B3.sort_merge_hot", one_rep, n_reps=args.reps, warmup=args.warmup
        )
        print_summary(
            "B3.sort_merge_hot", summarize(reps, skip_warmup=args.warmup)
        )
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
