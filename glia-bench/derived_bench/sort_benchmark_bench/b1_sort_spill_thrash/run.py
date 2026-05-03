"""B1: sort_spill_thrash.

Engages R1 (object-store spill thrash because shuffle working set far
exceeds object store memory) and R5 (in-flight shuffle blocks pegged at
plasma cap).

Mechanism: shrink Ray plasma to 2.5 GiB and run `Dataset.sort(key="c_0")`
on a ~4 GiB working set (16 partitions × 256 MiB). The all-to-all map+
reduce can't fit, so plasma stays at cap and significant spilling occurs.

Run:
    /opt/venv/bin/python run.py

Validation lever: pass --no-spill to shrink the partition payload 8×.
Working set drops below plasma cap; spilled_bytes should drop to ~0.
"""

from __future__ import annotations

import argparse
import os
import sys

# Make sibling _common importable when invoked from the benchmark dir.
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))

import ray  # noqa: E402
from ray.data.context import DataContext  # noqa: E402

from _common import (  # noqa: E402
    DeterministicIntRowDatasource,
    print_summary,
    run_reps,
    summarize,
)


def build_dataset(num_partitions: int, partition_bytes: int, row_bytes: int):
    rows_per_part = partition_bytes // (8 + row_bytes)
    src = DeterministicIntRowDatasource(seed=42)
    return ray.data.read_datasource(
        src,
        override_num_blocks=num_partitions,
        n=rows_per_part * num_partitions,
        row_size_bytes=row_bytes,
    )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--num-partitions", type=int, default=16)
    p.add_argument("--partition-bytes", type=int, default=256 * 1024 * 1024)
    p.add_argument("--row-bytes", type=int, default=100)
    p.add_argument("--object-store-gb", type=float, default=2.5)
    p.add_argument("--num-cpus", type=int, default=16)
    p.add_argument("--reps", type=int, default=3)
    p.add_argument("--warmup", type=int, default=1)
    # Validation: shrink partition_bytes 8× → working set fits in plasma.
    p.add_argument("--no-spill", action="store_true")
    args = p.parse_args()

    if args.no_spill:
        partition_bytes = max(8 * 1024 * 1024, args.partition_bytes // 8)
    else:
        partition_bytes = args.partition_bytes

    ray.init(
        num_cpus=args.num_cpus,
        object_store_memory=int(args.object_store_gb * 1024 ** 3),
        include_dashboard=False,
        log_to_driver=False,
        ignore_reinit_error=True,
    )
    try:
        # Match upstream: 1 GiB target_max_block_size.
        DataContext.get_current().target_max_block_size = 1 * 1024 ** 3

        cfg = (
            f"M={args.num_partitions} part={partition_bytes / 1e6:.0f}MB "
            f"row={args.row_bytes}B store={args.object_store_gb}GiB "
            f"cpus={args.num_cpus} no_spill={args.no_spill}"
        )
        print(f"[B1] config: {cfg}")
        sys.stdout.flush()

        def one_rep():
            ds = build_dataset(args.num_partitions, partition_bytes, args.row_bytes)
            ds = ds.sort(key="c_0")
            ds = ds.materialize()
            return {"rows": ds.count()}

        reps = run_reps(
            "B1.sort_spill_thrash",
            one_rep,
            n_reps=args.reps,
            warmup=args.warmup,
        )
        print_summary(
            "B1.sort_spill_thrash", summarize(reps, skip_warmup=args.warmup)
        )
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
