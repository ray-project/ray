"""B1: read_parquet_iter_bundles.

Engages R1 (object-store backpressure) and R2 (CPU-bound parquet decode).

Mechanism: generate N synthetic parquet files locally; read with
`read_parquet`, consume via `iter_internal_ref_bundles` (the upstream
test's choice for the read_parquet_* matrix). Cap object_store_memory
small relative to the per-file size so the executor can't keep many
files in flight (R1). With many files and capped CPUs, the read map op
sits at the cpu ceiling (R2).

Validation levers:
- `--no-spill`: shrink num_files 8× → working set fits in object store,
  spilled bytes drop to 0.
- `--low-cpu`: set num_cpus to 1 → wall jumps roughly Nx (R2 confirmation
  that decode is CPU-bound, not IO-bound).

Run:
    /opt/venv/bin/python bench_b1_read_parquet.py
"""

from __future__ import annotations

import argparse
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))

import ray  # noqa: E402

from _common import (  # noqa: E402
    collect_op_backpressure_metrics,
    install_iter_bundles_patch,
    make_parquet_dataset,
    parse_op_remote_total_seconds,
    print_summary,
    run_reps,
    summarize,
)


DEFAULT_DATA_PARENT = os.path.join(os.path.dirname(_HERE), "_data")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--num-files", type=int, default=128,
                   help="number of parquet files (drives R1 + R3)")
    p.add_argument("--rows-per-file", type=int, default=200_000)
    p.add_argument("--payload-bytes", type=int, default=512,
                   help="size of the binary column per row")
    p.add_argument("--num-cpus", type=int, default=8)
    p.add_argument("--object-store-gb", type=float, default=1.0,
                   help="object store cap (drives R1)")
    p.add_argument("--reps", type=int, default=3)
    p.add_argument("--warmup", type=int, default=1)
    p.add_argument("--data-dir", default=None)
    # Validation: shrink data 8× → fits in plasma → no spill.
    p.add_argument("--no-spill", action="store_true")
    # Validation: 1 cpu → R2 effect is visible.
    p.add_argument("--low-cpu", action="store_true")
    args = p.parse_args()

    num_files = args.num_files // 8 if args.no_spill else args.num_files
    num_cpus = 1 if args.low_cpu else args.num_cpus

    data_dir = args.data_dir or os.path.join(
        DEFAULT_DATA_PARENT,
        f"b1_pq_n{num_files}_r{args.rows_per_file}_p{args.payload_bytes}",
    )
    print(f"[B1] generating data at {data_dir}")
    sys.stdout.flush()
    make_parquet_dataset(
        data_dir,
        num_files=num_files,
        rows_per_file=args.rows_per_file,
        payload_bytes=args.payload_bytes,
        seed=1,
    )

    iter_state, restore_iter = install_iter_bundles_patch()

    ray.init(
        num_cpus=num_cpus,
        object_store_memory=int(args.object_store_gb * 1024 ** 3),
        include_dashboard=False,
        log_to_driver=False,
        ignore_reinit_error=True,
    )
    try:
        cfg = (f"N={num_files} rows={args.rows_per_file} "
               f"payload={args.payload_bytes}B cpus={num_cpus} "
               f"store={args.object_store_gb}GiB "
               f"no_spill={args.no_spill} low_cpu={args.low_cpu}")
        print(f"[B1] config: {cfg}")
        sys.stdout.flush()

        expected_total_rows = num_files * args.rows_per_file

        def one_rep():
            iter_state.n_bundles = 0
            iter_state.iter_seconds = 0.0
            ds = ray.data.read_parquet(data_dir)
            total = 0
            for bundle in ds.iter_internal_ref_bundles():
                # Sum rows from metadata; this matches the upstream
                # consumer pattern (iterate without materializing data).
                for _, md in bundle.blocks:
                    total += md.num_rows or 0
            assert total == expected_total_rows, (total, expected_total_rows)
            stats = ds.stats()
            read_remote_s = parse_op_remote_total_seconds(stats, "ReadParquet")
            bp = collect_op_backpressure_metrics(iter_state.captured_executor)
            # Sum submission backpressure across all ops; for read+iter
            # the dominant signal is the read op's submission_bp_s
            # (the rate-limit when consumer can't keep up).
            sub_bp = round(sum(v.get("submission_bp_s", 0) for v in bp.values()), 2)
            out_bp = round(sum(v.get("output_bp_s", 0) for v in bp.values()), 2)
            return {
                "rows": total,
                "n_bundles": iter_state.n_bundles,
                "iter_s": round(iter_state.iter_seconds, 3),
                "read_remote_s": round(read_remote_s, 3),
                "sub_bp_s": sub_bp,
                "out_bp_s": out_bp,
            }

        reps = run_reps("B1.read_parquet_iter_bundles", one_rep,
                        n_reps=args.reps, warmup=args.warmup,
                        num_cpus_target=num_cpus)
        print_summary("B1.read_parquet_iter_bundles",
                      summarize(reps, skip_warmup=args.warmup))
    finally:
        ray.shutdown()
        restore_iter()


if __name__ == "__main__":
    main()
