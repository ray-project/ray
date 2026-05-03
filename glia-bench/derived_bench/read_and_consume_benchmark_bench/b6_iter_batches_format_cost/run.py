"""B6: iter_batches_format_cost.

Engages R4 (batch-format conversion cost) by comparing three reps with
identical input but different `batch_format`:
    pyarrow → numpy → pandas

Mechanism: read a synthetic parquet dataset (mixed numeric + binary
cols). For each format, run `ds.iter_batches(batch_size=B, batch_format=F)`
and measure total wall + per-batch wall. The signal is the ratio
between formats — pyarrow is the cheapest (zero-copy), pandas the most
expensive (typed copy + DataFrame construction).

The benchmark intentionally engages all three formats in one process
so the format-specific signal is internally validated (the comparison
is the validity check).

Run:
    /opt/venv/bin/python bench_b6_iter_batches_format.py
"""

from __future__ import annotations

import argparse
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))

import ray  # noqa: E402

from _common import (  # noqa: E402
    make_parquet_dataset,
    print_summary,
    run_reps,
    summarize,
)


DEFAULT_DATA_PARENT = os.path.join(os.path.dirname(_HERE), "_data")
FORMATS = ("pyarrow", "numpy", "pandas")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--num-files", type=int, default=8)
    p.add_argument("--rows-per-file", type=int, default=400_000)
    p.add_argument("--payload-bytes", type=int, default=128)
    p.add_argument("--extra-int-cols", type=int, default=4,
                   help="extra int columns to widen the row")
    p.add_argument("--batch-size", type=int, default=4096)
    p.add_argument("--num-cpus", type=int, default=8)
    p.add_argument("--object-store-gb", type=float, default=2.0)
    p.add_argument("--reps", type=int, default=3)
    p.add_argument("--warmup", type=int, default=1)
    p.add_argument("--data-dir", default=None)
    args = p.parse_args()

    data_dir = args.data_dir or os.path.join(
        DEFAULT_DATA_PARENT,
        f"b6_pq_n{args.num_files}_r{args.rows_per_file}"
        f"_p{args.payload_bytes}_e{args.extra_int_cols}",
    )
    print(f"[B6] generating data at {data_dir}")
    sys.stdout.flush()
    make_parquet_dataset(
        data_dir,
        num_files=args.num_files,
        rows_per_file=args.rows_per_file,
        payload_bytes=args.payload_bytes,
        extra_int_columns=args.extra_int_cols,
        seed=6,
    )

    ray.init(num_cpus=args.num_cpus,
             object_store_memory=int(args.object_store_gb * 1024 ** 3),
             include_dashboard=False, log_to_driver=False,
             ignore_reinit_error=True)
    try:
        cfg = (f"N={args.num_files} rows={args.rows_per_file} "
               f"payload={args.payload_bytes}B extra_int={args.extra_int_cols} "
               f"batch={args.batch_size} cpus={args.num_cpus}")
        print(f"[B6] config: {cfg}")
        sys.stdout.flush()

        # Run each format as its own benchmark "case" with its own warmup
        # and reps, all in this process, sharing the same generated data.
        all_summaries = {}
        for fmt in FORMATS:
            def one_rep(fmt=fmt):
                ds = ray.data.read_parquet(data_dir)
                n_batches = 0
                n_rows = 0
                for batch in ds.iter_batches(batch_size=args.batch_size,
                                             batch_format=fmt):
                    n_batches += 1
                    if fmt == "pyarrow":
                        n_rows += batch.num_rows
                    elif fmt == "pandas":
                        n_rows += len(batch)
                    else:
                        # numpy: dict[str, ndarray]
                        n_rows += len(next(iter(batch.values())))
                return {"fmt": fmt, "n_batches": n_batches, "rows": n_rows}

            name = f"B6.{fmt}"
            reps = run_reps(name, one_rep, n_reps=args.reps,
                            warmup=args.warmup,
                            num_cpus_target=args.num_cpus)
            summary = summarize(reps, skip_warmup=args.warmup)
            print_summary(name, summary)
            all_summaries[fmt] = summary

        # Cross-format ratios — the load-bearing R4 signal.
        print("\n=== B6 cross-format wall ratios ===")
        base = all_summaries["pyarrow"]["wall_seconds_mean"]
        for fmt in FORMATS:
            mean = all_summaries[fmt]["wall_seconds_mean"]
            print(f"  {fmt}: wall_mean={mean:.3f}s ratio_vs_pyarrow={mean/base:.2f}x")
        sys.stdout.flush()
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
