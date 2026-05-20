"""B2: streaming_split equal-mode (equal=True).

Engages R2 (OutputSplitter dispatch overhead with equal=True): the
splitter's `_can_safely_dispatch` runs an O(N) buffer-requirement
check on every candidate bundle, deferring dispatch until the buffer
is large enough to equalize. Reports `output_splitter_overhead_time`
from the operator's extra_metrics.

Also engages R1 (coord) since equal-mode still routes through the
same coordinator. R2 is a perturbation on top of R1.

Note: at the calibrated scale (256-512 files, N=10), `equal=True`
empirically takes a different code path through the splitter and
the resulting wall is sometimes shorter than `equal=False` (the
fast-path dispatch loop in non-equal mode does a full buffer scan
per bundle even when locality always matches). The wall delta is
itself an R2 signal.

Mirrors the upstream `streaming_split.regular_equal` YAML row.

Run:
    /opt/venv/bin/python b2_streaming_split_equal/run.py
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
    run_streaming_split_once,
    summarize,
)


DEFAULT_DATA_PARENT = os.path.join(os.path.dirname(_HERE), "_data")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--num-files", type=int, default=512)
    p.add_argument("--rows-per-file", type=int, default=8_000)
    p.add_argument("--payload-bytes", type=int, default=4096)
    p.add_argument("--n-workers", type=int, default=10)
    p.add_argument("--num-cpus", type=int, default=20)
    p.add_argument("--object-store-gb", type=float, default=6.0)
    p.add_argument("--reps", type=int, default=3)
    p.add_argument("--warmup", type=int, default=1)
    p.add_argument("--data-dir", default=None)
    args = p.parse_args()

    data_dir = args.data_dir or os.path.join(
        DEFAULT_DATA_PARENT,
        f"ssb_n{args.num_files}_r{args.rows_per_file}_p{args.payload_bytes}",
    )
    print(f"[B2] generating data at {data_dir}")
    sys.stdout.flush()
    make_parquet_dataset(
        data_dir,
        num_files=args.num_files,
        rows_per_file=args.rows_per_file,
        payload_bytes=args.payload_bytes,
        seed=1,
    )

    ray.init(
        num_cpus=args.num_cpus,
        object_store_memory=int(args.object_store_gb * 1024 ** 3),
        include_dashboard=False,
        log_to_driver=False,
        ignore_reinit_error=True,
    )
    try:
        cfg = (f"N={args.n_workers} num_files={args.num_files} "
               f"rows={args.rows_per_file} payload={args.payload_bytes}B "
               f"cpus={args.num_cpus} store={args.object_store_gb}GiB equal=True")
        print(f"[B2] config: {cfg}")
        sys.stdout.flush()

        expected_total_rows = args.num_files * args.rows_per_file

        def one_rep():
            ds = ray.data.read_parquet(data_dir)
            rep = run_streaming_split_once(
                ds, num_workers=args.n_workers, equal=True, early_stop=False,
            )
            # equal=True may drop a tiny remainder; allow ±N rows tolerance.
            assert abs(rep["rows_consumed"] - expected_total_rows) <= args.n_workers, (
                rep["rows_consumed"], expected_total_rows
            )
            return rep

        reps = run_reps("B2.streaming_split_equal", one_rep,
                        n_reps=args.reps, warmup=args.warmup)
        print_summary("B2.streaming_split_equal",
                      summarize(reps, skip_warmup=args.warmup))
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
