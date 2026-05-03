"""B4: write_parquet_sink.

Engages R5 (write sink — encode + persist back to filesystem) and R2
(parquet encode CPU during the write).

Mechanism: read a synthetic parquet dataset, then `ds.write_parquet(out)`
to a local directory. The sink op encodes blocks back to parquet (with
default compression) and writes them. With `num_cpus` capped, encode CPU
gates throughput.

Validation lever:
- `--no-write`: replace the write with `iter_internal_ref_bundles` over
  the same input. Wall drops and the Write operator disappears from
  `ds.stats()` — confirms the wall-time delta is attributable to R5.

Run:
    /opt/venv/bin/python bench_b4_write_parquet.py
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import tempfile

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(_HERE))

import ray  # noqa: E402

from _common import (  # noqa: E402
    install_iter_bundles_patch,
    make_parquet_dataset,
    parse_op_remote_total_seconds,
    parse_top_operator_seconds,
    print_summary,
    run_reps,
    summarize,
)


DEFAULT_DATA_PARENT = os.path.join(os.path.dirname(_HERE), "_data")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--num-files", type=int, default=32)
    p.add_argument("--rows-per-file", type=int, default=200_000)
    p.add_argument("--payload-bytes", type=int, default=512)
    p.add_argument("--num-cpus", type=int, default=8)
    p.add_argument("--object-store-gb", type=float, default=2.0)
    p.add_argument("--reps", type=int, default=3)
    p.add_argument("--warmup", type=int, default=1)
    p.add_argument("--data-dir", default=None)
    # Validation: replace write with iter_bundles to subtract R5 cost.
    p.add_argument("--no-write", action="store_true")
    args = p.parse_args()

    data_dir = args.data_dir or os.path.join(
        DEFAULT_DATA_PARENT,
        f"b4_pq_n{args.num_files}_r{args.rows_per_file}_p{args.payload_bytes}",
    )
    print(f"[B4] generating data at {data_dir}")
    sys.stdout.flush()
    make_parquet_dataset(
        data_dir,
        num_files=args.num_files,
        rows_per_file=args.rows_per_file,
        payload_bytes=args.payload_bytes,
        seed=4,
    )

    iter_state, restore_iter = install_iter_bundles_patch()

    # Use a per-rep output dir under the (large) shared overlay.
    _data_root = os.path.join(os.path.dirname(_HERE), "_data")
    os.makedirs(_data_root, exist_ok=True)
    out_root = tempfile.mkdtemp(prefix="b4_write_", dir=_data_root)

    ray.init(num_cpus=args.num_cpus,
             object_store_memory=int(args.object_store_gb * 1024 ** 3),
             include_dashboard=False, log_to_driver=False,
             ignore_reinit_error=True)
    try:
        cfg = (f"N={args.num_files} rows={args.rows_per_file} "
               f"payload={args.payload_bytes}B cpus={args.num_cpus} "
               f"store={args.object_store_gb}GiB no_write={args.no_write}")
        print(f"[B4] config: {cfg}")
        sys.stdout.flush()

        rep_idx = [0]
        def one_rep():
            iter_state.n_bundles = 0
            iter_state.iter_seconds = 0.0
            ds = ray.data.read_parquet(data_dir)
            if args.no_write:
                total = 0
                for bundle in ds.iter_internal_ref_bundles():
                    for _, md in bundle.blocks:
                        total += md.num_rows or 0
                rows = total
                stats = ds.stats()
                write_op_s = 0.0
            else:
                rep_dir = os.path.join(out_root, f"rep_{rep_idx[0]:02d}")
                rep_idx[0] += 1
                ds.write_parquet(rep_dir)
                rows = args.num_files * args.rows_per_file
                stats = ds.stats()
                write_op_s = parse_top_operator_seconds(stats, "Write")
            read_op_s = parse_top_operator_seconds(stats, "ReadParquet")
            return {
                "rows": rows,
                "write_op_s": round(write_op_s, 3),
                "read_op_s": round(read_op_s, 3),
                "write_frac": (
                    round(write_op_s / max(read_op_s + write_op_s, 1e-9), 3)
                    if not args.no_write else 0
                ),
            }

        reps = run_reps("B4.write_parquet_sink", one_rep,
                        n_reps=args.reps, warmup=args.warmup,
                        num_cpus_target=args.num_cpus)
        print_summary("B4.write_parquet_sink",
                      summarize(reps, skip_warmup=args.warmup))
    finally:
        ray.shutdown()
        restore_iter()
        if os.path.exists(out_root):
            shutil.rmtree(out_root, ignore_errors=True)


if __name__ == "__main__":
    main()
