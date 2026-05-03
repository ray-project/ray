"""B2: read_images_pil_decode.

Engages R1 (object-store backpressure with image bytes) and R2 (PIL JPEG
decode is the gating compute stage in `read_images`).

Mechanism: generate N synthetic JPEG files locally with random uniform
pixel content (so JPEG of high-entropy noise stays large; decode work
is real). Read with `read_images(mode="RGB")` (matching upstream FIXME
in the test source). Consume via `iter_internal_ref_bundles`. With
`num_cpus` capped, the read map's PIL decode saturates the cores.

Validation levers:
- `--no-spill`: shrink num_files 8× → object store cap not exceeded,
  spilled bytes drop to 0.
- `--low-cpu`: set num_cpus=1 → wall increases roughly Nx (R2: decode
  is CPU-bound, not IO-bound).

Run:
    /opt/venv/bin/python bench_b2_read_images.py
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
    make_image_dataset,
    parse_op_remote_total_seconds,
    print_summary,
    run_reps,
    summarize,
)


DEFAULT_DATA_PARENT = os.path.join(os.path.dirname(_HERE), "_data")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--num-files", type=int, default=2048)
    p.add_argument("--width", type=int, default=320)
    p.add_argument("--height", type=int, default=320)
    p.add_argument("--num-cpus", type=int, default=8)
    p.add_argument("--object-store-gb", type=float, default=0.6,
                   help="object store cap (drives R1)")
    p.add_argument("--reps", type=int, default=3)
    p.add_argument("--warmup", type=int, default=1)
    p.add_argument("--data-dir", default=None)
    p.add_argument("--no-spill", action="store_true",
                   help="shrink data 8× to fit in object store")
    p.add_argument("--low-cpu", action="store_true",
                   help="num_cpus=1 to confirm decode is CPU-bound")
    args = p.parse_args()

    num_files = args.num_files // 8 if args.no_spill else args.num_files
    num_cpus = 1 if args.low_cpu else args.num_cpus

    data_dir = args.data_dir or os.path.join(
        DEFAULT_DATA_PARENT, f"b2_img_n{num_files}_w{args.width}_h{args.height}"
    )
    print(f"[B2] generating data at {data_dir}")
    sys.stdout.flush()
    make_image_dataset(data_dir, num_files=num_files,
                       width=args.width, height=args.height, seed=2)

    iter_state, restore_iter = install_iter_bundles_patch()

    ray.init(num_cpus=num_cpus,
             object_store_memory=int(args.object_store_gb * 1024 ** 3),
             include_dashboard=False, log_to_driver=False,
             ignore_reinit_error=True)
    try:
        cfg = (f"N={num_files} {args.width}x{args.height} cpus={num_cpus} "
               f"store={args.object_store_gb}GiB "
               f"no_spill={args.no_spill} low_cpu={args.low_cpu}")
        print(f"[B2] config: {cfg}")
        sys.stdout.flush()

        def one_rep():
            iter_state.n_bundles = 0
            iter_state.iter_seconds = 0.0
            ds = ray.data.read_images(data_dir, mode="RGB")
            total = 0
            for bundle in ds.iter_internal_ref_bundles():
                for _, md in bundle.blocks:
                    total += md.num_rows or 0
            assert total == num_files, (total, num_files)
            stats = ds.stats()
            read_remote_s = parse_op_remote_total_seconds(stats, "ReadImage")
            bp = collect_op_backpressure_metrics(iter_state.captured_executor)
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

        reps = run_reps("B2.read_images_pil_decode", one_rep,
                        n_reps=args.reps, warmup=args.warmup,
                        num_cpus_target=num_cpus)
        print_summary("B2.read_images_pil_decode",
                      summarize(reps, skip_warmup=args.warmup))
    finally:
        ray.shutdown()
        restore_iter()


if __name__ == "__main__":
    main()
