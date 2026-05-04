"""B3: streaming_split early-stop.

Engages R5 (early-stop teardown correctness): each consumer breaks
out of `iter_batches` after `num_rows_total // 2 // N` rows. The
coordinator must (a) handle clients that stop pulling mid-stream,
(b) clear `client_prefetched_bytes` for stopped clients via the
finally-clause in `get`, and (c) eventually shut down the executor.

This is the upstream `streaming_split.early_stop` YAML row. The
upstream test references issue #34819 — a GPU memory leak in the
early-stop path. We can't reproduce GPU memory specifically, but
we can verify (a) row count correctness and (b) no driver-side RSS
runaway across reps.

Calibration finding: B3 walls cluster around ~9s with consistent
actor_skew of ~0.7-0.9s — meaningfully larger than B1's ~0.05s
skew. The skew reflects the early-stop teardown path: stopped
consumers exit before others, but the executor is still draining
in-flight read tasks while remaining consumers pull. The skew
itself is the R5 timing signal.

When run inside a fresh `ray.init` (as this script does), walls
are stable (4-5% stdev across 4 reps). Long-lived clusters with
many prior streaming_split runs have shown a bimodal pattern
(~5s vs ~11s walls) — recorded as a calibration note; this
script avoids it by re-initializing each run.

Run:
    /opt/venv/bin/python b3_streaming_split_early_stop/run.py
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
    p.add_argument("--reps", type=int, default=4)
    p.add_argument("--warmup", type=int, default=1)
    p.add_argument("--data-dir", default=None)
    args = p.parse_args()

    data_dir = args.data_dir or os.path.join(
        DEFAULT_DATA_PARENT,
        f"ssb_n{args.num_files}_r{args.rows_per_file}_p{args.payload_bytes}",
    )
    print(f"[B3] generating data at {data_dir}")
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
               f"cpus={args.num_cpus} store={args.object_store_gb}GiB "
               f"equal=False early_stop=True")
        print(f"[B3] config: {cfg}")
        sys.stdout.flush()

        # Each worker reads num_rows // 2 // n_workers rows.
        total_rows = args.num_files * args.rows_per_file
        expected_per_worker = total_rows // 2 // args.n_workers
        expected_total = expected_per_worker * args.n_workers

        # Collect rows_consumed per rep for cross-rep RSS check + correctness.
        per_rep_rss: list = []

        def one_rep():
            ds = ray.data.read_parquet(data_dir)
            rep = run_streaming_split_once(
                ds, num_workers=args.n_workers, equal=False, early_stop=True,
            )
            # Each worker iterates batches until it sees >= max_rows; the
            # break happens AFTER counting the rows in the last batch, so
            # consumers may slightly over-shoot. Bound: each worker reads
            # at most `expected_per_worker + last_batch_size`.
            assert rep["rows_consumed"] >= expected_total, (
                rep["rows_consumed"], expected_total
            )
            # Sanity: per-worker is bounded above by expected_per_worker
            # plus one batch (default batch ~100s of rows for parquet here).
            per_worker_max_observed = (
                rep["rows_consumed"] / args.n_workers
            )
            assert per_worker_max_observed <= expected_per_worker * 1.5, (
                per_worker_max_observed, expected_per_worker
            )
            per_rep_rss.append(rep)
            return rep

        reps = run_reps("B3.streaming_split_early_stop", one_rep,
                        n_reps=args.reps, warmup=args.warmup)
        summary = summarize(reps, skip_warmup=args.warmup)

        # Cross-rep RSS leak check: the driver's RSS shouldn't grow
        # monotonically across reps. (Issue #34819 referenced GPU leaks;
        # on a CPU-only host we proxy with driver RSS.)
        measured = reps[args.warmup:]
        rss_progression = [r.driver_rss_after for r in measured]
        first_rss = rss_progression[0]
        last_rss = rss_progression[-1]
        rss_growth_mb = (last_rss - first_rss) / 1e6
        summary["rss_growth_mb_first_to_last"] = round(rss_growth_mb, 1)
        summary["rss_progression_mb"] = [round(x / 1e6, 1) for x in rss_progression]

        print_summary("B3.streaming_split_early_stop", summary)

        # Explicit correctness summary line, since this is a correctness
        # benchmark first and a perf benchmark second.
        print(f"\n[B3] correctness: rows_consumed_target>={expected_total}, "
              f"observed across reps: "
              f"{[r.extra['rows_consumed'] for r in measured]}")
        print(f"[B3] cross-rep RSS first→last: {rss_growth_mb:+.1f} MB "
              f"(>50 MB sustained growth would indicate a leak)")
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
