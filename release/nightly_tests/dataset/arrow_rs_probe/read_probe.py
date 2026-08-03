#!/usr/bin/env python3
"""Single-node read probe: arrow-rs vs PyArrow, reporting BOTH time and memory.

Reproduces the *read* portion of a release test on ONE node so we can measure and
optimize the two cases where arrow-rs was worse in the release run without a
multi-node cluster:
  - mix.8ds_equal_random_mix     : time 1.67x worse (imagenet, many tiny row groups, S3)
  - wide_schema_pipeline_primitives : mem 1.50x worse (5000 columns, S3)
Both effects (read wall time and per-worker decode memory) are single-worker
properties, so one node + S3 is enough.

IMPORTANT — why this must run on Linux + real S3:
  * The crate's byte-budgeted windowed-async decode (the "working set is a page,
    not the whole row group" property) only runs on the S3 path. On local disk a
    lone big row group is read whole (K=1), so the memory win does not appear
    locally — you MUST point --path at S3 to exercise it.
  * `peak_uss_gb` (the real per-worker private cost) is populated on Linux only;
    it stays None on macOS (shared pages make RSS misleading there).

For each reader it reports:
  - wall_s          : wall time of read + consume
  - worker_cpu_s    : summed CPU seconds across Ray worker processes during the read
  - cpu_over_wall   : worker_cpu_s / wall_s. The diagnostic (use --concurrency 1):
                        ~1  => CPU-bound decode  -> optimize decode
                        <<1 => I/O-waiting on S3  -> optimize prefetch
  - peak_rss_gb     : peak summed RSS across Ray workers
  - peak_uss_gb     : peak summed USS (Linux only) -- the metric of record
  - read_wall_s / read_output_gb / read_avg_max_uss_gb : from Ray's own op stats

Run each reader in its OWN process (Ray + the crate load once per process):

  # CPU-bound-vs-IO diagnostic: force a single read task
  python read_probe.py --preset wide_schema --reader pyarrow  --concurrency 1
  python read_probe.py --preset wide_schema --reader arrow_rs --concurrency 1

  # Realistic memory: let it fan out
  python read_probe.py --preset imagenet --reader arrow_rs

Presets encode the release-test read (path + columns); override with --path/--columns
(the exact S3 prefixes drift — confirm with `aws s3 ls` on the box). Set
MALLOC_ARENA_MAX=2, or LD_PRELOAD a jemalloc .so, to A/B the allocator on the
arrow_rs run.
"""
import argparse
import os
import threading
import time
from typing import Any, Dict, List, Optional

import psutil

PRESETS = {
    # wide_schema_pipeline_primitives: ~550MB, 5000 columns. mem was 1.50x worse.
    # data_type variants exist under .../wide_schema/{primitives,tensors,objects,nested_structs}.
    "wide_schema": {
        "path": "s3://ray-benchmark-data-internal-us-west-2/wide_schema/primitives",
        "columns": None,
    },
    # mix.8ds_equal_random_mix reads imagenet per dataset; the read is the imagenet
    # decode over many tiny row groups (the K=1 case). time was 1.67x worse.
    "imagenet": {
        "path": "s3://ray-benchmark-data-internal-us-west-2/imagenet/parquet",
        "columns": ["image", "label"],
    },
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--preset", choices=list(PRESETS), default=None)
    p.add_argument("--path", type=str, default=None)
    p.add_argument("--columns", nargs="+", default=None)
    p.add_argument(
        "--reader",
        choices=["pyarrow", "arrow_rs"],
        required=True,
        help="Which V2 reader to use (sets use_arrow_rs_parquet_reader).",
    )
    p.add_argument(
        "--concurrency",
        type=int,
        default=None,
        help="Cap read tasks. Use 1 for the CPU-bound-vs-IO diagnostic.",
    )
    p.add_argument(
        "--consume", choices=["iter_bundles", "count"], default="iter_bundles"
    )
    p.add_argument("--sample-hz", type=float, default=10.0)
    args = p.parse_args()
    if args.preset:
        args.path = args.path or PRESETS[args.preset]["path"]
        if args.columns is None:
            args.columns = PRESETS[args.preset]["columns"]
    if not args.path:
        p.error("need --path or --preset")
    return args


class WorkerMemSampler:
    """Samples summed RSS/USS/CPU across Ray worker processes at a fixed rate.

    Ray runs each read task in a separate ``ray::`` worker process, so decode memory
    lives there, not in this driver. We match those processes by cmdline and track the
    peak of their *summed* RSS (and USS on Linux) plus total CPU consumed while sampling.
    """

    def __init__(self, interval_s: float):
        self._interval_s = interval_s
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.peak_rss = 0
        self.peak_uss = 0  # stays 0 if USS is unavailable (macOS)
        self._uss_ok = True
        # Track CPU as (last_seen_total) per pid, summed into cpu_seconds on exit.
        self._cpu_last: Dict[int, float] = {}
        self.cpu_seconds = 0.0

    def _ray_workers(self) -> List[psutil.Process]:
        out = []
        for proc in psutil.process_iter(["name", "cmdline"]):
            try:
                cmd = " ".join(proc.info.get("cmdline") or [])
                if "ray::" in cmd or "raylet" in (proc.info.get("name") or ""):
                    out.append(proc)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return out

    def _sample(self):
        rss = uss = 0
        for proc in self._ray_workers():
            try:
                if self._uss_ok:
                    try:
                        mi = proc.memory_full_info()
                        uss += getattr(mi, "uss", 0)
                        rss += mi.rss
                    except (psutil.AccessDenied, NotImplementedError):
                        self._uss_ok = False
                        rss += proc.memory_info().rss
                else:
                    rss += proc.memory_info().rss
                ct = proc.cpu_times()
                total = ct.user + ct.system
                pid = proc.pid
                prev = self._cpu_last.get(pid)
                # Only accumulate forward deltas (new pid or grew); ignore pid reuse noise.
                if prev is not None and total >= prev:
                    self.cpu_seconds += total - prev
                self._cpu_last[pid] = total
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        self.peak_rss = max(self.peak_rss, rss)
        if self._uss_ok:
            self.peak_uss = max(self.peak_uss, uss)

    def _run(self):
        while not self._stop.wait(self._interval_s):
            self._sample()

    def __enter__(self):
        self._sample()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        if self._thread:
            self._thread.join()
        self._sample()


def _gb(b: Optional[float]) -> Optional[float]:
    return round(b / (1024**3), 4) if b else b


def collect_read_op_metrics(ds) -> Dict[str, Any]:
    """Pull the read operator's wall time / output bytes / avg-max-USS from Ray stats."""
    from ray.data._internal.stats import DatasetStatsSummary

    out: Dict[str, Any] = {}
    try:
        summary = ds.get_stats_summary(detail=True)
        for node in DatasetStatsSummary._collect_dataset_stats_summaries(summary):
            extra = getattr(node, "extra_metrics", {}) or {}
            uss = extra.get("average_max_uss_per_task")
            for op in node.operators_stats or []:
                if "Read" in (op.operator_name or ""):
                    out["read_operator_name"] = op.operator_name
                    out["read_wall_s"] = op.wall_time.sum if op.wall_time else None
                    out["read_output_gb"] = _gb(
                        op.output_size_bytes.sum if op.output_size_bytes else None
                    )
                    out["read_avg_max_uss_gb"] = _gb(uss)
                    return out
    except Exception as e:  # best-effort
        out["read_op_metrics_error"] = repr(e)
    return out


def main():
    args = parse_args()

    import ray
    from ray.data.context import DataContext

    ctx = DataContext.get_current()
    ctx.use_datasource_v2 = True
    ctx.use_arrow_rs_parquet_reader = args.reader == "arrow_rs"

    ray.init(ignore_reinit_error=True)

    read_kwargs: Dict[str, Any] = {}
    if args.columns:
        read_kwargs["columns"] = args.columns
    if args.concurrency is not None:
        read_kwargs["concurrency"] = args.concurrency
        read_kwargs["override_num_blocks"] = args.concurrency

    print(
        f"reader={args.reader} path={args.path} columns={args.columns} "
        f"concurrency={args.concurrency} arena_max={os.environ.get('MALLOC_ARENA_MAX')} "
        f"ld_preload={os.environ.get('LD_PRELOAD')}"
    )

    try:
        with WorkerMemSampler(1.0 / args.sample_hz) as sampler:
            t0 = time.perf_counter()
            ds = ray.data.read_parquet(args.path, **read_kwargs)
            if args.consume == "count":
                ds.count()
            else:
                for _ in ds.iter_internal_ref_bundles():
                    pass
            wall = time.perf_counter() - t0

        result = {
            "reader": args.reader,
            "wall_s": round(wall, 3),
            "worker_cpu_s": round(sampler.cpu_seconds, 3),
            "cpu_over_wall": round(sampler.cpu_seconds / wall, 3) if wall else None,
            "peak_rss_gb": _gb(sampler.peak_rss),
            "peak_uss_gb": _gb(sampler.peak_uss) if sampler.peak_uss else None,
            **collect_read_op_metrics(ds),
        }
        print("\n=== RESULT ===")
        for k, v in result.items():
            print(f"  {k}: {v}")
    finally:
        # Shut Ray down cleanly so the next reader's process does not find (and the
        # sampler does not sum) a stale second Ray instance's workers.
        ray.shutdown()


if __name__ == "__main__":
    main()
