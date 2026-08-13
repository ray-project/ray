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
        help=(
            "Cap read tasks AND pin output blocks (sets override_num_blocks too). "
            "Use 1 for the CPU-bound-vs-IO diagnostic."
        ),
    )
    p.add_argument(
        "--task-concurrency",
        type=int,
        default=None,
        help=(
            "Cap concurrent read tasks WITHOUT touching override_num_blocks — use "
            "this (not --concurrency) whenever the point of the run is the bin "
            "geometry, so a process holds exactly one bin at a time and per-task "
            "USS is attributable to one bin. (On the Parquet V2 path "
            "override_num_blocks is inert anyway — the footer indexer sets "
            "yields_read_units=True, so read_api.py:546-556 skips the partitioner "
            "— but keeping it out of the command line keeps that an observation "
            "rather than a dependency.)"
        ),
    )
    p.add_argument(
        "--mem-poll-s",
        type=float,
        default=None,
        help=(
            "DataContext.memory_usage_poll_interval_s for the in-task MemoryProfiler "
            "(default 1.0s, context.py:984). Short read tasks get one sample or none "
            "at 1 Hz, which silently flattens per-task USS — set ~0.05 for any run "
            "whose verdict is a USS number."
        ),
    )
    p.add_argument(
        "--consume",
        choices=["iter_bundles", "count", "write_parquet"],
        default="iter_bundles",
        help=(
            "write_parquet replicates the release write_parquet test (1aa): the "
            "read fuses into the write task, stats come from ds._write_ds (which "
            "materializes, so no capture_executor race). Output is deleted after "
            "the run."
        ),
    )
    p.add_argument(
        "--write-path",
        default=None,
        help="Output dir for --consume write_parquet (default: <tmp>/probe_write_out).",
    )
    # 50 Hz (20 ms): a 5000-column decode's builder-flush spike is short; at the
    # old 10 Hz it was caught on some runs and missed on others, giving ~1.5 GB of
    # run-to-run variance in peak_uss. The deterministic metric of record is Ray's
    # own per-task max USS (``read_avg_max_uss_gb``); this sampler is the backup.
    p.add_argument("--sample-hz", type=float, default=50.0)
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

    def __init__(self, interval_s: float, root_pid: Optional[int] = None):
        self._interval_s = interval_s
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.peak_rss = 0
        self.peak_uss = 0  # stays 0 if USS is unavailable (macOS)
        self._uss_ok = True
        # PID of OUR node's raylet. When set, we sum only its descendant workers,
        # not every ``ray::`` process on the box — otherwise the workspace's managed
        # Ray (:6379) workers get summed in, polluting the peak. None => fall back to
        # the cmdline heuristic (macOS / when the raylet pid can't be found).
        self._root_pid = root_pid
        self.matched_workers = 0
        # Track CPU as (last_seen_total) per pid, summed into cpu_seconds on exit.
        self._cpu_last: Dict[int, float] = {}
        self.cpu_seconds = 0.0

    def _ray_workers(self) -> List[psutil.Process]:
        # Preferred: OUR raylet + its descendant workers only. A private local Ray
        # instance owns exactly its read workers here, so this isolates us from the
        # workspace's :6379 node cleanly (ray:: proctitles can't be matched by
        # session dir — setproctitle overwrites the cmdline).
        if self._root_pid is not None:
            try:
                root = psutil.Process(self._root_pid)
                procs = [root] + root.children(recursive=True)
                self.matched_workers = len(procs)
                return procs
            except psutil.NoSuchProcess:
                return []
        out = []
        for proc in psutil.process_iter(["name", "cmdline"]):
            try:
                cmd = " ".join(proc.info.get("cmdline") or [])
                if "ray::" in cmd or "raylet" in (proc.info.get("name") or ""):
                    out.append(proc)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        self.matched_workers = len(out)
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
    """Pull the read operator's wall time / output bytes / per-task USS from
    Ray stats.

    GE1 (2026-08-11) returned ``read_avg_max_uss_gb=None`` in most cells.
    Root-caused 2026-08-11 (reproduced locally with a faked-USS
    MemoryProfiler): the workers always reported USS; the loss was a
    driver-side snapshot race. ``Dataset._execute_to_iterator`` caches
    ``executor.get_stats()`` right after the FIRST bundle, and
    ``iter_internal_ref_bundles()`` passes ``capture_executor=False`` — so
    ``get_stats_summary()`` fell back to that mid-execution snapshot, taken
    before the last read task's ``on_task_finished`` populated
    ``average_max_uss_per_task``. ListFiles always had values because it
    finishes long before consumption ends. Fixed in ``main()`` by consuming
    via ``_execute_to_iterator(capture_executor=True)`` so this reads the
    post-shutdown final stats. The instrumentation stays: (a) per-node
    ``uss_debug`` dump, (b) ``max_uss_per_task`` (the worst task, often the
    OOM-relevant number), (c) labeled ``uss_fallback_*`` when the read node
    has no USS — never silently substituted into ``read_avg_max_uss_gb``,
    which stays the metric of record."""
    from ray.data._internal.stats import DatasetStatsSummary

    out: Dict[str, Any] = {}
    debug = []
    try:
        summary = ds.get_stats_summary(detail=True)
        nodes = DatasetStatsSummary._collect_dataset_stats_summaries(summary)
        read_hit = None  # (op_summary, extra_metrics) of the first Read node
        for node in nodes:
            extra = getattr(node, "extra_metrics", {}) or {}
            op_names = [op.operator_name or "" for op in (node.operators_stats or [])]
            debug.append(
                {
                    "operators": op_names,
                    "average_max_uss_bytes": extra.get("average_max_uss_per_task"),
                    "max_uss_bytes": extra.get("max_uss_per_task"),
                    "num_extra_metrics": len(extra),
                }
            )
            if read_hit is None:
                for op in node.operators_stats or []:
                    if "Read" in (op.operator_name or ""):
                        read_hit = (op, extra)
                        break
        if read_hit is not None:
            op, extra = read_hit
            out["read_operator_name"] = op.operator_name
            out["read_wall_s"] = op.wall_time.sum if op.wall_time else None
            out["read_output_gb"] = _gb(
                op.output_size_bytes.sum if op.output_size_bytes else None
            )
            out["read_avg_max_uss_gb"] = _gb(extra.get("average_max_uss_per_task"))
            out["read_max_uss_gb"] = _gb(extra.get("max_uss_per_task"))
            # --- the bin-bound denominators -------------------------------------
            # A read task == one bin, so decoded bytes/task is the *decoded* size of
            # a bin (RAY_DATA_PARQUET_BIN_PACKING_BYTES budgets Parquet
            # total_uncompressed_size, i.e. pages after decompression but still
            # ENCODED — dictionary/RLE columns decode larger, so the knob is a proxy,
            # not an identity). Reporting both lets the bound be stated against the
            # decoded number and the expansion factor be seen rather than assumed.
            ntasks = extra.get("num_tasks_finished")
            out["read_num_tasks"] = ntasks
            if ntasks:
                out["read_bytes_per_task_gb"] = (
                    round(out["read_output_gb"] / ntasks, 4)
                    if out.get("read_output_gb")
                    else None
                )
                # max/avg over tasks: ~1.0 => every task costs the same (bounded);
                # rising with task count => the worker is retaining across tasks
                # (allocator retention or a real leak), which no bin cap can bound.
                a, m = (
                    extra.get("average_max_uss_per_task"),
                    extra.get("max_uss_per_task"),
                )
                if a and m:
                    out["uss_max_over_avg"] = round(m / a, 3)
        if out.get("read_avg_max_uss_gb") is None:
            for i, row in enumerate(debug):
                if row["average_max_uss_bytes"]:
                    out["uss_fallback_avg_gb"] = _gb(row["average_max_uss_bytes"])
                    out["uss_fallback_max_gb"] = _gb(row["max_uss_bytes"])
                    out["uss_fallback_source"] = (
                        ",".join(row["operators"]) or f"node_{i}"
                    )
                    break
        out["uss_debug"] = debug
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
    if args.mem_poll_s is not None:
        ctx.memory_usage_poll_interval_s = args.mem_poll_s

    ray.init(ignore_reinit_error=True)

    # Pin subsequent Ray/GCS resolution to THIS instance's address. With
    # RAY_ADDRESS=local next to the workspace's managed Ray (:6379), the stats
    # collection re-resolves "local", finds two running instances, and errors —
    # which is why read_avg_max_uss_gb (the deterministic per-task USS) has been
    # empty every run. Pinning the explicit gcs address disambiguates it.
    try:
        gcs = ray.get_runtime_context().gcs_address
        if gcs:
            os.environ["RAY_ADDRESS"] = gcs
    except Exception:
        pass

    # Find OUR raylet's pid so the sampler sums only our node's workers.
    root_pid: Optional[int] = None
    try:
        node = ray._private.worker._global_node  # noqa: SLF001
        for name, procs in (node.all_processes or {}).items():
            if "raylet" in name.lower() and procs:
                root_pid = procs[0].process.pid
                break
    except Exception:
        pass

    read_kwargs: Dict[str, Any] = {}
    if args.columns:
        read_kwargs["columns"] = args.columns
    if args.concurrency is not None:
        read_kwargs["concurrency"] = args.concurrency
        read_kwargs["override_num_blocks"] = args.concurrency
    elif args.task_concurrency is not None:
        read_kwargs["concurrency"] = args.task_concurrency

    print(
        f"reader={args.reader} path={args.path} columns={args.columns} "
        f"concurrency={args.concurrency} arena_max={os.environ.get('MALLOC_ARENA_MAX')} "
        f"ld_preload={os.environ.get('LD_PRELOAD')} "
        f"raylet_pid={root_pid} sample_hz={args.sample_hz}"
    )

    try:
        with WorkerMemSampler(1.0 / args.sample_hz, root_pid=root_pid) as sampler:
            t0 = time.perf_counter()
            ds = ray.data.read_parquet(args.path, **read_kwargs)
            if args.consume == "count":
                ds.count()
            elif args.consume == "write_parquet":
                import shutil
                import tempfile

                write_out = args.write_path or os.path.join(
                    tempfile.gettempdir(), "probe_write_out"
                )
                shutil.rmtree(write_out, ignore_errors=True)
                try:
                    ds.write_parquet(write_out)
                finally:
                    # The fused read->write op's stats live on ds._write_ds
                    # (dataset.py: get_stats_summary falls through to it), which
                    # is materialized — read them via collect_read_op_metrics
                    # below as usual; only the bytes on disk need cleanup.
                    shutil.rmtree(write_out, ignore_errors=True)
            else:
                # Same zero-copy consumption as ds.iter_internal_ref_bundles(),
                # but with capture_executor=True so ds.get_stats_summary() can
                # read the executor's post-shutdown final stats.
                # iter_internal_ref_bundles() drops the executor
                # (capture_executor=False), which pins get_stats_summary() to a
                # stats snapshot cached after the FIRST bundle — taken before
                # the last read task finishes, so average_max_uss_per_task is
                # still empty in it (the GE1 "read_avg_max_uss_gb=None" bug).
                bundle_iter, _, _ = ds._execute_to_iterator(capture_executor=True)
                for _ in bundle_iter:
                    pass
            wall = time.perf_counter() - t0

        result = {
            "reader": args.reader,
            "wall_s": round(wall, 3),
            "worker_cpu_s": round(sampler.cpu_seconds, 3),
            "cpu_over_wall": round(sampler.cpu_seconds / wall, 3) if wall else None,
            "peak_rss_gb": _gb(sampler.peak_rss),
            "peak_uss_gb": _gb(sampler.peak_uss) if sampler.peak_uss else None,
            "sampled_workers": sampler.matched_workers,
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
