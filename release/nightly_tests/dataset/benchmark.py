import gc
import json
import logging
import math
import os
import threading
import time
from enum import Enum
from typing import Any, Callable, Dict, List, Tuple, Union
import dataclasses
import ray
from ray._private.internal_api import get_memory_info_reply, get_state_from_address
from ray.util.state import list_runtime_envs

logger = logging.getLogger(__name__)

# Poll per-task memory (MemoryProfiler in each map worker) at 20 Hz instead of the
# 1 Hz production default. At 1 Hz, a task shorter than ~3 s gets its "max USS"
# from the single synchronous end-of-task sample — end-of-task RESIDENT memory,
# not peak working set — which made 16 of 37 instrumented tests unmeasurable in
# the 2026-08-14 A/B. Each sample is one /proc/self/statm read (microseconds), so
# 20 Hz costs ~0.01% CPU per worker. Set here (release harness only, both arms of
# an A/B identically) rather than upstream, where the conservative default is
# deliberate. The DataContext is captured at dataset creation, so importing
# benchmark.py before building datasets — which every release script does — is
# sufficient for this to reach the workers.
ray.data.DataContext.get_current().memory_usage_poll_interval_s = float(
    os.environ.get("RAY_DATA_BENCH_MEMORY_POLL_S", "0.05")
)


def _get_spilled_bytes_total(state) -> float:
    """Get the total number of spilled bytes across the cluster."""
    return get_memory_info_reply(state).store_stats.spilled_bytes_total


def _bytes_to_gb(b: float) -> float:
    return round(b / (1024**3), 4)


class ObjectStoreMemorySampler:
    """Samples aggregate object store usage and tracks the peak value.

    Object store usage is an instantaneous gauge, so checking only at the
    beginning and end of a benchmark can miss short-lived memory spikes.
    """

    def __init__(self, state, interval_s: float = 1.0):
        self._state = state
        self._interval_s = interval_s
        self._stop_event = threading.Event()
        self._thread = None

        self._peak_used_bytes = 0
        self._peak_utilization = 0.0

    @property
    def peak_used_bytes(self) -> int:
        return self._peak_used_bytes

    @property
    def peak_utilization(self) -> float:
        return self._peak_utilization

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def start(self):
        self._sample_once()
        self._thread = threading.Thread(
            target=self._run,
            name="object-store-memory-sampler",
            daemon=True,
        )
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join()
        self._sample_once()

    def _run(self):
        while not self._stop_event.wait(self._interval_s):
            self._sample_once()

    def _sample_once(self):
        try:
            store_stats = get_memory_info_reply(self._state).store_stats
        except Exception:
            logger.warning("Failed to sample object store memory.", exc_info=True)
            return

        used_bytes = store_stats.object_store_bytes_used
        capacity_bytes = store_stats.object_store_bytes_avail

        self._peak_used_bytes = max(self._peak_used_bytes, used_bytes)

        if capacity_bytes > 0:
            self._peak_utilization = max(
                self._peak_utilization,
                used_bytes / capacity_bytes,
            )


def collect_dataset_stats(ds: "ray.data.Dataset") -> Dict[str, Any]:
    """Collect execution stats from a Dataset as a JSON-serializable dict.
    This is a subset from `get_stats_summary`, because we are only adding the ones
    we care about for the release tests."""
    summary = ds.get_stats_summary(detail=True)
    return {
        "total_scheduling_runtime": summary.streaming_exec_schedule_s,
        "avg_scheduling_loop_duration_s": summary.streaming_exec_schedule_avg_s,
        "max_scheduling_loop_duration_s": summary.streaming_exec_schedule_max_s,
        "p50_scheduling_loop_duration_s": summary.streaming_exec_schedule_p50_s,
        "p90_scheduling_loop_duration_s": summary.streaming_exec_schedule_p90_s,
        "operators": [
            {
                "operator_name": op.operator_name,
                "earliest_start_time": op.earliest_start_time,
                "latest_end_time": op.latest_end_time,
                "scheduling_overhead": (
                    [dataclasses.asdict(bucket) for bucket in op.scheduling_overhead]
                    if op.scheduling_overhead
                    else []
                ),
            }
            for op in summary.operators_stats
        ],
    }


class _NullMonitor:
    """Stand-in used when the node memory monitor is disabled or unimportable."""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def summary(self) -> Dict[str, Any]:
        return {}


def _node_memory_monitor(case_name: str):
    """Return a started-on-enter node memory monitor, or a no-op stand-in.

    Import is deferred and failure is swallowed: the monitor is diagnostics, and a
    benchmark must not fail because a diagnostic could not be imported (release images
    and local runs do not always carry the same files).
    """
    try:
        import node_memory_monitor

        if node_memory_monitor.enabled():
            return node_memory_monitor.NodeMemoryMonitor(case_name)
    except Exception:  # noqa: BLE001
        logger.warning("node memory monitor unavailable", exc_info=True)
    return _NullMonitor()


def consume_ref_bundles(ds: "ray.data.Dataset", per_bundle: Callable = None) -> int:
    """Consume ``ds`` as RefBundles *without* losing the executor's final stats.

    Use this instead of ``ds.iter_internal_ref_bundles()`` in any benchmark that then
    reads stats off ``ds`` (``collect_operator_metrics`` / ``collect_dataset_stats``).
    Consumption is identical — the same zero-copy bundle iterator, no blocks fetched —
    the only difference is ``capture_executor=True``.

    Why it matters: ``Dataset._execute_to_iterator`` caches ``executor.get_stats()``
    right after the FIRST bundle, and ``iter_internal_ref_bundles`` drops the executor
    (``capture_executor=False``, ``dataset.py:7482-7486``), so a later
    ``get_stats_summary()`` is pinned to that mid-execution snapshot — taken before the
    last read task's ``on_task_finished`` populated ``average_max_uss_per_task``. The
    per-task memory metrics then come back ``None`` non-deterministically, depending on
    whether the operator happened to finish early: ``ListFiles`` always had values,
    ``ReadParquet`` often did not. It cost both arms of two multi-node release A/Bs
    their ``read_large_parquet`` memory numbers.

    Args:
        ds: the dataset to consume.
        per_bundle: optional callback invoked with each ``RefBundle``. Leave unset to
            drop each bundle as it arrives (the usual "read it all and throw it away").

    Returns:
        The number of bundles consumed.
    """
    bundle_iter, _, _ = ds._execute_to_iterator(capture_executor=True)
    # Deliberately NOT calling ds._synchronize_progress_bar() here, though
    # iter_internal_ref_bundles does: it is a no-op there (nothing was captured) but
    # here it would `shutdown(force=True)` the executor we just captured, truncating
    # consumption to the one bundle _execute_to_iterator already forced. Verified
    # locally: with the call, an 8-block dataset yields 1 bundle and 1/8 of the rows.
    num_bundles = 0
    for bundle in bundle_iter:
        num_bundles += 1
        if per_bundle is not None:
            per_bundle(bundle)
    return num_bundles


def collect_operator_metrics(ds: "ray.data.Dataset") -> Dict[str, Any]:
    """Per-operator time / output-bytes / worker-memory, for merging into a result dict.

    Surfaces numbers that otherwise live only on the Prometheus dashboard, not in the
    release log or databricks: each operator's wall time, output size/rows, and its
    per-task peak worker memory — USS (private working set) and RSS (OS-visible
    footprint, includes mapped object-store pages), both as the average across tasks
    and the single worst task. All four come from ``MemoryProfiler`` sampling inside
    the task (Linux-only; ``None`` on macOS). This isolates the read operator's cost
    from downstream compute and exposes the decode-memory metrics that the aggregate
    object-store peak cannot see. Best-effort: returns a partial/empty dict rather
    than failing the benchmark.

    A ``read_*`` top-level convenience is filled from the ``Read*`` operator with
    the most tasks (ties: plan order) so the "parquet part" (read wall time +
    output bytes + decode USS/RSS) is a first-class field; ``read_operators``
    lists every ``Read*`` operator compactly, since a multi-table plan (TPC-H
    q17: ``part`` AND ``lineitem``) has several and the headline used to be
    whichever came first.

    NOTE: stats attach to the consumed dataset handle. Consume ``ds`` itself
    (``iter_*``/``write_*``/``materialize``) before calling this; ``ds.count()``
    executes a *copy* of the plan and leaves ``ds`` without stats. And consume via
    ``consume_ref_bundles(ds)``, not ``ds.iter_internal_ref_bundles()`` — the latter
    drops the executor, which pins the stats to a snapshot taken after the first bundle
    and silently nulls every per-task memory field below.
    """
    from ray.data._internal.stats import DatasetStatsSummary

    def _sum(stat) -> Any:
        return stat.sum if stat is not None else None

    # (result-dict key, extra_metrics key) for the per-task memory metrics.
    mem_keys = [
        ("avg_max_uss_per_task_bytes", "average_max_uss_per_task"),
        ("max_uss_per_task_bytes", "max_uss_per_task"),
        ("avg_max_rss_per_task_bytes", "average_max_rss_per_task"),
        ("max_rss_per_task_bytes", "max_rss_per_task"),
    ]
    # (result-dict key, extra_metrics key) for the full per-task distributions.
    # ``max_uss_bytes``/``max_rss_bytes`` are DistributionTracker.as_dict() outputs:
    # num_samples/mean/variance/min/max plus p25..p99 (quantiles come from a KLL
    # sketch and are None unless ``datasketches`` is importable in the worker —
    # it is pinned in requirements_compiled.txt, so release images have it).
    dist_keys = [
        ("max_uss_per_task_dist", "max_uss_bytes"),
        ("max_rss_per_task_dist", "max_rss_bytes"),
        ("task_duration_dist", "op_task_duration_stats"),
        # Reader-level per-task aggregates (ReadFilesTaskStats, reported by the
        # ReadFiles transform on every V2 file read): what the DECODER did,
        # independent of the memory profiler — bytes/decode-seconds per task
        # and the largest single table the reader yielded (working-set proxy).
        ("decoded_bytes_per_task_dist", "read_task_decoded_bytes"),
        ("decode_wall_s_per_task_dist", "read_task_decode_wall_s"),
        ("peak_batch_bytes_per_task_dist", "read_task_peak_batch_bytes"),
        # Wall seconds of the reader's end-of-stream finalizer (the arrow-rs
        # malloc_trim under RAY_DATA_ARROW_RS_MALLOC_TRIM_EOS); inside
        # decode_wall_s. 0 for pyarrow and for arrow-rs with the knob off.
        ("trim_wall_s_per_task_dist", "read_task_trim_wall_s"),
        ("yield_wall_s_per_task_dist", "read_task_yield_wall_s"),
        ("first_table_wall_s_per_task_dist", "read_task_first_table_wall_s"),
    ]

    out: Dict[str, Any] = {"operators_detail": []}
    try:
        summary = ds.get_stats_summary(detail=True)
        for node in DatasetStatsSummary._collect_dataset_stats_summaries(summary):
            extra = getattr(node, "extra_metrics", {}) or {}
            mem = {out_key: extra.get(in_key) for out_key, in_key in mem_keys}
            dists = {out_key: extra.get(in_key) for out_key, in_key in dist_keys}
            for op in node.operators_stats or []:
                # Output-block granularity: StatsSummary is per-block, so its
                # count/min/mean/max ARE the block-size distribution — the
                # signal for shuffle-feeding-granularity questions.
                osb = op.output_size_bytes
                out["operators_detail"].append(
                    {
                        "operator_name": op.operator_name,
                        "wall_time_s": _sum(op.wall_time),
                        "cpu_time_s": _sum(op.cpu_time),
                        "udf_time_s": _sum(op.udf_time),
                        "output_num_rows": _sum(op.output_num_rows),
                        "output_size_bytes": _sum(op.output_size_bytes),
                        "output_num_blocks": osb.count if osb else None,
                        "block_size_bytes_min": osb.min if osb else None,
                        "block_size_bytes_mean": osb.mean if osb else None,
                        "block_size_bytes_max": osb.max if osb else None,
                        **mem,
                        **dists,
                    }
                )

        def _n_tasks(entry) -> int:
            # One sample per finished task that emitted a block. Task duration
            # is recorded on every platform; max_uss only where USS is readable
            # (Linux), so it is the fallback.
            for key in ("task_duration_dist", "max_uss_per_task_dist"):
                n = (entry.get(key) or {}).get("num_samples")
                if n:
                    return n
            return 0

        def _q(entry, dist_key, stat):
            return (entry.get(dist_key) or {}).get(stat)

        read_entries = [
            e for e in out["operators_detail"] if "Read" in (e["operator_name"] or "")
        ]
        # One compact row per Read operator, in plan order. A task that emits
        # no block carries no worker stats at all (TaskExecWorkerStats rides
        # on block metadata), so ``tasks`` can be below the operator's task
        # count and ``decoded_*_n`` == ``tasks`` when the reader stats fired.
        out["read_operators"] = [
            {
                "operator_name": e["operator_name"],
                "tasks": _n_tasks(e),
                "wall_time_s": e["wall_time_s"],
                "output_num_rows": e["output_num_rows"],
                "output_size_bytes": e["output_size_bytes"],
                "output_num_blocks": e["output_num_blocks"],
                "max_uss_per_task_p50": _q(e, "max_uss_per_task_dist", "p50"),
                "max_uss_per_task_max": _q(e, "max_uss_per_task_dist", "max"),
                "task_duration_p50": _q(e, "task_duration_dist", "p50"),
                "decoded_bytes_n": _q(e, "decoded_bytes_per_task_dist", "num_samples"),
                "decoded_bytes_p50": _q(e, "decoded_bytes_per_task_dist", "p50"),
                "decoded_bytes_max": _q(e, "decoded_bytes_per_task_dist", "max"),
                "peak_batch_bytes_p50": _q(e, "peak_batch_bytes_per_task_dist", "p50"),
                "peak_batch_bytes_max": _q(e, "peak_batch_bytes_per_task_dist", "max"),
                "trim_wall_s_p50": _q(e, "trim_wall_s_per_task_dist", "p50"),
                "trim_wall_s_max": _q(e, "trim_wall_s_per_task_dist", "max"),
                "yield_wall_s_p50": _q(e, "yield_wall_s_per_task_dist", "p50"),
                "yield_wall_s_max": _q(e, "yield_wall_s_per_task_dist", "max"),
                "first_table_wall_s_p50": _q(
                    e, "first_table_wall_s_per_task_dist", "p50"
                ),
                "first_table_wall_s_max": _q(
                    e, "first_table_wall_s_per_task_dist", "max"
                ),
            }
            for e in read_entries
        ]
        if read_entries:
            # Headline = the Read with the most tasks; ``max`` keeps the first
            # maximal entry, i.e. plan order breaks ties.
            entry = max(read_entries, key=_n_tasks)
            out["read_operator_name"] = entry["operator_name"]
            out["read_wall_time_s"] = entry["wall_time_s"]
            out["read_output_size_bytes"] = entry["output_size_bytes"]
            out["read_output_num_blocks"] = entry["output_num_blocks"]
            out["read_block_size_bytes_mean"] = entry["block_size_bytes_mean"]
            for out_key, _ in mem_keys:
                out[f"read_{out_key}"] = entry[out_key]
            for out_key, _ in dist_keys:
                out[f"read_{out_key}"] = entry[out_key]
    except Exception:
        logger.warning("collect_operator_metrics failed", exc_info=True)
    return out


def collect_task_distribution(case_start_unix_ms: float) -> Dict[str, Any]:
    """How this case's tasks were spread over workers and nodes.

    Answers the placement question the per-task memory distributions cannot:
    two arms can run identical tasks yet land them on different worker pools —
    a faster arm satisfies autoscaling demand with fewer workers, so each
    worker runs MORE tasks and accumulates a higher between-tasks memory
    floor. These aggregates make that visible per arm: tasks-per-worker /
    per-node distributions, worker lifespan (first task start to last task
    end), and worker busy fraction (summed task seconds over lifespan).

    Uses the state API's task list, filtered to tasks that started after this
    case began (cases run sequentially in one job). The API caps at 10k tasks;
    ``task_dist_truncated`` flags when the cap was hit, in which case the
    per-worker numbers undercount instead of erroring.
    """
    from ray.util.state.api import list_tasks

    def _quantile(sorted_vals, q):
        if not sorted_vals:
            return None
        i = max(0, min(len(sorted_vals) - 1, round(q * (len(sorted_vals) - 1))))
        return sorted_vals[i]

    out: Dict[str, Any] = {}
    try:
        tasks = list_tasks(detail=True, limit=10_000, raise_on_missing_output=False)
        per_worker: Dict[str, List[Tuple[float, float]]] = {}
        nodes = set()
        durations = []
        recs = []
        n = 0
        for t in tasks:
            if not t.start_time_ms or t.start_time_ms < case_start_unix_ms:
                continue
            if not t.worker_id or not t.end_time_ms:
                continue
            n += 1
            per_worker.setdefault(t.worker_id, []).append(
                (t.start_time_ms, t.end_time_ms)
            )
            if t.node_id:
                nodes.add((t.node_id, t.worker_id))
            durations.append((t.end_time_ms - t.start_time_ms) / 1000.0)
            recs.append(
                (
                    t.name,
                    t.start_time_ms,
                    t.end_time_ms,
                    t.node_id,
                    t.worker_id,
                    t.worker_pid,
                )
            )

        out["task_dist_num_tasks"] = n
        out["task_dist_truncated"] = len(tasks) >= 10_000
        out["task_dist_num_workers"] = len(per_worker)
        node_ids = {nid for nid, _ in nodes}
        out["task_dist_num_nodes"] = len(node_ids)

        counts = sorted(len(v) for v in per_worker.values())
        out["task_dist_tasks_per_worker_mean"] = (
            round(n / len(per_worker), 2) if per_worker else None
        )
        out["task_dist_tasks_per_worker_p50"] = _quantile(counts, 0.5)
        out["task_dist_tasks_per_worker_max"] = counts[-1] if counts else None

        per_node: Dict[str, int] = {}
        for nid, wid in nodes:
            per_node[nid] = per_node.get(nid, 0) + 1
        workers_per_node = sorted(per_node.values())
        out["task_dist_workers_per_node_max"] = (
            workers_per_node[-1] if workers_per_node else None
        )

        durations.sort()
        out["task_dist_task_duration_s_p50"] = _quantile(durations, 0.5)
        out["task_dist_task_duration_s_p90"] = _quantile(durations, 0.9)
        out["task_dist_task_duration_s_max"] = durations[-1] if durations else None

        lifespans = []
        busy_fracs = []
        for spans in per_worker.values():
            start = min(s for s, _ in spans)
            end = max(e for _, e in spans)
            lifespan_s = (end - start) / 1000.0
            lifespans.append(lifespan_s)
            busy_s = sum(e - s for s, e in spans) / 1000.0
            if lifespan_s > 0:
                busy_fracs.append(busy_s / lifespan_s)
        lifespans.sort()
        busy_fracs.sort()
        out["task_dist_worker_lifespan_s_p50"] = _quantile(lifespans, 0.5)
        out["task_dist_worker_lifespan_s_max"] = lifespans[-1] if lifespans else None
        out["task_dist_worker_busy_frac_p50"] = (
            round(_quantile(busy_fracs, 0.5), 4) if busy_fracs else None
        )
        out["task_dist_worker_busy_frac_min"] = (
            round(busy_fracs[0], 4) if busy_fracs else None
        )

        # Per-task spawn timeline (the 2x2 topology probe wants WHEN and WHERE
        # tasks ran, not just the aggregates above). Compact legend-indexed
        # rows keep this a few hundred KB inside result.json -- loose files do
        # not survive a release run. RAY_DATA_BENCH_TASK_TIMELINE=0 disables.
        if os.environ.get("RAY_DATA_BENCH_TASK_TIMELINE", "1") != "0":
            out["task_timeline"] = _build_task_timeline(recs, case_start_unix_ms)
    except Exception:
        logger.warning("collect_task_distribution failed", exc_info=True)
    return out


_TASK_TIMELINE_CAP = 6000


def _build_task_timeline(recs, t0_ms):
    """Legend-indexed task timeline: each row is
    [name_idx, start_ms_rel, dur_ms, node_idx, worker_idx].

    When the cap bites, read tasks are kept preferentially (they are what the
    2x2 experiment is about), earliest-first within each class, and
    ``truncated`` is set. Node/worker ids are truncated to 16 hex chars in the
    legends; ``worker_pids`` aligns with ``workers`` so rows can be joined
    against node_memory_monitor's per-pid USS samples.
    """
    recs.sort(key=lambda r: ("Read" not in (r[0] or ""), r[1]))
    truncated = len(recs) > _TASK_TIMELINE_CAP
    recs = recs[:_TASK_TIMELINE_CAP]
    recs.sort(key=lambda r: r[1])
    names, nodes, workers, worker_pids, rows = [], [], [], [], []
    name_idx, node_idx, worker_idx = {}, {}, {}
    for name, start, end, node, worker, pid in recs:
        name, node, worker = name or "", (node or "")[:16], (worker or "")[:16]
        if name not in name_idx:
            name_idx[name] = len(names)
            names.append(name)
        if node not in node_idx:
            node_idx[node] = len(nodes)
            nodes.append(node)
        if worker not in worker_idx:
            worker_idx[worker] = len(workers)
            workers.append(worker)
            worker_pids.append(pid)
        rows.append(
            [
                name_idx[name],
                int(start - t0_ms),
                int(end - start),
                node_idx[node],
                worker_idx[worker],
            ]
        )
    return {
        "t0_unix_ms": t0_ms,
        "names": names,
        "nodes": nodes,
        "workers": workers,
        "worker_pids": worker_pids,
        "rows": rows,
        "truncated": truncated,
    }


class RuntimeEnvSetupTracker:
    """Collects runtime environment creation times across the cluster.

    Queries the Ray State API for all runtime environments and reports
    aggregate statistics (mean, stdev) for creation time.

    Usage::

        # After a pipeline or job completes:
        stats = RuntimeEnvSetupTracker.collect()
    """

    @staticmethod
    def collect() -> List[Dict[str, Any]]:
        try:
            groups: Dict[str, List[float]] = {}
            for env in list_runtime_envs(limit=1000):
                if env.creation_time_ms is None:
                    continue
                label = "+".join(sorted(env.runtime_env.keys()))
                groups.setdefault(label, []).append(env.creation_time_ms)
        except Exception:
            logger.warning("Failed to query runtime env creation times.", exc_info=True)
            return []

        results: List[Dict[str, Any]] = []
        for label, times in groups.items():
            mean = sum(times) / len(times)
            variance = sum((t - mean) ** 2 for t in times) / len(times)
            results.append(
                {
                    "runtime_env_type": label,
                    "count": len(times),
                    "mean_creation_time_ms": round(mean, 2),
                    "stdev_creation_time_ms": round(math.sqrt(variance), 2),
                }
            )
        return results


def benchmark_py_modules() -> List[str]:
    """Return paths to benchmark.py and the profiling
    package for use in runtime_env py_modules."""
    dataset_dir = os.path.dirname(os.path.realpath(__file__))
    return [
        os.path.realpath(__file__),
        os.path.join(dataset_dir, "profiling"),
        # The sampler actor class must be importable on every worker node, not just
        # wherever the driver ran.
        os.path.join(dataset_dir, "node_memory_monitor.py"),
    ]


class BenchmarkMetric(Enum):
    RUNTIME = "time"
    NUM_ROWS = "num_rows"
    THROUGHPUT = "tput"
    ACCURACY = "accuracy"
    OBJECT_STORE_SPILLED_TOTAL_GB = "object_store_spilled_total_gb"
    OBJECT_STORE_MEMORY_USED_PEAK_GB = "object_store_memory_used_peak_gb"
    OBJECT_STORE_MEMORY_UTILIZATION_PEAK = "object_store_memory_utilization_peak"


class Benchmark:
    """Runs benchmarks in a way that's compatible with our release test infrastructure.

    Here's an example of typical usage:

    .. testcode::

        import time
        from benchmark import Benchmark

        def sleep(sleep_s)
            time.sleep(sleep_s)
            # Return any extra metrics you want to record. This can include
            # configuration parameters, accuracy, etc.
            return {"sleep_s": sleep_s}

        benchmark = Benchmark()
        benchmark.run_fn("short", sleep, 1)
        benchmark.run_fn("long", sleep, 10)
        benchmark.write_result()

    This code outputs a JSON file with contents like this:

    .. code-block:: json

        {"short": {"time": 1.0, "sleep_s": 1}, "long": {"time": 10.0 "sleep_s": 10}}
    """

    def __init__(self):
        self.result = {}

    def run_fn(
        self,
        name: str,
        fn: Callable[..., Dict[Union[str, BenchmarkMetric], Any]],
        *fn_args,
        **fn_kwargs,
    ):
        """Benchmark a function.

        This is the most general benchmark utility available. Use it if the other
        methods are too specific.

        ``run_fn`` automatically records the runtime of ``fn``. To report additional
        metrics, return a ``Dict[str, Any]`` of metric labels to metric values from your
        function.
        """
        gc.collect()

        print(f"Running case: {name}")
        state = get_state_from_address(ray.get_runtime_context().gcs_address)

        # Per-node worker memory with stage provenance. Off unless
        # RAY_DATA_BENCH_NODE_MEM_MONITOR=1; a no-op context manager otherwise, so the
        # default path is byte-for-byte what it was.
        node_mem = _node_memory_monitor(name)

        case_start_unix_ms = time.time() * 1000.0

        with node_mem, ObjectStoreMemorySampler(state) as memory_sampler:
            start_time = time.perf_counter()
            start_spilled_bytes = _get_spilled_bytes_total(state)

            try:
                fn_output = fn(*fn_args, **fn_kwargs)
            finally:
                duration = time.perf_counter() - start_time

        assert fn_output is None or isinstance(fn_output, dict), fn_output

        spilled_bytes_total = _get_spilled_bytes_total(state) - start_spilled_bytes
        curr_case_metrics = {
            BenchmarkMetric.RUNTIME.value: duration,
            BenchmarkMetric.OBJECT_STORE_SPILLED_TOTAL_GB.value: _bytes_to_gb(
                spilled_bytes_total
            ),
            BenchmarkMetric.OBJECT_STORE_MEMORY_USED_PEAK_GB.value: _bytes_to_gb(
                memory_sampler.peak_used_bytes
            ),
            BenchmarkMetric.OBJECT_STORE_MEMORY_UTILIZATION_PEAK.value: round(
                memory_sampler.peak_utilization,
                4,
            ),
            **node_mem.summary(),
            **collect_task_distribution(case_start_unix_ms),
        }
        if isinstance(fn_output, dict):
            for key, value in fn_output.items():
                if isinstance(key, BenchmarkMetric):
                    curr_case_metrics[key.value] = value
                elif isinstance(key, str):
                    curr_case_metrics[key] = value
                else:
                    raise ValueError(f"Unexpected metric key type: {type(key)}")

        self.result[name] = curr_case_metrics
        print(f"Result of case {name}: {curr_case_metrics}")

    def write_result(self):
        """Write all results to the appropriate JSON file.

        Our release test infrastructure consumes the JSON file and uploads the results
        to our internal dashboard.
        """
        # 'TEST_OUTPUT_JSON' is set in the release test environment.
        test_output_json = os.environ.get("TEST_OUTPUT_JSON", "./result.json")
        with open(test_output_json, "w") as f:
            f.write(json.dumps(self.result))

        print(f"Finished benchmark, metrics exported to '{test_output_json}':")
        print(json.dumps(self.result, indent=4))
