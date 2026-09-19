import asyncio
import gc
import json
import logging
import math
import os
import threading
import time
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union
import dataclasses
import ray
from ray._private.internal_api import get_memory_info_reply, get_state_from_address
from ray.core.generated import common_pb2
from ray.data import DataContext
from ray.util.state import list_runtime_envs

try:
    import benchmark_health_checks as health_checks
except ModuleNotFoundError as exc:
    if exc.name != "benchmark_health_checks":
        raise
    from release.nightly_tests.dataset import benchmark_health_checks as health_checks

logger = logging.getLogger(__name__)

PROMETHEUS_QUERY_TIMEOUT_S = 30
PROMETHEUS_RANGE_STEP_S = 15

PROMETHEUS_CATCH_UP_TIMEOUT_S = 120
PROMETHEUS_CATCH_UP_POLL_INTERVAL_S = 5


def _import_prometheus_client():
    # The release test runner copies prometheus_metrics.py into the workload directory.
    try:
        from prometheus_metrics import PrometheusClient
    except ModuleNotFoundError as exc:
        if exc.name != "prometheus_metrics":
            raise
        # Use the source module when importing the benchmark from the repo root.
        from release.ray_release.command_runner._prometheus_metrics import (
            PrometheusClient,
        )
    return PrometheusClient


def _run_prometheus_query(query_type: str, **kwargs):
    PrometheusClient = _import_prometheus_client()

    async def run_query():
        client = PrometheusClient()
        try:
            return await asyncio.wait_for(
                client.query_prometheus(query_type, **kwargs),
                timeout=PROMETHEUS_QUERY_TIMEOUT_S,
            )
        except asyncio.TimeoutError as exc:
            raise RuntimeError(
                f"Prometheus query timed out after {PROMETHEUS_QUERY_TIMEOUT_S} "
                f"seconds: {kwargs.get('query')!r}"
            ) from exc
        finally:
            await client.close()

    return asyncio.run(run_query())


def _query_prometheus(query: str, timestamp: float):
    """Instant query. Returns entries with a single ``value`` sample."""
    return _run_prometheus_query("query", query=query, time=timestamp)


def _query_prometheus_range(query: str, start_unix_time: float, end_unix_time: float):
    """Range query. Returns entries with a list of ``values`` samples."""
    return _run_prometheus_query(
        "query_range",
        query=query,
        start=int(start_unix_time),
        end=int(math.ceil(end_unix_time)),
        step=PROMETHEUS_RANGE_STEP_S,
    )


def _session_label() -> str:
    session_name = ray.get_runtime_context().get_session_name()
    return f"SessionName={json.dumps(session_name)}"


def _get_positive_worker_failure_metrics(metric_name: str) -> List[Dict[str, Any]]:
    """Return positive worker-failure counters for the current Ray session."""
    selector = f"{metric_name}{{{_session_label()}}}"
    results = _query_prometheus(f"sum({selector}) by (Type, Name) > 0", time.time())
    if results is None:
        raise RuntimeError(
            f"Failed to query Prometheus for worker-failure metric {metric_name!r}."
        )
    return results


def _get_node_sample_unix_times() -> List[float]:
    """Return each node's latest scrape timestamp; raises if Prometheus is down."""
    results = _query_prometheus(
        f"timestamp(ray_node_mem_used_host{{{_session_label()}}})", time.time()
    )
    if results is None:
        raise RuntimeError("Failed to query Prometheus for node scrape timestamps.")
    timestamps = []
    for entry in results:
        try:
            timestamps.append(float(entry["value"][1]))
        except (KeyError, IndexError, TypeError, ValueError):
            continue
    return timestamps


def _get_oldest_node_sample_unix_time() -> Optional[float]:
    timestamps = _get_node_sample_unix_times()
    return min(timestamps) if timestamps else None


def _wait_for_prometheus_to_catch_up(workload_end_unix_time: float) -> bool:
    # Diagnostic: the catch-up wait assumes one series per node.
    num_series = len(_get_node_sample_unix_times())
    num_alive_nodes = sum(1 for node in ray.nodes() if node["Alive"])
    print(
        f"Prometheus reports scrape timestamps for {num_series} node(s); "
        f"{num_alive_nodes} node(s) are alive."
    )
    if num_series != num_alive_nodes:
        logger.warning(
            "Prometheus node series count differs from the number of alive nodes. "
            "The metrics catch-up wait may be unreliable."
        )

    caught_up = health_checks.wait_for_metrics_to_catch_up(
        workload_end_unix_time=workload_end_unix_time,
        oldest_node_sample_unix_time=_get_oldest_node_sample_unix_time,
        timeout_s=PROMETHEUS_CATCH_UP_TIMEOUT_S,
        poll_interval_s=PROMETHEUS_CATCH_UP_POLL_INTERVAL_S,
    )
    if not caught_up:
        logger.warning(
            "Prometheus did not report post-workload samples from every node within "
            f"{PROMETHEUS_CATCH_UP_TIMEOUT_S}s. Running the cluster health checks on "
            "possibly incomplete metrics."
        )
    return caught_up


def _get_peak_object_store_utilization(
    start_unix_time: float, end_unix_time: float
) -> Optional[float]:
    """Return the cluster-wide object-store peak over the window as a fraction."""
    session_label = _session_label()
    query = (
        f"sum(ray_object_store_memory{{{session_label}}}) / on() "
        f'sum(ray_resources{{Name="object_store_memory",{session_label}}})'
    )
    results = _query_prometheus_range(query, start_unix_time, end_unix_time)
    if results is None:
        raise RuntimeError("Failed to query Prometheus for object-store utilization.")
    return health_checks.peak_metric_value(results)


def _get_unexpectedly_dead_nodes() -> List[str]:
    reasons = {
        common_pb2.NodeDeathInfo.Reason.Value("UNEXPECTED_TERMINATION"),
        common_pb2.NodeDeathInfo.Reason.Value("UNSPECIFIED"),
    }
    return health_checks.find_unexpectedly_dead_nodes(ray.nodes(), reasons)


def _get_peak_head_node_memory_used_bytes(
    start_unix_time: float, end_unix_time: float
) -> float:
    """Return peak head-node physical memory reported by Prometheus."""
    assert start_unix_time <= end_unix_time, (start_unix_time, end_unix_time)

    # Read only the head node from the current Ray session.
    session_name = ray.get_runtime_context().get_session_name()
    metric_selector = (
        "ray_node_mem_used_host{"
        f'RayNodeType="head",SessionName={json.dumps(session_name)}'
        "}"
    )

    # Ray's default Prometheus scrape interval is 10 seconds, so shorter benchmark
    # cases may have no sample in their time window.
    # Prometheus requires a positive range, so use 1 ms for a zero-duration case.
    duration_ms = max(1, math.ceil((end_unix_time - start_unix_time) * 1000))
    results = _query_prometheus(
        f"max_over_time({metric_selector}[{duration_ms}ms])",
        end_unix_time,
    )
    if results is None:
        raise RuntimeError("Failed to query Prometheus for head-node physical memory.")

    # The session and head-node filters should match exactly one time series.
    if len(results) != 1:
        raise RuntimeError(
            f"Expected one head-node physical-memory result, got {len(results)}."
        )

    # Prometheus returns an instant value as [timestamp, value].
    try:
        _, raw_value = results[0]["value"]
        peak_memory_bytes = float(raw_value)
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(
            "Prometheus returned an invalid head-node physical-memory value."
        ) from exc

    if not math.isfinite(peak_memory_bytes):
        raise RuntimeError(
            "Prometheus returned an invalid head-node physical-memory value."
        )

    return peak_memory_bytes


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
    """Return the paths workers need to ``import benchmark`` via ``py_modules``."""
    dataset_dir = os.path.dirname(os.path.realpath(__file__))
    return [
        os.path.realpath(__file__),
        os.path.join(dataset_dir, "benchmark_health_checks.py"),
        os.path.join(dataset_dir, "profiling"),
    ]


class BenchmarkMetric(Enum):
    RUNTIME = "time"
    NUM_ROWS = "num_rows"
    THROUGHPUT = "tput"
    ACCURACY = "accuracy"
    OBJECT_STORE_SPILLED_TOTAL_GB = "object_store_spilled_total_gb"
    OBJECT_STORE_MEMORY_USED_PEAK_GB = "object_store_memory_used_peak_gb"
    OBJECT_STORE_MEMORY_UTILIZATION_PEAK = "object_store_memory_utilization_peak"
    HEAD_NODE_MEMORY_USED_PEAK_GB = "head_node_memory_used_peak_gb"


class Benchmark:
    """Runs benchmarks in a way that's compatible with our release test infrastructure.

    Args:
        fail_on_worker_oom: Fail if Ray's memory monitor evicted a task or actor
            worker. Expected idle-worker evictions are ignored.
        fail_on_unexpected_worker_failure: Fail if a worker died without an eviction
            recorded by Ray's memory monitor.
        fail_on_dead_nodes: Fail if a node died unexpectedly during the test.
        debug_progress_manager: Include verbose resource-manager details in Ray Data
            progress output.
        max_object_store_utilization: Fail if cluster-wide object-store utilization,
            as reported by Prometheus over the whole benchmark run, exceeded this
            fraction of aggregate capacity. Set to ``None`` to disable the check.
        max_head_node_memory_bytes: If set, query Prometheus after each case and fail
            if peak physical memory used on the head node exceeds this limit.

    The cluster health checks run in ``write_result`` after the metrics JSON is
    written, and only when every case succeeded. The worker and object-store checks
    need Prometheus (``RAY_PROMETHEUS_HOST``); outside the release infrastructure,
    disable them with ``fail_on_worker_oom=False``,
    ``fail_on_unexpected_worker_failure=False`` and
    ``max_object_store_utilization=None``.

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

    def __init__(
        self,
        *,
        fail_on_worker_oom: bool = True,
        fail_on_unexpected_worker_failure: bool = True,
        fail_on_dead_nodes: bool = True,
        debug_progress_manager: bool = True,
        max_object_store_utilization: Optional[float] = 1.0,
        max_head_node_memory_bytes: Optional[int] = None,
    ):
        if (
            max_object_store_utilization is not None
            and max_object_store_utilization < 0
        ):
            raise ValueError("max_object_store_utilization must be nonnegative.")
        if max_head_node_memory_bytes is not None and max_head_node_memory_bytes <= 0:
            raise ValueError("max_head_node_memory_bytes must be greater than 0.")

        self.result = {}
        self._fail_on_worker_oom = fail_on_worker_oom
        self._fail_on_unexpected_worker_failure = fail_on_unexpected_worker_failure
        self._fail_on_dead_nodes = fail_on_dead_nodes
        self._max_object_store_utilization = max_object_store_utilization
        self._max_head_node_memory_bytes = max_head_node_memory_bytes
        self._failed_cases: List[str] = []
        self._first_case_start_unix_time: Optional[float] = None
        self._last_case_end_unix_time: Optional[float] = None

        DataContext.get_current().debug_resource_manager = debug_progress_manager

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

        Call ``write_result`` in a ``finally`` block to save metrics even if this
        method fails.
        """
        gc.collect()

        print(f"Running case: {name}")
        state = get_state_from_address(ray.get_runtime_context().gcs_address)

        with ObjectStoreMemorySampler(state) as memory_sampler:
            start_unix_time = time.time()
            start_time = time.perf_counter()
            start_spilled_bytes = _get_spilled_bytes_total(state)

            try:
                fn_output = fn(*fn_args, **fn_kwargs)
            except BaseException:
                self._failed_cases.append(name)
                raise
            finally:
                duration = time.perf_counter() - start_time
                end_unix_time = time.time()
                if self._first_case_start_unix_time is None:
                    self._first_case_start_unix_time = start_unix_time
                self._last_case_end_unix_time = end_unix_time

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

        max_head_node_memory_bytes = self._max_head_node_memory_bytes
        peak_head_node_memory_bytes = None
        if max_head_node_memory_bytes is not None:
            peak_head_node_memory_bytes = _get_peak_head_node_memory_used_bytes(
                start_unix_time, end_unix_time
            )
            curr_case_metrics[
                BenchmarkMetric.HEAD_NODE_MEMORY_USED_PEAK_GB.value
            ] = _bytes_to_gb(peak_head_node_memory_bytes)

        print(f"Result of case {name}: {curr_case_metrics}")

        if (
            peak_head_node_memory_bytes is not None
            and max_head_node_memory_bytes is not None
            and peak_head_node_memory_bytes > max_head_node_memory_bytes
        ):
            raise AssertionError(
                f"Benchmark case {name!r} peak head-node physical memory "
                f"({_bytes_to_gb(peak_head_node_memory_bytes)} GiB) exceeded the "
                f"configured limit "
                f"({_bytes_to_gb(max_head_node_memory_bytes)} GiB)."
            )

    def write_result(self):
        """Write all results to the appropriate JSON file.

        Our release test infrastructure consumes the JSON file and uploads the results
        to our internal dashboard.
        """
        # 'TEST_OUTPUT_JSON' is set in the release test environment.
        test_output_json = os.environ.get("TEST_OUTPUT_JSON", "./result.json")
        with open(test_output_json, "w") as f:
            f.write(json.dumps(self.result))

        print(f"Benchmark metrics exported to '{test_output_json}':")
        print(json.dumps(self.result, indent=4))

        # Only check a successful run, so a failing case reports its own error.
        if self._failed_cases:
            print(
                "Skipping cluster health checks because these benchmark cases "
                f"failed: {self._failed_cases}"
            )
            return

        failures = self._run_cluster_health_checks()
        if failures:
            raise AssertionError(
                "Benchmark safety checks failed:\n- " + "\n- ".join(failures)
            )

    def _run_cluster_health_checks(self) -> List[str]:
        """Return a description of every enabled check that failed."""
        failures = []

        worker_checks_enabled = (
            self._fail_on_worker_oom or self._fail_on_unexpected_worker_failure
        )
        has_run_window = (
            self._first_case_start_unix_time is not None
            and self._last_case_end_unix_time is not None
        )
        object_store_check_enabled = (
            self._max_object_store_utilization is not None and has_run_window
        )
        if self._max_object_store_utilization is not None and not has_run_window:
            print("Skipping the object-store check: no benchmark case ran.")

        if (worker_checks_enabled or object_store_check_enabled) and has_run_window:
            print("Waiting for Prometheus to scrape post-workload metrics...")
            _wait_for_prometheus_to_catch_up(self._last_case_end_unix_time)

        if worker_checks_enabled:
            failures.extend(
                health_checks.check_worker_failures(
                    fail_on_worker_oom=self._fail_on_worker_oom,
                    fail_on_unexpected_worker_failure=(
                        self._fail_on_unexpected_worker_failure
                    ),
                    query_positive_counters=_get_positive_worker_failure_metrics,
                )
            )

        if self._fail_on_dead_nodes:
            dead_nodes = _get_unexpectedly_dead_nodes()
            if dead_nodes:
                failures.append(f"Dead nodes found, node IDs: {dead_nodes}")

        if object_store_check_enabled:
            peak_utilization = _get_peak_object_store_utilization(
                self._first_case_start_unix_time, self._last_case_end_unix_time
            )
            if peak_utilization is None:
                logger.warning(
                    "Prometheus returned no object-store utilization samples for the "
                    "benchmark window; skipping the object-store check."
                )
            else:
                print(
                    f"Peak cluster-wide object-store utilization: {peak_utilization:.1%}"
                )
            failures.extend(
                health_checks.check_object_store_utilization(
                    peak_utilization, self._max_object_store_utilization
                )
            )

        return failures
