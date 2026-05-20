import gc
import json
import logging
import math
import os
import threading
import time
from enum import Enum
from typing import Any, Callable, Dict, List, Union
import dataclasses
import ray
from ray._private.internal_api import get_memory_info_reply, get_state_from_address
from ray.util.state import list_runtime_envs

logger = logging.getLogger(__name__)


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
        "avg_scheduling_loop_duration_s": summary.streaming_exec_schedule_avg_s,
        "max_scheduling_loop_duration_s": summary.streaming_exec_schedule_max_s,
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
    """Return a list containing the absolute path to benchmark.py for use in runtime_env py_modules."""
    return [os.path.abspath(__file__)]


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

        with ObjectStoreMemorySampler(state) as memory_sampler:
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
