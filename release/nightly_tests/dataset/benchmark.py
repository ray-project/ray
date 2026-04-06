import gc
import json
import logging
import math
import os
import time
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

import ray
from ray._private.internal_api import get_memory_info_reply, get_state_from_address
from ray.data._internal.execution.execution_callback import ExecutionCallback
from ray.data._internal.execution.streaming_executor import StreamingExecutor
from ray.util.state import list_runtime_envs

logger = logging.getLogger(__name__)


def _get_spilled_bytes_total() -> float:
    """Get the total number of spilled bytes across the cluster."""
    memory_info = get_memory_info_reply(
        get_state_from_address(ray.get_runtime_context().gcs_address)
    )
    return memory_info.store_stats.spilled_bytes_total


def _bytes_to_gb(b: float) -> float:
    return round(b / (1024**3), 4)


class OperatorStatsTracker(ExecutionCallback):
    """Records per-operator start time and duration.

    Tracks when each operator first submits a task (start) and when it
    completes (duration = completion time - start time).

    Uses class-level state because the planner instantiates callback classes
    with ``cls()``, so callers can't hold a reference to the instance.

    Usage::

        ctx = ray.data.DataContext.get_current()
        ctx.custom_execution_callback_classes.append(OperatorStatsTracker)

        # ... run pipeline ...

        metrics = OperatorStatsTracker.collect()
    """

    _op_start: Dict[str, float] = {}
    _op_end: Dict[str, Optional[float]] = {}
    _start_time: float = 0

    def before_execution_starts(self, executor: "StreamingExecutor"):
        cls = type(self)
        cls._start_time = executor._start_time
        cls._op_start.clear()
        cls._op_end.clear()

    def on_execution_step(self, executor: "StreamingExecutor"):
        cls = type(self)
        if executor._topology is None:
            return
        for i, op in enumerate(executor._topology):
            op_key = f"{op.name}_{i}"
            if op_key not in cls._op_start and op.metrics.num_tasks_submitted > 0:
                cls._op_start[op_key] = time.perf_counter()
                cls._op_end[op_key] = None
            if (
                op_key in cls._op_start
                and cls._op_end[op_key] is None
                and op.has_completed()
            ):
                cls._op_end[op_key] = time.perf_counter()

    @classmethod
    def collect(cls) -> Dict[str, Any]:
        stats: Dict[str, Dict[str, Any]] = {}
        now_wall = time.time()
        now_perf = time.perf_counter()
        for key, start in cls._op_start.items():
            end = cls._op_end.get(key)
            duration_s = round(end - start, 2) if end is not None else None
            start_dt = cls._make_readable_timestamp(ts=now_wall - (now_perf - start))
            stats[key] = {
                "start": start_dt,
                "duration_s": duration_s,
            }

        seconds_since_start = now_perf - cls._start_time
        start_time = cls._make_readable_timestamp(ts=now_wall - seconds_since_start)

        return {
            "execution_loop_start_time": start_time,
            "op_stats": stats,
        }

    @classmethod
    def _make_readable_timestamp(cls, ts: float) -> str:
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )


class RuntimeEnvSetupTracker:
    """Collects runtime environment creation times across the cluster.

    Queries the Ray State API for all runtime environments and reports
    aggregate statistics (mean, stdev) for creation time.

    Usage::

        # After a pipeline or job completes:
        stats = RuntimeEnvSetupTracker.collect()
    """

    @staticmethod
    def collect() -> Dict[str, Any]:
        try:
            groups: Dict[str, List[float]] = {}
            for env in list_runtime_envs():
                if env.creation_time_ms is None:
                    continue
                key = json.dumps(env.runtime_env, sort_keys=True)
                groups.setdefault(key, []).append(env.creation_time_ms)
        except Exception:
            logger.warning("Failed to query runtime env creation times.", exc_info=True)
            return {}

        if not groups:
            return {}

        result: Dict[str, Any] = {}
        for key, times in groups.items():
            mean = sum(times) / len(times)
            variance = sum((t - mean) ** 2 for t in times) / len(times)
            result[key] = {
                "num_nodes": len(times),
                "mean_creation_time_ms": round(mean, 2),
                "stdev_creation_time_ms": round(math.sqrt(variance), 2),
            }
        return result


class BenchmarkMetric(Enum):
    RUNTIME = "time"
    NUM_ROWS = "num_rows"
    THROUGHPUT = "tput"
    ACCURACY = "accuracy"
    OBJECT_STORE_SPILLED_TOTAL_GB = "object_store_spilled_total_gb"


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
        start_time = time.perf_counter()
        start_spilled_bytes = _get_spilled_bytes_total()
        fn_output = fn(*fn_args, **fn_kwargs)
        assert fn_output is None or isinstance(fn_output, dict), fn_output
        duration = time.perf_counter() - start_time
        spilled_bytes_total = _get_spilled_bytes_total() - start_spilled_bytes

        curr_case_metrics = {
            BenchmarkMetric.RUNTIME.value: duration,
            BenchmarkMetric.OBJECT_STORE_SPILLED_TOTAL_GB.value: _bytes_to_gb(
                spilled_bytes_total
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
