import json
import os
import time
from typing import Callable, Dict, Any, Optional

from enum import Enum


class BenchmarkMetric(Enum):
    RUNTIME = "time"
    NUM_ROWS = "num_rows"
    THROUGHPUT = "tput"
    ACCURACY = "accuracy"


def run_benchmark(
    benchmark_fn: Callable[[], Optional[Dict]], config: Optional[Dict[str, Any]] = None
):
    """Run a benchmark and write the results to the appropriate output file.

    By default, this function measures the total runtime of the benchmark. You can
    specify additional metrics by returning a dictionary from the benchmark function.

    Example:

        .. testcode::

            import time
            from benchmark import run_benchmark

            sleep_s = 1

            def benchmark_sleep():
                time.sleep(sleep_s)
                return {"extra_metric": 42}

            run_benchmark(benchmark_sleep, config={"sleep_s": sleep_s})


        The above code will write the following results to the output file:

        .. code-block:: json

            {'sleep_s': 1, 'time': 1.005..., 'extra_metric': 42}

    Args:
        benchmark_fn: A function that runs the benchmark. The function can optionally
            return a dictionary of extra metrics.
        config: A dictionary of configuration parameters for the benchmark.
    """
    if config is None:
        config = {}

    metrics = _run_benchmark_fn(benchmark_fn)
    _write_results(metrics, config)


def _run_benchmark_fn(benchmark_fn) -> Dict[str, Any]:
    print("Running benchmark...")
    start_time = time.time()
    fn_output = benchmark_fn()
    end_time = time.time()

    metrics = {BenchmarkMetric.RUNTIME.value: end_time - start_time}
    if isinstance(fn_output, dict):
        metrics.update(fn_output)

    return metrics


def _write_results(metrics: Dict[BenchmarkMetric, Any], config: Dict[str, Any]):
    results = {**config, **metrics}

    write_path = os.environ.get("TEST_OUTPUT_JSON", "/tmp/result.json")
    with open(write_path, "w") as f:
        f.write(json.dumps(results))

    print(f"Finished benchmark, results exported to '{write_path}':")
    print(results)
