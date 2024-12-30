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

    # Extra metrics not matching the above categories/keys, stored as a Dict[str, Any].
    EXTRA_METRICS = "extra_metrics"


def run_benchmark(
    benchmark_fn: Callable[[], Dict], config: Optional[Dict[str, Any]] = None
):
    if config is None:
        config = {}

    metrics = _run_benchmark_fn(benchmark_fn)
    _write_results(metrics, config)


def _run_benchmark_fn(benchmark_fn) -> Dict[BenchmarkMetric, Any]:
    metrics = {}

    print("Running benchmark...")
    start_time = time.time()
    fn_output = benchmark_fn()
    end_time = time.time()

    metrics[BenchmarkMetric.RUNTIME] = end_time - start_time

    extra_metrics = {}
    if isinstance(fn_output, dict):
        for metric_key, metric_val in fn_output.items():
            if isinstance(metric_key, BenchmarkMetric):
                metrics[metric_key.value] = metric_val
            else:
                extra_metrics[metric_key] = metric_val

        metrics[BenchmarkMetric.EXTRA_METRICS] = extra_metrics

    return metrics


def _write_results(metrics: Dict[BenchmarkMetric, Any], config: Dict[str, Any]):
    results = {**config, **metrics}

    write_path = os.environ.get("TEST_OUTPUT_JSON", "/tmp/result.json")
    with open(write_path, "w") as f:
        f.write(json.dumps(results))

    print(f"Finished benchmark, results exported to '{write_path}':")
    print(results)
