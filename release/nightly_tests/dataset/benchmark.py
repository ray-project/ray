import gc
import json
import os
import time
from typing import Callable

from ray.data.dataset import Dataset
from typing import Any


from enum import Enum

# class syntax
class BenchmarkMetric(Enum):
    RUNTIME = "time"
    THROUGHPUT = "tput"

class Benchmark:
    """Benchmark class used for Ray Datasets.

    Call ``run(fn)`` to benchmark a specific piece of code/function.
    ``fn`` is expected to return the final Dataset. Benchmark ensures
    final Dataset is fully executed. Plan to add Dataset statistics
    logging in the future.

    Call ``write_result()`` to write benchmark result in file.
    Result can be rendered in dashboard later through other tool.
    We should use this class for any benchmark related to Ray Datasets.
    It works for both local and distribute benchmarking.

    A typical workflow would be:

    benchmark = Benchmark(...)

    # set up (such as input read or generation)
    ...

    benchmark.run(..., fn_1)
    benchmark.run(..., fn_2)
    benchmark.run(..., fn_3)

    benchmark.write_result()

    See example usage in ``aggregate_benchmark.py``.
    """

    def __init__(self, name):
        self.name = name
        self.result = {}
        print(f"Running benchmark: {name}")

    def run(self, name: str, fn: Callable[..., Dataset], **fn_run_args):
        gc.collect()

        print(f"Running case: {name}")
        start_time = time.perf_counter()
        output_ds = fn(**fn_run_args)
        output_ds.materialize()
        duration = time.perf_counter() - start_time

        # TODO(chengsu): Record more metrics based on dataset stats.
        self.result[name] = {BenchmarkMetric.RUNTIME: duration}
        print(f"Result of case {name}: {self.result[name]}")
    
    def run_fn(self, name: str, fn: Callable[..., Any], **fn_run_args):
        gc.collect()

        print(f"Running case: {name}")
        start_time = time.perf_counter()
        # e.g. fn may output a dict of metrics
        fn_output = fn(**fn_run_args)
        duration = time.perf_counter() - start_time

        curr_case_metrics = {
            BenchmarkMetric.RUNTIME: duration,
        }
        if isinstance(fn_output, dict):
            for m in BenchmarkMetric:
                metric_value = fn_output.get(m.value)
                if metric_value:
                    curr_case_metrics[m.value] = tput
        
        self.result[name] = curr_case_metrics
        print(f"Result of case {name}: {curr_case_metrics}")

    def write_result(self, output_path="/tmp/result.json"):
        test_output_json = os.environ.get("TEST_OUTPUT_JSON", output_path)
        with open(test_output_json, "w") as f:
            f.write(json.dumps(self.result))

        print(f"Finished benchmark {self.name}, metrics exported to {test_output_json}:")
        print(self.result)