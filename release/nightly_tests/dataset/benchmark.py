import gc
import json
import os
import time
from typing import Callable

from ray.data.dataset import Dataset


class Benchmark:
    """Benchmark class used for Ray Datasets.
    Call ``run(fn)`` to benchmark a specific piece of code/function.
    Call ``write_result()`` to write benchmark result in file.
    Result can be rendered in dashboard later through other tool.
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
        self.time = 0
        print(f"Running benchmark: {name}")

    def run(self, name: str, fn: Callable[..., Dataset], **fn_run_args):
        gc.collect()

        print(f"Running case: {name}")
        start_time = time.perf_counter()
        output_ds = fn(**fn_run_args)
        output_ds.fully_executed()
        duration = time.perf_counter() - start_time

        # TODO(chengsu): Record more metrics based on dataset stats.
        self.result[name] = {"time": duration}
        self.time += duration
        print(f"Result of case {name}: {self.result[name]}")

    def write_result(self):
        test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/result.json")
        with open(test_output_json, "w") as f:
            f.write(json.dumps(self.result))
        print(f"Finish benchmark: {self.name} in {self.time} seconds")
