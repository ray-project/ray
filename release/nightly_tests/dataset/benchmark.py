import gc
import json
import os
import time
from typing import Callable, Dict, Iterator

from ray.data.dataset import Dataset
from typing import Any


from enum import Enum


class BenchmarkMetric(Enum):
    RUNTIME = "time"
    THROUGHPUT = "tput"


class Benchmark:
    """Utility class used for Ray Data release tests and benchmarks, which works
    for both local and distributed benchmarking. When run on the nightly release
    test pipeline, the results are written to our internal database, which
    can then be rendered on the dashboard. Usage tips:

    A typical workflow would be:

    benchmark = Benchmark("benchmark-name")

    # set up (such as input read or generation)
    ...

    benchmark.run_materialize_ds("case-1", fn_1)
    benchmark.run_iterate_ds("case-2", ds_iter)
    benchmark.run_fn("case-3", fn_3)

    benchmark.write_result("output.json")

    See example usage in ``aggregate_benchmark.py``.
    """

    def __init__(self, name):
        self.name = name
        self.result = {}
        print(f"Running benchmark: {name}")

    def run_materialize_ds(
        self,
        name: str,
        fn: Callable[..., Dataset],
        *fn_args,
        **fn_run_args,
    ):
        """Run a benchmark on materializing a Ray Dataset. ``fn`` is expected to
        return the Dataset which is to be materialized. Runtime and throughput
        are automatically calculated and reported."""

        gc.collect()

        print(f"Running case: {name}")
        start_time = time.perf_counter()
        output_ds = fn(*fn_args, **fn_run_args)
        output_ds.materialize()
        duration = time.perf_counter() - start_time

        # TODO(chengsu): Record more metrics based on dataset stats.
        self.result[name] = {
            BenchmarkMetric.RUNTIME.value: duration,
            BenchmarkMetric.THROUGHPUT.value: output_ds.count() / duration,
        }
        print(f"Result of case {name}: {self.result[name]}")

    def run_iterate_ds(
        self,
        name: str,
        ds_iterator: Iterator[Any],
        batch_size: int,
    ):
        """Run a benchmark iterating over a dataset (not limited to Ray Data,
        e.g. could be an iterator over Torch dataloader). ``ds_iterator`` is expected
        to be an iterator over the dataset, yielding batches of the given size.
        Runtime and throughput are automatically calculated and reported."""

        gc.collect()

        print(f"Running case: {name}")
        start_time = time.perf_counter()
        record_count = 0
        for batch in ds_iterator:
            # Note, we are using `batch_size` as an approximate way to get the total
            # record count, because each batch type has its own method of getting
            # batch length, also may depend on the underlying data structure, etc.
            record_count += batch_size
            pass
        duration = time.perf_counter() - start_time
        self.result[name] = {
            BenchmarkMetric.RUNTIME.value: duration,
            BenchmarkMetric.THROUGHPUT.value: record_count / duration,
        }
        print(f"Result of case {name}: {self.result[name]}")

    def run_fn(
        self, name: str, fn: Callable[..., Dict[str, Any]], *fn_args, **fn_kwargs
    ):
        """Run a benchmark for a specific function. ``fn`` is expected to return a
        Dict[str, Any] of metric labels to metric values, which are reported at
        the end of the benchmark. Runtime is automatically calculated and reported."""

        gc.collect()

        print(f"Running case: {name}")
        start_time = time.perf_counter()
        # e.g. fn may output a dict of metrics
        fn_output = fn(*fn_args, **fn_kwargs)
        duration = time.perf_counter() - start_time

        curr_case_metrics = {
            BenchmarkMetric.RUNTIME.value: duration,
        }
        if isinstance(fn_output, dict):
            # Filter out metrics which are not in BenchmarkMetric,
            # to ensure proper format of outputs.
            for m in BenchmarkMetric:
                metric_value = fn_output.get(m.value)
                if metric_value:
                    curr_case_metrics[m.value] = metric_value

        self.result[name] = curr_case_metrics
        print(f"Result of case {name}: {curr_case_metrics}")

    def write_result(self, output_path="/tmp/result.json"):
        """Write all collected benchmark results to `output_path`.
        The result is a dict of the form:
        ``{case_name: {metric_name: metric_value, ...}}``."""

        test_output_json = os.environ.get("TEST_OUTPUT_JSON", output_path)
        with open(test_output_json, "w") as f:
            self.result["name"] = self.name
            f.write(json.dumps(self.result))

        print(
            f"Finished benchmark {self.name}, metrics exported to {test_output_json}:"
        )
        print(self.result)
