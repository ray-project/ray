import gc
import json
import os
import time
from typing import Callable, Dict

from ray.data.dataset import Dataset
from typing import Any


from enum import Enum

import pyarrow as pa
import pandas as pd


class BenchmarkMetric(Enum):
    RUNTIME = "time"
    THROUGHPUT = "tput"
    ACCURACY = "accuracy"

    # Extra metrics not matching the above categories/keys, stored as a Dict[str, Any].
    EXTRA_METRICS = "extra_metrics"


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
    # Could be Ray Data iterator, Torch DataLoader, TF Dataset...
    benchmark.run_iterate_ds("case-2", dataset)
    benchmark.run_fn("case-3", fn_3)

    # Writes a JSON with metrics of the form:
    # {"case-1": {...}, "case-2": {...}, "case-3": {...}}
    benchmark.write_result()

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
        **fn_kwargs,
    ):
        """Run a benchmark on materializing a Ray Dataset. ``fn`` is expected to
        return the Dataset which is to be materialized. Runtime and throughput
        are automatically calculated and reported."""

        gc.collect()

        print(f"Running case: {name}")
        start_time = time.perf_counter()
        output_ds = fn(*fn_args, **fn_kwargs)
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
        dataset: Any,
    ):
        """Run a benchmark iterating over a dataset. Runtime and throughput
        are automatically calculated and reported. Supported dataset types are:
        - Ray Dataset (`ray.data.Dataset`)
        - iterator over Ray Dataset (`ray.data.iterator._IterableFromIterator` from
            `.iter_batches()`,`.iter_torch_batches()`, `.iter_tf_batches()`)
        - Torch DataLoader (`torch.utils.data.DataLoader`)
        - TensorFlow Dataset (`tf.data.Dataset`)
        """
        # Import TF/Torch within this method, as not all benchmarks
        # will use/install these libraries.
        import tensorflow as tf
        import torch

        gc.collect()

        print(f"Running case: {name}")
        start_time = time.perf_counter()
        record_count = 0
        ds_iterator = iter(dataset)
        for batch in ds_iterator:
            # Unwrap list to get the underlying batch format.
            if isinstance(batch, (list, tuple)) and len(batch) > 0:
                batch = batch[0]

            # Get the batch size for various batch formats.
            if isinstance(batch, dict):
                feature_lengths = {k: len(batch[k]) for k in batch}
                batch_size = max(feature_lengths.values())
                continue
            elif isinstance(batch, (pa.Table, pd.DataFrame)):
                batch_size = len(batch)
                continue
            elif isinstance(batch, torch.Tensor):
                batch_size = batch.size(dim=0)
            elif isinstance(batch, tf.Tensor):
                batch_size = batch.shape.as_list()[0]
            else:
                raise TypeError(f"Unexpected batch type: {type(batch)}")
            record_count += batch_size

        duration = time.perf_counter() - start_time
        self.result[name] = {
            BenchmarkMetric.RUNTIME.value: duration,
            BenchmarkMetric.THROUGHPUT.value: record_count / duration,
        }
        print(f"Result of case {name}: {self.result[name]}")

    def run_fn(
        self, name: str, fn: Callable[..., Dict[str, Any]], *fn_args, **fn_kwargs
    ):
        """Run a benchmark for a specific function; this is the most general
        benchmark utility available and will work if the other benchmark methods
        are too specific. However, ``fn`` is expected to return a
        `Dict[str, Any]` of metric labels to metric values, which are reported
        at the end of the benchmark. Runtime is automatically calculated and reported,
        but all other metrics of interest must be calculated and returned by ``fn``."""

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
            extra_metrics = {}
            for metric_key, metric_val in fn_output.items():
                if isinstance(metric_key, BenchmarkMetric):
                    curr_case_metrics[metric_key.value] = metric_val
                else:
                    extra_metrics[metric_key] = metric_val
            curr_case_metrics[BenchmarkMetric.EXTRA_METRICS.value] = extra_metrics

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
