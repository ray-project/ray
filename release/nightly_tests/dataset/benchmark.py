import gc
import json
import os
import time
from typing import Callable, Dict, Union

from ray.data.dataset import Dataset
from typing import Any


from enum import Enum

import pyarrow as pa
import pandas as pd


class BenchmarkMetric(Enum):
    RUNTIME = "time"
    NUM_ROWS = "num_rows"
    THROUGHPUT = "tput"
    ACCURACY = "accuracy"


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
        num_rows = output_ds.count()
        self.result[name] = {
            BenchmarkMetric.RUNTIME.value: duration,
            BenchmarkMetric.NUM_ROWS.value: num_rows,
            BenchmarkMetric.THROUGHPUT.value: num_rows / duration,
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
            elif isinstance(batch, (pa.Table, pd.DataFrame)):
                batch_size = len(batch)
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
            BenchmarkMetric.NUM_ROWS.value: record_count,
            BenchmarkMetric.THROUGHPUT.value: record_count / duration,
        }
        print(f"Result of case {name}: {self.result[name]}")

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
        fn_output = fn(*fn_args, **fn_kwargs)
        assert fn_output is None or isinstance(fn_output, dict), fn_output
        duration = time.perf_counter() - start_time

        curr_case_metrics = {
            BenchmarkMetric.RUNTIME.value: duration,
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
