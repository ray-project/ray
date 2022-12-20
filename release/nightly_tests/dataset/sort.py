import json
import os
import resource
import time
from typing import List
import traceback

import numpy as np
import psutil

import ray
from ray._private.internal_api import memory_summary
from ray.data._internal.arrow_block import ArrowRow
from ray.data._internal.util import _check_pyarrow_version
from ray.data.block import Block, BlockMetadata
from ray.data.context import DatasetContext
from ray.data.datasource import Datasource, ReadTask


class RandomIntRowDatasource(Datasource[ArrowRow]):
    """An example datasource that generates rows with random int64 columns.

    Examples:
        >>> source = RandomIntRowDatasource()
        >>> ray.data.read_datasource(source, n=10, num_columns=2).take()
        ... {'c_0': 1717767200176864416, 'c_1': 999657309586757214}
        ... {'c_0': 4983608804013926748, 'c_1': 1160140066899844087}
    """

    def prepare_read(
        self, parallelism: int, n: int, num_columns: int
    ) -> List[ReadTask]:
        _check_pyarrow_version()
        import pyarrow

        read_tasks: List[ReadTask] = []
        block_size = max(1, n // parallelism)

        def make_block(count: int, num_columns: int) -> Block:
            return pyarrow.Table.from_arrays(
                np.random.randint(
                    np.iinfo(np.int64).max, size=(num_columns, count), dtype=np.int64
                ),
                names=[f"c_{i}" for i in range(num_columns)],
            )

        schema = pyarrow.Table.from_pydict(
            {f"c_{i}": [0] for i in range(num_columns)}
        ).schema

        i = 0
        while i < n:
            count = min(block_size, n - i)
            meta = BlockMetadata(
                num_rows=count,
                size_bytes=8 * count * num_columns,
                schema=schema,
                input_files=None,
                exec_stats=None,
            )
            read_tasks.append(
                ReadTask(
                    lambda count=count, num_columns=num_columns: [
                        make_block(count, num_columns)
                    ],
                    meta,
                )
            )
            i += block_size

        return read_tasks


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num-partitions", help="number of partitions", default="50", type=str
    )
    parser.add_argument(
        "--partition-size",
        help="partition size (bytes)",
        default="200e6",
        type=str,
    )
    parser.add_argument(
        "--shuffle", help="shuffle instead of sort", action="store_true"
    )
    parser.add_argument("--use-polars", action="store_true")

    args = parser.parse_args()

    if args.use_polars and not args.shuffle:
        print("Using polars for sort")
        ctx = DatasetContext.get_current()
        ctx.use_polars = True

    num_partitions = int(args.num_partitions)
    partition_size = int(float(args.partition_size))
    print(
        f"Dataset size: {num_partitions} partitions, "
        f"{partition_size / 1e9}GB partition size, "
        f"{num_partitions * partition_size / 1e9}GB total"
    )
    start_time = time.time()
    source = RandomIntRowDatasource()
    num_rows_per_partition = partition_size // 8
    ds = ray.data.read_datasource(
        source,
        parallelism=num_partitions,
        n=num_rows_per_partition * num_partitions,
        num_columns=1,
    )
    exc = None
    try:
        if args.shuffle:
            ds = ds.random_shuffle()
        else:
            ds = ds.sort(key="c_0")
    except Exception as e:
        exc = e
        pass

    end_time = time.time()

    duration = end_time - start_time
    print("Finished in", duration)
    print("")

    print("==== Driver memory summary ====")
    maxrss = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1e3)
    print(f"max: {maxrss / 1e9}/GB")
    process = psutil.Process(os.getpid())
    rss = int(process.memory_info().rss)
    print(f"rss: {rss / 1e9}/GB")

    try:
        print(memory_summary(stats_only=True))
    except Exception:
        print("Failed to retrieve memory summary")
        print(traceback.format_exc())
    print("")

    print(ds.stats())

    if "TEST_OUTPUT_JSON" in os.environ:
        out_file = open(os.environ["TEST_OUTPUT_JSON"], "w")
        results = {
            "time": duration,
            "success": "1" if exc is None else "0",
            "num_partitions": num_partitions,
            "partition_size": partition_size,
            "perf_metrics": [
                {
                    "perf_metric_name": "peak_driver_memory",
                    "perf_metric_value": maxrss,
                    "perf_metric_type": "MEMORY",
                },
                {
                    "perf_metric_name": "runtime",
                    "perf_metric_value": duration,
                    "perf_metric_type": "LATENCY",
                },
            ],
        }
        json.dump(results, out_file)

    if exc:
        raise exc
