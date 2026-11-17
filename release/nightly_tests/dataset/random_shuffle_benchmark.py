import json
import math
import os
import resource
import time
from typing import Dict, List
import traceback

import numpy as np
import psutil

from benchmark import Benchmark, _query_prometheus
import ray
from ray._private.internal_api import memory_summary
from ray.data._internal.util import _check_pyarrow_version, GiB
from ray.data.block import Block, BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource import Datasource, ReadTask


class RandomIntRowDatasource(Datasource):
    """An example datasource that generates rows with random int64 keys and a
    row of the given byte size.

    Examples:
        >>> source = RandomIntRowDatasource()
        >>> ray.data.read_datasource(source, n=10, row_size_bytes=2).take()
        ... {'c_0': 1717767200176864416, 'c_1': b"..."}
        ... {'c_0': 4983608804013926748, 'c_1': b"..."}
    """

    def prepare_read(
        self, parallelism: int, n: int, row_size_bytes: int
    ) -> List[ReadTask]:
        _check_pyarrow_version()
        import pyarrow

        read_tasks: List[ReadTask] = []
        block_size = max(1, n // parallelism)
        row = np.random.bytes(row_size_bytes)

        schema = pyarrow.schema(
            [
                pyarrow.field("c_0", pyarrow.int64()),
                # NOTE: We use fixed-size binary type to avoid Arrow (list) offsets
                #       overflows when using non-fixed-size data-types (like string,
                #       binary, list, etc) whose size exceeds int32 limit (of 2^31-1)
                pyarrow.field("c_1", pyarrow.binary(row_size_bytes)),
            ]
        )

        def make_block(count: int) -> Block:
            return pyarrow.Table.from_arrays(
                [
                    np.random.randint(
                        np.iinfo(np.int64).max, size=(count,), dtype=np.int64
                    ),
                    [row for _ in range(count)],
                ],
                schema=schema,
            )

        i = 0
        while i < n:
            count = min(block_size, n - i)
            meta = BlockMetadata(
                num_rows=count,
                size_bytes=count * (8 + row_size_bytes),
                input_files=None,
                exec_stats=None,
            )
            read_tasks.append(
                ReadTask(
                    lambda count=count: [make_block(count)],
                    meta,
                    schema=schema,
                )
            )
            i += block_size

        return read_tasks


def _peak_worker_memory_metrics(
    start_unix_time: float, end_unix_time: float
) -> Dict[str, float]:
    """Peak private worker memory per node and peak worker-node memory, from Prometheus.

    Worker memory is ``ray_component_uss_mb`` (RSS minus shared pages), not
    ``ray_component_rss_mb``: a worker's RSS includes the object store pages it has
    mapped, so summing RSS over a node's workers counts that shared memory once per
    worker and can exceed the node's physical memory. USS is the private heap, which
    is where allocator retention shows up. Workers are summed per node at each
    sample before taking the peak, so processes that peaked at different times
    aren't added together.
    """
    session = json.dumps(ray.get_runtime_context().get_session_name())
    window_ms = max(1, math.ceil((end_unix_time - start_unix_time) * 1000))

    def peaks(query: str) -> List[float]:
        series = _query_prometheus(query, end_unix_time) or []
        return [float(s["value"][1]) for s in series]

    # Subquery at Ray's 10 s scrape interval: peak over time of the per-node sum.
    worker_uss_mb = peaks(
        "max_over_time((sum by (ip) (ray_component_uss_mb"
        f'{{Component=~"ray::.*",SessionName={session}}}))[{window_ms}ms:10s])'
    )
    node_mem_bytes = peaks(
        "max_over_time(ray_node_mem_used"
        f'{{RayNodeType="worker",SessionName={session}}}[{window_ms}ms])'
    )

    def max_and_mean(values: List[float], scale: float) -> List[float]:
        if not values:
            return [0.0, 0.0]
        return [max(values) / scale, sum(values) / len(values) / scale]

    uss_max, uss_mean = max_and_mean(worker_uss_mb, 1e3)  # MB -> GB
    mem_max, mem_mean = max_and_mean(node_mem_bytes, 1e9)  # bytes -> GB
    return {
        "peak_worker_uss_per_node_gb_max": uss_max,
        "peak_worker_uss_per_node_gb_mean": uss_mean,
        "peak_worker_node_mem_used_gb_max": mem_max,
        "peak_worker_node_mem_used_gb_mean": mem_mean,
    }


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
    # Use 100-byte records to approximately match Cloudsort benchmark.
    parser.add_argument(
        "--row-size-bytes",
        help="Size of each row in bytes.",
        default=100,
        type=int,
    )
    parser.add_argument("--limit-num-blocks", type=int, default=None)

    args = parser.parse_args()

    if args.limit_num_blocks is not None:
        DataContext.get_current().set_config(
            "debug_limit_shuffle_execution_to_num_blocks", args.limit_num_blocks
        )

    num_partitions = int(args.num_partitions)
    partition_size = int(float(args.partition_size))
    print(
        f"Dataset size: {num_partitions} partitions, "
        f"{partition_size / GiB}GB partition size, "
        f"{num_partitions * partition_size / GiB}GB total"
    )

    # Override target max-block size to avoid creating too many blocks
    DataContext.get_current().target_max_block_size = 1 * GiB
    source = RandomIntRowDatasource()
    # Each row has an int64 key.
    num_rows_per_partition = partition_size // (8 + args.row_size_bytes)

    holder = {}

    def run_benchmark(args):
        ds = ray.data.read_datasource(
            source,
            override_num_blocks=num_partitions,
            n=num_rows_per_partition * num_partitions,
            row_size_bytes=args.row_size_bytes,
        )

        ds = ds.random_shuffle()
        try:
            holder["ds"] = ds.materialize()
        except Exception as e:
            holder["exc"] = e
            holder["ds"] = ds

    # The p90 scheduling loop duration was 100s+ on Ray 2.59. While we investigate the
    # performance, I've temporarily disabled the check.
    #
    # TODO: Re-enable this once `SHUFFLE_V2` supports random shuffles.
    benchmark = Benchmark(max_sched_loop_duration_s=None)
    start_unix_time = time.time()
    benchmark.run_fn("main", run_benchmark, args)
    end_unix_time = time.time()

    ds = holder["ds"]
    ds_stats = ds.stats()

    # TODO(swang): Add stats for OOM worker kills. This is not very
    # convenient to do programmatically right now because it requires
    # querying Prometheus.
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

    if ds_stats is not None:
        print(ds_stats)

    results = {
        "num_partitions": num_partitions,
        "partition_size": partition_size,
        "peak_driver_memory": maxrss,
    }
    try:
        results.update(_peak_worker_memory_metrics(start_unix_time, end_unix_time))
    except Exception:
        print("Failed to query worker memory metrics from Prometheus")
        print(traceback.format_exc())
    benchmark.result["main"].update(results)

    # Wait until after the stats have been printed to raise any exceptions.
    if "exc" in holder:
        print(results)
        raise holder["exc"]

    benchmark.write_result()

    ray.timeline("dump.json")
