import argparse
import os
import uuid

import ray

from benchmark import Benchmark
from ray.data import DataContext
from ray.data.context import ShuffleStrategy
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

DEFAULT_OUTPUT_DIR = f"/mnt/local_storage/repartition_benchmark_{uuid.uuid4().hex}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sf",
        choices=["1", "10", "100", "1000", "10000"],
        type=str,
        help="The scale factor of the TPCH dataset. 1 is 1GB.",
        default="1",
    )
    parser.add_argument(
        "--keys",
        required=True,
        nargs="+",
        type=str,
        help="Which columns to hash-partition by",
    )
    parser.add_argument(
        "--num-partitions",
        required=True,
        type=int,
        help="Number of output partitions to repartition into",
    )
    parser.add_argument(
        "--shuffle-strategy",
        required=False,
        default=ShuffleStrategy.SHUFFLE_V2.value,
        nargs="?",
        type=str,
        help="Strategy to use when shuffling data (see ShuffleStrategy for accepted values)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=DEFAULT_OUTPUT_DIR,
        help="Local directory to write Parquet output to on each node.",
    )
    return parser.parse_args()


def _create_output_dir_on_all_nodes(path: str) -> None:
    """Create ``path`` on every alive node.

    Write tasks run on arbitrary nodes and pyarrow's local filesystem doesn't
    create missing parent directories, so each node needs the directory before
    the distributed write starts (`FileDatasink.on_write_start` only creates
    it on the driver's node).
    """

    @ray.remote(num_cpus=0)
    def _mkdir() -> None:
        os.makedirs(path, exist_ok=True)

    ray.get(
        [
            _mkdir.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node["NodeID"], soft=False
                )
            ).remote()
            for node in ray.nodes()
            if node["Alive"]
        ]
    )


def main(args):
    # Connect up front: _create_output_dir_on_all_nodes needs ray.nodes()
    # before any other Ray call would auto-init.
    ray.init()

    # Don't gate on scheduling-loop duration until this SF10000-scale test has
    # an established baseline.
    benchmark = Benchmark(max_sched_loop_duration_s=None)

    def benchmark_fn():
        path = f"s3://ray-benchmark-data/tpch/parquet/sf{args.sf}/lineitem"

        # Configure appropriate shuffle-strategy
        DataContext.get_current().shuffle_strategy = ShuffleStrategy(
            args.shuffle_strategy
        )

        _create_output_dir_on_all_nodes(args.output_dir)

        ds = ray.data.read_parquet(path)
        ds = ds.repartition(args.num_partitions, keys=args.keys)
        ds.write_parquet(args.output_dir)

        # Report arguments for the benchmark.
        return vars(args)

    benchmark.run_fn("main", benchmark_fn)
    benchmark.write_result()


if __name__ == "__main__":
    args = parse_args()
    main(args)
