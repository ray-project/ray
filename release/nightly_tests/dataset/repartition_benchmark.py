"""Benchmark: Actorless Hash Shuffle vs Actor-based Hash Shuffle repartition.

Loads TPC-H lineitem table and runs key-based repartition with both strategies,
reporting wall-clock time and throughput.

Usage:
    python repartition_benchmark.py --sf 100 --num-partitions 50
    python repartition_benchmark.py --sf 100 --shuffle-strategy actorless_hash_shuffle
"""

import argparse

import ray
from benchmark import Benchmark
from ray.data import DataContext
from ray.data.context import ShuffleStrategy


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Repartition Benchmark")
    parser.add_argument(
        "--sf",
        choices=["1", "10", "100", "1000"],
        type=str,
        default="100",
        help="TPC-H scale factor (1 = 1GB)",
    )
    parser.add_argument(
        "--num-partitions",
        type=int,
        default=50,
        help="Number of output partitions",
    )
    parser.add_argument(
        "--key-columns",
        nargs="+",
        type=str,
        default=["column00"],
        help="Key columns for hash partitioning (column00 = l_orderkey)",
    )
    parser.add_argument(
        "--shuffle-strategy",
        required=False,
        nargs="?",
        type=str,
        default=None,
        help="Shuffle strategy to benchmark. If not set, benchmarks all hash strategies.",
    )
    return parser.parse_args()


def main(args):
    benchmark = Benchmark()

    path = f"s3://ray-benchmark-data/tpch/parquet/sf{args.sf}/lineitem"

    if args.shuffle_strategy:
        strategies = [ShuffleStrategy(args.shuffle_strategy)]
    else:
        strategies = [
            ShuffleStrategy.HASH_SHUFFLE,
            ShuffleStrategy.ACTORLESS_HASH_SHUFFLE,
        ]

    for strategy in strategies:

        def benchmark_fn(strategy=strategy):
            DataContext.get_current().shuffle_strategy = strategy
            ds = ray.data.read_parquet(path)
            ds = ds.repartition(
                args.num_partitions, keys=args.key_columns
            ).materialize()

            num_rows = ds.count()
            return {
                "num_rows": num_rows,
                "sf": args.sf,
                "num_partitions": args.num_partitions,
                "key_columns": args.key_columns,
                "shuffle_strategy": strategy.value,
            }

        benchmark.run_fn(f"repartition_{strategy.value}", benchmark_fn)

    benchmark.write_result()


if __name__ == "__main__":
    args = parse_args()
    main(args)
