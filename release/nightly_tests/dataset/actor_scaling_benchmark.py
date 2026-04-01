"""Benchmark for measuring actor scaling overhead with 1000 actors.

Measures how long it takes to spin up actors, process data, and tear down
across a range(N) -> map_batches(1000 actors) -> consume pipeline.
"""

import argparse

import ray
from benchmark import Benchmark, OperatorStatsTracker


NUM_ACTORS = 1000
NUM_ROWS = 1_000_000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num-actors",
        type=int,
        default=NUM_ACTORS,
        help="Number of actors to use for map_batches.",
    )
    parser.add_argument(
        "--num-rows",
        type=int,
        default=NUM_ROWS,
        help="Number of rows in the input dataset.",
    )
    return parser.parse_args()


class NoOpUDF:
    def __call__(self, batch):
        return batch


def main(args: argparse.Namespace):
    benchmark = Benchmark()

    ctx = ray.data.DataContext.get_current()
    ctx.custom_execution_callback_classes.append(OperatorStatsTracker)

    def benchmark_fn():
        ds = ray.data.range(args.num_rows).map_batches(
            NoOpUDF,
            concurrency=args.num_actors,
        )

        total_rows = 0
        for bundle in ds.iter_internal_ref_bundles():
            total_rows += bundle.num_rows()

        metrics = OperatorStatsTracker.collect()
        return metrics

    benchmark.run_fn("actor_scaling", benchmark_fn)
    benchmark.write_result()


if __name__ == "__main__":
    ray.init()
    args = parse_args()
    main(args)
