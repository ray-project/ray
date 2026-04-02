"""Benchmark for measuring actor scaling overhead with 1000 actors.

Measures how long it takes to spin up actors, process data, and tear down
across a range(N) -> map_batches(1000 actors) -> consume pipeline.
"""

import argparse

import ray
from benchmark import Benchmark, OperatorStatsTracker

BLOCKS_PER_WORKER: int = 10
TARGET_BLOCK_SIZE_BYTES: int = 128 * 1024 * 1024  # 128 MiB
BYTES_PER_ROW: int = 8  # ray.data.range produces one int64 per row
ROWS_PER_BLOCK: int = TARGET_BLOCK_SIZE_BYTES // BYTES_PER_ROW


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num-workers",
        type=int,
        required=True,
        help="Number of actors/tasks to use for map_batches.",
    )
    parser.add_argument(
        "--worker-type",
        type=str,
        choices=["actors", "tasks"],
        default="actors",
        help="Whether to use actors or regular tasks for map_batches.",
    )
    return parser.parse_args()


class NoOpUDF:
    def __call__(self, batch):
        return batch


def no_op_udf(batch):
    return batch


def main(args: argparse.Namespace):
    benchmark = Benchmark()

    ctx = ray.data.DataContext.get_current()
    ctx.custom_execution_callback_classes.append(OperatorStatsTracker)

    def benchmark_fn():
        num_blocks = BLOCKS_PER_WORKER * args.num_workers
        num_rows = num_blocks * ROWS_PER_BLOCK
        ds = ray.data.range(num_rows, override_num_blocks=num_blocks)

        if args.worker_type == "actors":
            ds = ds.map_batches(
                NoOpUDF,
                num_cpus=1,
                compute=ray.data.ActorPoolStrategy(size=args.num_workers),
            )
        else:
            ds = ds.map_batches(
                no_op_udf,
                num_cpus=1,
                compute=ray.data.TaskPoolStrategy(size=args.num_workers),
            )

        total_rows = 0
        for bundle in ds.iter_internal_ref_bundles():
            total_rows += bundle.num_rows()
        metrics = OperatorStatsTracker.collect()
        assert total_rows == num_rows
        metrics["num_blocks"] = num_blocks
        metrics["num_rows"] = num_rows
        return metrics

    benchmark.run_fn("worker_scaling", benchmark_fn)
    benchmark.write_result()


if __name__ == "__main__":
    ray.init()
    args = parse_args()
    main(args)
