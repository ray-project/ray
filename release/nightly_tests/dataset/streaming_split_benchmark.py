import argparse
from typing import Optional

from benchmark import Benchmark
import ray


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-workers", type=int, required=True)
    parser.add_argument(
        "--early-stop",
        action="store_true",
        help="If set, each worker will read only half of the data",
    )
    return parser.parse_args()


def main(args):
    """Benchmark for `Dataset.streaming_split`.

    This benchmark splits ImageNet into equally-sized shards and consumes them on
    `num_workers` actors in parallel.

    Ray Train uses the same functionality to load data across training workers.
    """
    benchmark = Benchmark()

    ds = ray.data.read_parquet(
        "s3://ray-benchmark-data-internal-us-west-2/imagenet/parquet"
    )

    num_rows = ds.count()
    if args.early_stop is not None:
        max_rows_to_read_per_worker = num_rows // 2 // args.num_workers
    else:
        max_rows_to_read_per_worker = None

    consumers = [
        ConsumingActor.options(scheduling_strategy="SPREAD").remote()
        for _ in range(args.num_workers)
    ]
    locality_hints = ray.get([actor.get_location.remote() for actor in consumers])

    def benchmark_fn():
        splits = ds.streaming_split(
            args.num_workers, equal=True, locality_hints=locality_hints
        )
        future = [
            consumers[i].consume.remote(split, max_rows_to_read_per_worker)
            for i, split in enumerate(splits)
        ]
        ray.get(future)

        # Report arguments for the benchmark.
        return vars(args)

    benchmark.run_fn("main", benchmark_fn)
    benchmark.write_result()


@ray.remote
class ConsumingActor:
    def consume(self, split, max_rows_to_read: Optional[int] = None):
        rows_read = 0
        for _ in split.iter_batches():
            if max_rows_to_read is not None:
                if rows_read >= max_rows_to_read:
                    break

    def get_location(self):
        return ray.get_runtime_context().get_node_id()


if __name__ == "__main__":
    args = parse_args()
    main(args)
