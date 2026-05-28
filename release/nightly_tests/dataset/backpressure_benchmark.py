import argparse
import functools
import time

import numpy as np
import pyarrow as pa
import ray

from benchmark import Benchmark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backpressure benchmark")
    parser.add_argument(
        "--case",
        choices=["fast-producer-slow-consumer", "training-prefetch"],
        required=True,
    )
    parser.add_argument("--num-input-blocks", type=int, default=128)
    parser.add_argument("--output-batches-per-input-batch", type=int, default=8)
    parser.add_argument("--output-batch-rows", type=int, default=128)
    parser.add_argument("--output-row-bytes", type=int, default=1024**2)
    parser.add_argument("--consumer-sleep-s", type=float, default=1.0)
    parser.add_argument("--num-trainers", type=int, default=8)
    parser.add_argument("--prefetch-batches", type=int, default=8)
    return parser.parse_args()


def make_inputs(num_input_blocks: int):
    return [
        pa.Table.from_pydict({"id": [input_id]}) for input_id in range(num_input_blocks)
    ]


def produce(
    batch,
    *,
    output_batches_per_input_batch: int,
    output_batch_rows: int,
    output_row_bytes: int,
):
    for _ in range(output_batches_per_input_batch):
        yield {
            "data": np.zeros((output_batch_rows, output_row_bytes), dtype=np.uint8),
        }


def consume_slow(batch, *, sleep_s: float):
    time.sleep(sleep_s)
    return {"status": ["ok"]}


def run_fast_producer_slow_consumer(args: argparse.Namespace):
    producer = functools.partial(
        produce,
        output_batches_per_input_batch=args.output_batches_per_input_batch,
        output_batch_rows=args.output_batch_rows,
        output_row_bytes=args.output_row_bytes,
    )
    consumer = functools.partial(consume_slow, sleep_s=args.consumer_sleep_s)

    ds = (
        ray.data.from_blocks(make_inputs(args.num_input_blocks))
        .map_batches(producer)
        .map_batches(consumer, compute=ray.data.TaskPoolStrategy(size=1))
    )
    for _ in ds.iter_internal_ref_bundles():
        pass

    return vars(args)


def run_training_prefetch(args: argparse.Namespace):
    producer = functools.partial(
        produce,
        output_batches_per_input_batch=args.output_batches_per_input_batch,
        output_batch_rows=args.output_batch_rows,
        output_row_bytes=args.output_row_bytes,
    )

    iterators = (
        ray.data.from_blocks(make_inputs(args.num_input_blocks))
        .map_batches(producer)
        .streaming_split(args.num_trainers, equal=True)
    )

    trainers = [
        Trainer.options(scheduling_strategy="SPREAD").remote(
            consumer_sleep_s=args.consumer_sleep_s,
            prefetch_batches=args.prefetch_batches,
        )
        for _ in range(args.num_trainers)
    ]
    ray.get(
        [
            trainers[i].train.remote(iterators[i], batch_size=args.output_batch_rows)
            for i in range(args.num_trainers)
        ]
    )

    return vars(args)


@ray.remote(num_cpus=1)
class Trainer:
    def __init__(self, consumer_sleep_s: float, prefetch_batches: int):
        self._consumer_sleep_s = consumer_sleep_s
        self._prefetch_batches = prefetch_batches

    def train(self, data_iterator, batch_size: int):
        for _ in data_iterator.iter_batches(
            batch_size=batch_size,
            prefetch_batches=self._prefetch_batches,
        ):
            time.sleep(self._consumer_sleep_s)


def main(args: argparse.Namespace):
    benchmark = Benchmark()

    if args.case == "fast-producer-slow-consumer":
        benchmark.run_fn(args.case, run_fast_producer_slow_consumer, args)
    elif args.case == "training-prefetch":
        benchmark.run_fn(args.case, run_training_prefetch, args)
    else:
        raise ValueError(f"Unexpected benchmark case: {args.case}")

    benchmark.write_result()


if __name__ == "__main__":
    main(parse_args())
