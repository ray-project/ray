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
        choices=[
            "fast-producer-slow-consumer",
            "many-tiny-objects",
            "training-prefetch",
            "training-prefetch-single-node",
        ],
        required=True,
    )
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


def run_fast_producer_slow_consumer():
    """Benchmark backpressure from a fast producer and a slow consumer.

    Produces 1,024 batches of 128 MiB each, for 128 GiB of logical output
    data. A single consumer sleeps for 1 second per batch, creating sustained
    backpressure on the producer.
    """
    num_input_blocks = 128
    output_batches_per_input_batch = 8
    output_batch_rows = 128
    output_row_bytes = 1024**2
    consumer_sleep_s = 1.0

    producer = functools.partial(
        produce,
        output_batches_per_input_batch=output_batches_per_input_batch,
        output_batch_rows=output_batch_rows,
        output_row_bytes=output_row_bytes,
    )
    consumer = functools.partial(consume_slow, sleep_s=consumer_sleep_s)

    ds = (
        ray.data.from_blocks(make_inputs(num_input_blocks))
        .map_batches(producer)
        .map_batches(consumer, compute=ray.data.TaskPoolStrategy(size=1))
    )
    for _ in ds.iter_internal_ref_bundles():
        pass


def run_many_tiny_objects():
    """Benchmark backpressure from many small task outputs.

    Produces 100,000 outputs of about 50 KiB each, or about 4.8 GiB of logical
    output data. The payload size targets Ray's small-object direct-call path,
    while a single consumer sleeps for 10 ms per batch, creating pressure from
    many queued outputs.
    """
    num_input_blocks = 100_000
    output_batches_per_input_batch = 1
    output_batch_rows = 1
    # Stay below Ray's 100 KiB direct-call limit so outputs are sent inline.
    output_row_bytes = 50 * 1024
    consumer_sleep_s = 0.01

    producer = functools.partial(
        produce,
        output_batches_per_input_batch=output_batches_per_input_batch,
        output_batch_rows=output_batch_rows,
        output_row_bytes=output_row_bytes,
    )
    consumer = functools.partial(consume_slow, sleep_s=consumer_sleep_s)

    ds = (
        ray.data.from_blocks(make_inputs(num_input_blocks))
        .map_batches(producer)
        .map_batches(consumer, compute=ray.data.TaskPoolStrategy(size=1))
    )
    for _ in ds.iter_internal_ref_bundles():
        pass


def run_training_prefetch(*, num_trainers: int):
    """Benchmark backpressure from training consumers that prefetch data.

    Produces 1,024 batches of 128 MiB each, for 128 GiB of logical output data,
    then splits them evenly across ``num_trainers`` trainers.

    Each trainer prefetches 8 batches, corresponding to about 1 GiB of data
    per trainer, and sleeps for 1 second after consuming each batch.
    """
    num_input_blocks = 128
    output_batches_per_input_batch = 8
    output_batch_rows = 128
    output_row_bytes = 1024**2
    consumer_sleep_s = 1.0
    prefetch_batches = 8

    producer = functools.partial(
        produce,
        output_batches_per_input_batch=output_batches_per_input_batch,
        output_batch_rows=output_batch_rows,
        output_row_bytes=output_row_bytes,
    )

    trainers = [
        Trainer.options(scheduling_strategy="SPREAD").remote(
            consumer_sleep_s=consumer_sleep_s,
            prefetch_batches=prefetch_batches,
        )
        for _ in range(num_trainers)
    ]

    trainer_node_ids = ray.get([trainer.get_node_id.remote() for trainer in trainers])

    iterators = (
        ray.data.from_blocks(make_inputs(num_input_blocks))
        .map_batches(producer)
        .streaming_split(
            num_trainers,
            equal=True,
            locality_hints=trainer_node_ids,
        )
    )

    ray.get(
        [
            trainers[i].train.remote(iterators[i], batch_size=output_batch_rows)
            for i in range(num_trainers)
        ]
    )


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

    def get_node_id(self) -> str:
        return ray.get_runtime_context().get_node_id()


def main(args: argparse.Namespace):
    benchmark = Benchmark()

    if args.case == "fast-producer-slow-consumer":
        benchmark.run_fn(args.case, run_fast_producer_slow_consumer)
    elif args.case == "many-tiny-objects":
        benchmark.run_fn(args.case, run_many_tiny_objects)
    elif args.case == "training-prefetch":
        benchmark.run_fn(args.case, run_training_prefetch, num_trainers=8)
    elif args.case == "training-prefetch-single-node":
        benchmark.run_fn(args.case, run_training_prefetch, num_trainers=1)
    else:
        raise ValueError(f"Unexpected benchmark case: {args.case}")

    benchmark.write_result()


if __name__ == "__main__":
    main(parse_args())
