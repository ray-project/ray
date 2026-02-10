import argparse

import functools
import time
import numpy
import pyarrow as pa
import pyarrow.compute as pc
import pandas as pd
import ray

from benchmark import Benchmark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Map benchmark")
    parser.add_argument(
        "--api", choices=["map", "map_batches", "flat_map"], required=True
    )
    parser.add_argument(
        "--sf", choices=["1", "10", "100", "1000", "10000"], default="1"
    )
    parser.add_argument(
        "--batch-format",
        choices=["numpy", "pandas", "pyarrow"],
        help=(
            "Batch format to use with 'map_batches'. This argument is ignored for "
            "'map' and 'flat_map'.",
        ),
    )
    parser.add_argument(
        "--compute",
        choices=["tasks", "actors"],
        help=(
            "Compute strategy to use with 'map_batches'. This argument is ignored for "
            "'map' and 'flat_map'.",
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10_000,
        help="Batch size to use with 'map_batches'.",
    )
    parser.add_argument(
        "--map-batches-sleep-ms",
        type=int,
        default=50,
        help=(
            "Sleep time in milliseconds for each map_batches call. This is useful to "
            "simulate complex computation."
        ),
    )
    parser.add_argument(
        "--repeat-inputs",
        type=int,
        default=1,
        help=(
            "Number of times to repeat the input data. This is useful to make the "
            "job run longer."
        ),
    )
    parser.add_argument(
        "--repeat-map-batches",
        choices=["once", "repeat"],
        default="once",
        help=(
            "Whether to repeat map_batches. If 'once', the map_batches will run once. "
            "If 'repeat', the map_batches will run twice, with the second run using the "
            "output of the first run as input."
        ),
    )
    parser.add_argument(
        "--concurrency",
        default=[1, 1024],
        nargs=2,
        type=int,
        help="Concurrency to use with 'map_batches'.",
    )
    return parser.parse_args()


MODEL_SIZE = 1024**3


def main(args: argparse.Namespace) -> None:
    benchmark = Benchmark()
    path = f"s3://ray-benchmark-data/tpch/parquet/sf{args.sf}/lineitem"
    path = [path] * args.repeat_inputs

    def apply_map_batches(ds):
        use_actors = args.compute == "actors"
        if not use_actors:
            return ds.map_batches(
                functools.partial(
                    increment_batch,
                    map_batches_sleep_ms=args.map_batches_sleep_ms,
                ),
                batch_format=args.batch_format,
                batch_size=args.batch_size,
            )
        else:
            # Simulate the use case where a model is passed to the
            # actors as an object ref.
            dummy_model = numpy.zeros(MODEL_SIZE, dtype=numpy.int8)
            model_ref = ray.put(dummy_model)
            return ds.map_batches(
                IncrementBatch,
                fn_constructor_args=[model_ref, args.map_batches_sleep_ms],
                batch_format=args.batch_format,
                batch_size=args.batch_size,
                concurrency=tuple(args.concurrency),
            )

    def benchmark_fn():
        # Load the dataset.
        ds = ray.data.read_parquet(path)

        # Apply the map transformation.
        if args.api == "map":
            ds = ds.map(increment_row)
        elif args.api == "map_batches":
            ds = apply_map_batches(ds)
            if args.repeat_map_batches == "repeat":
                ds = apply_map_batches(ds)
        elif args.api == "flat_map":
            ds = ds.flat_map(flat_increment_row)

        def dummy_write(batch):
            return {"num_rows": [len(batch["column00"])]}

        ds = ds.map_batches(dummy_write)

        for _ in ds.iter_internal_ref_bundles():
            pass

        # Report arguments for the benchmark.
        return vars(args)

    benchmark.run_fn("main", benchmark_fn)
    benchmark.write_result()


def increment_row(row):
    row["column00"] += 1
    return row


def flat_increment_row(row):
    row["column00"] += 1
    return [row]


def increment_batch(batch, map_batches_sleep_ms=0):
    if map_batches_sleep_ms > 0:
        time.sleep(map_batches_sleep_ms / 1000.0)

    if isinstance(batch, (dict, pd.DataFrame)):
        # Avoid modifying the column in-place (i.e., +=) because NumPy arrays are
        # read-only. See https://github.com/ray-project/ray/issues/369.
        batch["column00"] = batch["column00"] + 1
    elif isinstance(batch, pa.Table):
        column00_incremented = pc.add(batch["column00"], 1)
        batch = batch.set_column(
            batch.column_names.index("column00"), "column00", column00_incremented
        )
    else:
        assert False, f"Invalid batch format: {type(batch)}"
    return batch


class IncrementBatch:
    def __init__(self, model_ref, map_batches_sleep_ms=0):
        self.model = ray.get(model_ref)
        self.map_batches_sleep_ms = map_batches_sleep_ms

    def __call__(self, batch):
        return increment_batch(batch, self.map_batches_sleep_ms)


if __name__ == "__main__":
    args = parse_args()
    main(args)
