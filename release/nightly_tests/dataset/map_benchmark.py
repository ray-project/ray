import argparse

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
    parser.add_argument("--batch-format", choices=["numpy", "pandas", "pyarrow"])
    parser.add_argument("--compute", choices=["tasks", "actors"])
    return parser.parse_args()


def main(args: argparse.Namespace) -> None:
    benchmark = Benchmark("map")
    path = f"s3://ray-benchmark-data/tpch/parquet/sf{args.sf}/lineitem"

    def benchmark_fn():
        # Load the dataset.
        ds = ray.data.read_parquet(path)

        # Apply the map transformation.
        if args.api == "map":
            ds = ds.map(increment_row)
        elif args.api == "map_batches":
            if not args.compute or args.compute == "tasks":
                ds = ds.map_batches(increment_batch, batch_format=args.batch_format)
            else:
                ds = ds.map_batches(
                    IncrementBatch,
                    batch_format=args.batch_format,
                    concurrency=(1, 1024),
                )
        elif args.api == "flat_map":
            ds = ds.flat_map(flat_increment_row)

        # Iterate over the results.
        for _ in ds.iter_internal_ref_bundles():
            pass

    benchmark.run_fn(str(vars(args)), benchmark_fn)
    benchmark.write_result()


def increment_row(row):
    row["column00"] += 1
    return row


def flat_increment_row(row):
    row["column00"] += 1
    return [row]


def increment_batch(batch):
    if isinstance(batch, (dict, pd.DataFrame)):
        # Avoid modifying the column in-place (i.e., +=) because it might be read-only.
        # See https://github.com/ray-project/ray/issues/369.
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
    def __call__(self, batch):
        return increment_batch(batch)


if __name__ == "__main__":
    args = parse_args()
    main(args)
