import ray
import argparse
from typing import Callable

import pandas as pd

from benchmark import Benchmark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=str)
    parser.add_argument("--keys", required=True, nargs="+")

    consume_group = parser.add_mutually_exclusive_group(required=True)
    consume_group.add_argument("--aggregate", choices=["count", "mean", "std"])
    consume_group.add_argument("--map-groups", action="store_true")

    return parser.parse_args()


def main(args):
    benchmark = Benchmark("groupby")
    consume_fn = get_consume_fn(args)

    def benchmark_fn():
        ds = ray.data.read_parquet(args.path).groupby(args.keys)
        consume_fn(ds)

    benchmark.run_fn(str(vars(args)), benchmark_fn)
    benchmark.write_result()


def get_consume_fn(
    args: argparse.Namespace,
) -> Callable[["ray.data.grouped_data.GroupedData"], None]:
    if args.aggregate:

        def consume_fn(ds):
            if args.aggregate == "count":
                aggregate_ds = ds.count()
            elif args.aggregate == "mean":
                aggregate_ds = ds.mean()
            elif args.aggregate == "std":
                aggregate_ds = ds.std()
            else:
                assert False, f"Invalid aggregate argument: {args.aggregate}"

            aggregate_ds.materialize()

    elif args.map_groups:

        def consume_fn(ds):
            def normalize_group(group: pd.DataFrame) -> pd.DataFrame:
                return (group - group.mean()) / group.std()

            # Users typically use the 'pandas' batch format with 'map_groups'.
            ds.map_groups(normalize_group, batch_format="pandas")

            for _ in ds.iter_internal_ref_bundles():
                pass

    else:
        assert False, f"Invalid consume argument: {args}"

    return consume_fn


if __name__ == "__main__":
    args = parse_args()
    main(args)
