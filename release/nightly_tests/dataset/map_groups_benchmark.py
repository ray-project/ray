import ray
import argparse

import pandas as pd

from benchmark import Benchmark

import pyarrow as pa


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=str)
    parser.add_argument("--keys", required=True, nargs="+", type=str)
    parser.add_argument("--batch-format", choices=["pyarrow", "pandas"], required=True)
    return parser.parse_args()


def main(args):
    benchmark = Benchmark("map-groups")

    def benchmark_fn():
        ds = (
            ray.data.read_parquet(args.path)
            .groupby(args.keys)
            .map_groups(normalize_group, batch_format=args.batch_format)
        )
        for _ in ds.iter_internal_ref_bundles():
            pass

    benchmark.run_fn(str(vars(args)), benchmark_fn)
    benchmark.write_result()


def normalize_group(group: pd.DataFrame) -> pd.DataFrame:
    assert isinstance(
        group, (pd.DataFrame, pa.Table)
    ), f"Invalid group type: {type(group)}"

    if isinstance(group, pd.DataFrame):
        return _normalize_dataframe(group)
    elif isinstance(group, pa.Table):
        return _normalize_table(group)


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    return (df - df.mean()) / df.std()


def _normalize_table(table: pa.Table) -> pa.Table:
    return ...


if __name__ == "__main__":
    args = parse_args()
    main(args)
