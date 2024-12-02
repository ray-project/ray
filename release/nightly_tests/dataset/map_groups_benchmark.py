import argparse

import pandas as pd
import pyarrow as pa
from pyarrow import types
import pyarrow.compute as pc
import ray

from benchmark import Benchmark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=str)
    parser.add_argument("--group-by", required=True, nargs="+", type=str)
    parser.add_argument("--batch-format", choices=["pyarrow", "pandas"], required=True)
    return parser.parse_args()


def main(args):
    benchmark = Benchmark("map-groups")

    def benchmark_fn():
        ds = (
            ray.data.read_parquet(args.path)
            .groupby(args.group_by)
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
    normalized_columns = []
    for column_name in df.columns:
        column = df[column_name]
        if column.dtype.kind != "f":
            normalized_columns.append(column)
            continue

        normalized_column = (column - column.mean()) / column.std()
        normalized_columns.append(normalized_column)

    return pd.DataFrame(normalized_columns)


def _normalize_table(table: pa.Table) -> pa.Table:
    normalized_columns = []
    for column_name in table.column_names:
        column = table[column_name]
        if not types.is_floating(column.type):
            normalized_columns.append(column)
            continue

        normalized_column = pc.divide(
            pc.subtract(column, pc.mean(column)), pc.stddev(column)
        )
        normalized_columns.append(normalized_column)

    return pa.Table.from_arrays(normalized_columns, schema=table.schema)


if __name__ == "__main__":
    args = parse_args()
    main(args)
