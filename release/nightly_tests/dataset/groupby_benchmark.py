import argparse

import pyarrow as pa
from pyarrow import types
import pyarrow.compute as pc
import ray

from benchmark import Benchmark
from ray.data import DataContext
from ray.data.context import ShuffleStrategy


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sf",
        choices=["1", "10", "100", "1000", "10000"],
        type=str,
        help="The scale factor of the TPCH dataset. 1 is 1GB.",
        default="1",
    )
    parser.add_argument(
        "--group-by",
        required=True,
        nargs="+",
        type=str,
        help="Which columns to group by",
    )
    parser.add_argument(
        "--shuffle-strategy",
        required=False,
        default=ShuffleStrategy.SORT_SHUFFLE_PULL_BASED,
        nargs="?",
        type=str,
        help="Strategy to use when shuffling data (see ShuffleStrategy for accepted values)",
    )

    consume_group = parser.add_mutually_exclusive_group()
    consume_group.add_argument("--aggregate", action="store_true")
    consume_group.add_argument("--map-groups", action="store_true")

    return parser.parse_args()


def main(args):
    benchmark = Benchmark()
    consume_fn = get_consume_fn(args)

    def benchmark_fn():
        path = f"s3://ray-benchmark-data/tpch/parquet/sf{args.sf}/lineitem"

        # Configure appropriate shuffle-strategy
        DataContext.get_current().shuffle_strategy = ShuffleStrategy(
            args.shuffle_strategy
        )

        grouped_ds = ray.data.read_parquet(path).groupby(args.group_by)
        consume_fn(grouped_ds)

        # Report arguments for the benchmark.
        return vars(args)

    benchmark.run_fn("main", benchmark_fn)
    benchmark.write_result()


def get_consume_fn(args: argparse.Namespace):
    if args.aggregate:

        def consume_fn(grouped_ds):
            # 'column05' is 'l_extendedprice'
            grouped_ds.mean("column05").materialize()

    elif args.map_groups:

        def consume_fn(grouped_ds):
            ds = grouped_ds.map_groups(normalize_table, batch_format="pyarrow")
            for _ in ds.iter_internal_ref_bundles():
                pass

    else:
        assert False, f"Invalid consume argument: {args}"

    return consume_fn


def normalize_table(table: pa.Table) -> pa.Table:
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
