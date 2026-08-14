import argparse

import pyarrow as pa
from pyarrow import types
import pyarrow.compute as pc
import ray

from benchmark import Benchmark, collect_operator_metrics, consume_ref_bundles
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
        # TODO: Don't override once we fix range-based shuffle
        override_num_blocks = (
            100
            if args.shuffle_strategy == ShuffleStrategy.SORT_SHUFFLE_PULL_BASED.value
            else None
        )
        grouped_ds = ray.data.read_parquet(
            path, override_num_blocks=override_num_blocks
        ).groupby(args.group_by)
        consumed_ds = consume_fn(grouped_ds)

        # Arguments, plus per-operator wall time / output bytes / per-task USS+RSS so a
        # regression can be attributed to the read, the shuffle or the aggregation
        # rather than to the job as a whole.
        return {**vars(args), **collect_operator_metrics(consumed_ds)}

    benchmark.run_fn("main", benchmark_fn)
    benchmark.write_result()


def get_consume_fn(args: argparse.Namespace):
    # Each consume_fn returns the *consumed* dataset handle: execution stats attach to
    # the handle that was consumed, so that is what collect_operator_metrics needs.
    if args.aggregate:

        def consume_fn(grouped_ds):
            # 'column05' is 'l_extendedprice'
            return grouped_ds.mean("column05").materialize()

    elif args.map_groups:

        def consume_fn(grouped_ds):
            ds = grouped_ds.map_groups(normalize_table, batch_format="pyarrow")
            consume_ref_bundles(ds)
            return ds

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
