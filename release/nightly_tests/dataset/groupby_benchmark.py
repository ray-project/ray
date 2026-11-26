import argparse
import json
import math
import time

import pyarrow as pa
from pyarrow import types
import pyarrow.compute as pc
import ray

from benchmark import Benchmark, _query_prometheus, benchmark_job_config
from ray.data import DataContext
from ray.data.aggregate import (
    AbsMax,
    AggregateFnV2,
    AsList,
    Mean,
    Quantile,
    Std,
    TopKUnique,
    ValueCounter,
)
from ray.data.block import BlockAccessor
from ray.data.context import ShuffleStrategy

# 'column05' is 'l_extendedprice'
_VALUE_COLUMN = "column05"


class _SumOfSquares(AggregateFnV2):
    """A Python-engine aggregation that implements ``raw_to_partial``."""

    def __init__(self, on: str):
        super().__init__(f"sum_sq({on})", lambda: 0, on=on, ignore_nulls=True)

    def aggregate_block(self, block):
        column = BlockAccessor.for_block(block).to_arrow()[self._target_col_name]
        return pc.sum(pc.multiply(column, column)).as_py()

    def combine(self, current_accumulator, new):
        return current_accumulator + new

    def raw_to_partial(self, block):
        column = block[self._target_col_name]
        return pc.multiply(column, column)


_AGGREGATIONS = {
    "mean": lambda: Mean(_VALUE_COLUMN),
    "std": lambda: Std(_VALUE_COLUMN),
    "absmax": lambda: AbsMax(_VALUE_COLUMN),
    "quantile": lambda: Quantile(_VALUE_COLUMN, q=0.5),
    "aslist": lambda: AsList(_VALUE_COLUMN),
    "value_counter": lambda: ValueCounter(_VALUE_COLUMN),
    "topk_unique": lambda: TopKUnique(_VALUE_COLUMN, k=10),
    "custom": lambda: _SumOfSquares(_VALUE_COLUMN),
}


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
        default=ShuffleStrategy.SHUFFLE_V2.value,
        nargs="?",
        type=str,
        help="Strategy to use when shuffling data (see ShuffleStrategy for accepted values)",
    )

    parser.add_argument(
        "--num-partitions",
        type=int,
        default=None,
        help=(
            "Number of shuffle partitions. Sets "
            "DataContext.default_hash_shuffle_parallelism (hash strategies only)."
        ),
    )

    parser.add_argument(
        "--aggs",
        nargs="+",
        choices=sorted(_AGGREGATIONS),
        default=None,
        help=(
            "With --aggregate, time each of these aggregations separately "
            "(reported as agg_<name>). Defaults to a single mean, reported "
            "as main."
        ),
    )

    consume_group = parser.add_mutually_exclusive_group()
    consume_group.add_argument("--aggregate", action="store_true")
    consume_group.add_argument("--map-groups", action="store_true")

    return parser.parse_args()


def main(args):
    # 1.5 is ~2x the p90 scheduling loop duration observed on Ray 2.59.
    #
    # TODO: Ratchet this down as we improve scheduling loop overhead.
    benchmark = Benchmark(max_sched_loop_duration_s=1.5)
    if args.aggs:
        failed = []
        for name in args.aggs:
            # Keep timing the others if one fails (e.g. runs out of memory).
            try:
                benchmark.run_fn(
                    f"agg_{name}", _make_benchmark_fn(args, _consume_agg(name))
                )
            except Exception as e:
                print(f"agg_{name} failed: {e!r}")
                failed.append(name)
            print(f"Results so far: {benchmark.result}")
        benchmark.write_result()
        if failed:
            raise RuntimeError(f"Aggregations failed: {failed}")
        return
    else:
        benchmark.run_fn("main", _make_benchmark_fn(args, get_consume_fn(args)))
    benchmark.write_result()


def _consume_agg(name: str):
    def consume_fn(grouped_ds):
        grouped_ds.aggregate(_AGGREGATIONS[name]()).materialize()

    return consume_fn


def _make_benchmark_fn(args, consume_fn):
    def benchmark_fn():
        path = f"s3://ray-benchmark-data/tpch/parquet/sf{args.sf}/lineitem"

        # Configure appropriate shuffle-strategy
        DataContext.get_current().shuffle_strategy = ShuffleStrategy(
            args.shuffle_strategy
        )
        if args.num_partitions is not None:
            DataContext.get_current().default_hash_shuffle_parallelism = (
                args.num_partitions
            )
        # TODO: Don't override once we fix range-based shuffle
        override_num_blocks = (
            100
            if args.shuffle_strategy == ShuffleStrategy.SORT_SHUFFLE_PULL_BASED.value
            else None
        )
        ds = ray.data.read_parquet(path, override_num_blocks=override_num_blocks)

        if args.aggregate:
            ds = ds.select_columns(list(dict.fromkeys([*args.group_by, _VALUE_COLUMN])))
        else:
            ds = ds.map_batches(_cast_strings_to_large, batch_format="pyarrow")
        grouped_ds = ds.groupby(args.group_by)
        start = time.time()
        consume_fn(grouped_ds)
        end = time.time()

        # Report arguments for the benchmark, plus peak memory.
        return {**vars(args), **_peak_node_memory_gb(start, end)}

    return benchmark_fn


def _peak_node_memory_gb(start_unix_time: float, end_unix_time: float) -> dict:
    """Peak physical memory used across the cluster (sum over nodes) and on
    the busiest node, from Prometheus. Covers all processes on the nodes,
    including the object store's shared memory. Empty if the query fails."""
    selector = "ray_node_mem_used_host{SessionName=%s}" % json.dumps(
        ray.get_runtime_context().get_session_name()
    )
    seconds = max(1, math.ceil(end_unix_time - start_unix_time))
    queries = {
        "cluster_memory_used_peak_gb": (
            f"max_over_time(sum({selector})[{seconds}s:10s])"
        ),
        "node_memory_used_peak_gb": f"max(max_over_time({selector}[{seconds}s]))",
    }
    metrics = {}
    for name, query in queries.items():
        try:
            (result,) = _query_prometheus(query, end_unix_time)
            metrics[name] = round(float(result["value"][1]) / 1e9, 2)
        except Exception as e:
            print(f"Failed to query {name}: {e!r}")
    return metrics


def get_consume_fn(args: argparse.Namespace):
    if args.aggregate:

        def consume_fn(grouped_ds):
            grouped_ds.mean(_VALUE_COLUMN).materialize()

    elif args.map_groups:

        def consume_fn(grouped_ds):
            ds = grouped_ds.map_groups(normalize_table, batch_format="pyarrow")
            for _ in ds.iter_internal_ref_bundles():
                pass

    else:
        assert False, f"Invalid consume argument: {args}"

    return consume_fn


def _cast_strings_to_large(table: pa.Table) -> pa.Table:
    schema = pa.schema(
        [
            pa.field(
                f.name,
                pa.large_string() if types.is_string(f.type) else f.type,
                f.nullable,
            )
            for f in table.schema
        ],
        metadata=table.schema.metadata,
    )
    return table.cast(schema)


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
    # Start Ray explicitly so the job config applies (see benchmark_job_config).
    ray.init(job_config=benchmark_job_config())
    args = parse_args()
    main(args)
