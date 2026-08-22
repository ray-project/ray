import argparse
import time

import pyarrow as pa
from pyarrow import types
import pyarrow.compute as pc
import ray

from benchmark import Benchmark
from ray.data import DataContext
from ray.data.context import ShuffleStrategy

# Same row-size estimate as bench_shuffle.py (TPC-H lineitem).
APPROX_BYTES_PER_ROW = 145


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
        "--data-size-gb",
        type=int,
        default=None,
        help=(
            "If set, limit the lineitem read to about this many GB "
            f"(rows ≈ GB * 1024**3 / {APPROX_BYTES_PER_ROW})."
        ),
    )
    parser.add_argument(
        "--shuffle-transport",
        choices=["in-memory", "external"],
        default="in-memory",
        help=(
            "Under shuffle_v2 / SHUFFLE_V2: object-store shards (in-memory) vs "
            "on-disk Flight file-transport (external). Ignored for non-v2 "
            "strategies."
        ),
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Print ds.stats() after the consume phase.",
    )

    consume_group = parser.add_mutually_exclusive_group()
    consume_group.add_argument("--aggregate", action="store_true")
    consume_group.add_argument("--map-groups", action="store_true")

    return parser.parse_args()


def _plan_op_names(ds) -> list:
    from ray.data._internal.logical.optimizers import get_execution_plan

    dag = get_execution_plan(ds._logical_plan)[0].dag
    return [type(op).__name__ for op in dag.post_order_iter()]


def main(args):
    benchmark = Benchmark()
    consume_fn = get_consume_fn(args)

    def benchmark_fn():
        path = f"s3://ray-benchmark-data/tpch/parquet/sf{args.sf}/lineitem"

        # Configure appropriate shuffle-strategy
        ctx = DataContext.get_current()
        ctx.shuffle_strategy = ShuffleStrategy(args.shuffle_strategy)
        if args.shuffle_transport == "external":
            if ctx.shuffle_strategy != ShuffleStrategy.SHUFFLE_V2:
                raise ValueError(
                    "--shuffle-transport=external requires "
                    "--shuffle-strategy shuffle_v2 (or HASH_SHUFFLE_V2)"
                )
            ctx.use_external_hash_shuffle = True
        else:
            ctx.use_external_hash_shuffle = False
        if args.num_partitions is not None:
            ctx.default_hash_shuffle_parallelism = args.num_partitions
        # TODO: Don't override once we fix range-based shuffle
        override_num_blocks = (
            100
            if args.shuffle_strategy == ShuffleStrategy.SORT_SHUFFLE_PULL_BASED.value
            else None
        )

        limit_rows = None
        if args.data_size_gb is not None:
            limit_rows = int(args.data_size_gb * 1024**3 / APPROX_BYTES_PER_ROW)

        print(
            f"CONFIG shuffle_strategy={ctx.shuffle_strategy} "
            f"use_external_hash_shuffle={ctx.use_external_hash_shuffle} "
            f"num_partitions={ctx.default_hash_shuffle_parallelism} "
            f"data_size_gb={args.data_size_gb} limit_rows={limit_rows}",
            flush=True,
        )

        t0 = time.perf_counter()
        ds = ray.data.read_parquet(path, override_num_blocks=override_num_blocks)
        if limit_rows is not None:
            ds = ds.limit(limit_rows)
        # Cast string columns to large_string: on low-cardinality keys a single
        # group's string data can exceed 2GB per column, overflowing Arrow's
        # int32 string offsets when the shuffle reduce sorts the partition
        # into one contiguous table.
        ds = ds.map_batches(_cast_strings_to_large, batch_format="pyarrow")
        grouped_ds = ds.groupby(args.group_by)

        # Build the lazy sink first so we can assert the physical plan before
        # spending ~10min on SF1000 execution.
        if args.map_groups:
            out_ds = grouped_ds.map_groups(normalize_table, batch_format="pyarrow")
            op_names = _plan_op_names(out_ds)
            print(f"PLAN_OPS {op_names}", flush=True)
            if args.shuffle_transport == "external":
                assert any(
                    "ExternalHashShuffle" in n for n in op_names
                ), f"expected ExternalHashShuffle* in plan, got {op_names}"
            else:
                assert not any(
                    "ExternalHashShuffle" in n for n in op_names
                ), f"unexpected ExternalHashShuffle* in in-memory plan: {op_names}"
            t_exec0 = time.perf_counter()
            n_bundles = 0
            for _ in out_ds.iter_internal_ref_bundles():
                n_bundles += 1
            exec_s = time.perf_counter() - t_exec0
            print(
                f"CONSUME map_groups exec_s={exec_s:.1f} bundles={n_bundles}",
                flush=True,
            )
            extra = {"map_groups_exec_s": exec_s, "output_bundles": n_bundles}
        else:
            out_ds, extra = consume_fn(grouped_ds)
            op_names = _plan_op_names(out_ds)
            print(f"PLAN_OPS {op_names}", flush=True)

        wall_s = time.perf_counter() - t0

        if args.stats:
            stats_str = out_ds.stats()
            print("\n===== ds.stats() =====\n" + stats_str + "\n", flush=True)
        else:
            stats_str = None

        print(f"RESULT_WALL wall_s={wall_s:.1f}", flush=True)

        metrics = vars(args)
        metrics.update(
            {
                "wall_s": wall_s,
                "use_external_hash_shuffle": ctx.use_external_hash_shuffle,
                "plan_ops": op_names,
                **extra,
            }
        )
        if stats_str is not None:
            metrics["stats"] = stats_str
        return metrics

    benchmark.run_fn("main", benchmark_fn)
    benchmark.write_result()


def get_consume_fn(args: argparse.Namespace):
    if args.aggregate:

        def consume_fn(grouped_ds):
            # 'column05' is 'l_extendedprice'
            out = grouped_ds.mean("column05").materialize()
            return out, {}

    elif args.map_groups:

        def consume_fn(grouped_ds):
            ds = grouped_ds.map_groups(normalize_table, batch_format="pyarrow")
            t_exec0 = time.perf_counter()
            n_bundles = 0
            for _ in ds.iter_internal_ref_bundles():
                n_bundles += 1
            exec_s = time.perf_counter() - t_exec0
            print(
                f"CONSUME map_groups exec_s={exec_s:.1f} bundles={n_bundles}",
                flush=True,
            )
            return ds, {"map_groups_exec_s": exec_s, "output_bundles": n_bundles}

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
    args = parse_args()
    main(args)
