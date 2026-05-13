"""Benchmark for measuring actor scaling overhead with 1000 actors.

Measures how long it takes to spin up actors, process data, and tear down
across a range(N) -> map_batches(1000 actors) -> consume pipeline.
"""

import argparse

import ray
from benchmark import (
    Benchmark,
    RuntimeEnvSetupTracker,
    benchmark_py_modules,
    collect_dataset_stats,
)

BLOCKS_PER_WORKER: int = 10
TARGET_BLOCK_SIZE_BYTES: int = 128 * 1024 * 1024  # 128 MiB
BYTES_PER_ROW: int = 8  # ray.data.range produces one int64 per row
ROWS_PER_BLOCK: int = TARGET_BLOCK_SIZE_BYTES // BYTES_PER_ROW

# When --num-schema-columns > 1, the UDF expands each input batch into a
# table with this many int64 columns (all aliasing the input array, so
# memory stays small but the PyArrow schema gets wide). The wider schema
# inflates the per-block ``BlockMetadataWithSchema`` blob that the
# StreamingExecutor's ``on_data_ready`` retrieves via
# ``ray.get(meta_ref)``, which can become a noticeable bottleneck at
# scale. Pick a row count for the output block accordingly.
DEFAULT_NUM_SCHEMA_COLUMNS: int = 1
# Cap the output block size for the wide-schema path so we don't blow up
# worker memory once each row materializes ``num_schema_columns`` cells.
WIDE_SCHEMA_TARGET_BLOCK_SIZE_BYTES: int = 16 * 1024 * 1024  # 16 MiB
# Column-name length (chars). Mirrors the pattern from #56353, which
# generates wide-schema parquet datasets with 500-char column names so
# the serialized schema bytes stay proportional to the field count, not
# just the count itself.
WIDE_SCHEMA_COLUMN_NAME_LEN: int = 500


def _rows_per_block_for_schema(num_columns: int) -> int:
    """Choose a row count so the output block (rows × cols × 8B) stays bounded."""
    if num_columns <= 1:
        return ROWS_PER_BLOCK
    rows = WIDE_SCHEMA_TARGET_BLOCK_SIZE_BYTES // (8 * num_columns)
    return max(rows, 1)


def _make_wide_column_names(num_columns: int) -> list:
    """Generate unique column names padded to ``WIDE_SCHEMA_COLUMN_NAME_LEN`` chars.

    For ``num_columns == 1`` we return the original short ``"col_0"`` so
    the default (no-op) configuration is byte-identical to the previous
    benchmark output. Wide configurations pad every name to a fixed
    length so the serialized schema scales with both the field count
    *and* the per-field bytes (matching #56353's data layout).
    """
    if num_columns <= 1:
        return ["col_0"]
    digits = max(len(str(num_columns - 1)), 1)
    pad = "x" * max(WIDE_SCHEMA_COLUMN_NAME_LEN - len("col__") - digits, 0)
    return [f"col_{i:0{digits}d}_{pad}" for i in range(num_columns)]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num-workers",
        type=int,
        required=True,
        help="Number of actors/tasks to use for map_batches.",
    )
    parser.add_argument(
        "--worker-type",
        type=str,
        choices=["actors", "tasks"],
        default="actors",
        help="Whether to use actors or regular tasks for map_batches.",
    )
    parser.add_argument(
        "--num-schema-columns",
        type=int,
        default=DEFAULT_NUM_SCHEMA_COLUMNS,
        help=(
            "Number of int64 columns each output block carries. The "
            "default (1) preserves the original no-op behavior. Larger "
            "values (e.g., 1000) inflate the per-block "
            "BlockMetadataWithSchema, making ray.get(meta_ref) inside "
            "on_data_ready a measurable bottleneck for scheduler "
            "stress testing."
        ),
    )
    return parser.parse_args()


class WideSchemaUDF:
    """Returns a wide-schema batch by aliasing the input ``id`` array into
    ``num_columns`` named columns.

    The aliasing keeps memory cheap on the producer side — all output
    columns reference the same NumPy array — but the resulting PyArrow
    schema is genuinely wide, so the serialized
    ``BlockMetadataWithSchema`` that the StreamingExecutor retrieves
    per block grows proportionally. Column names are padded to
    ``WIDE_SCHEMA_COLUMN_NAME_LEN`` chars to match the pattern from
    #56353 (wide-schema parquet datasets), so the schema bytes scale
    with both field count and per-field name length.
    """

    def __init__(self, num_columns: int = DEFAULT_NUM_SCHEMA_COLUMNS):
        self._num_columns = num_columns
        self._column_names = _make_wide_column_names(num_columns)

    def __call__(self, batch):
        ids = batch["id"]
        return {name: ids for name in self._column_names}


def make_wide_schema_udf(num_columns: int):
    """Functional variant of ``WideSchemaUDF`` for the task-based path."""
    column_names = _make_wide_column_names(num_columns)

    def _udf(batch):
        ids = batch["id"]
        return {name: ids for name in column_names}

    return _udf


def main(args: argparse.Namespace):
    benchmark = Benchmark()

    def benchmark_fn():
        num_blocks = BLOCKS_PER_WORKER * args.num_workers
        rows_per_block = _rows_per_block_for_schema(args.num_schema_columns)
        num_rows = num_blocks * rows_per_block
        ds = ray.data.range(num_rows, override_num_blocks=num_blocks)

        if args.worker_type == "actors":
            ds = ds.map_batches(
                WideSchemaUDF,
                fn_constructor_kwargs={"num_columns": args.num_schema_columns},
                num_cpus=1,
                compute=ray.data.ActorPoolStrategy(size=args.num_workers),
            )
        else:
            ds = ds.map_batches(
                make_wide_schema_udf(args.num_schema_columns),
                num_cpus=1,
            )

        ds = ds.materialize()
        metrics = collect_dataset_stats(ds)
        metrics["runtime_env_setup"] = RuntimeEnvSetupTracker.collect()
        metrics["num_blocks"] = num_blocks
        metrics["num_rows"] = num_rows
        metrics["num_schema_columns"] = args.num_schema_columns
        metrics["rows_per_block"] = rows_per_block
        return metrics

    benchmark.run_fn("worker_scaling", benchmark_fn)
    benchmark.write_result()


if __name__ == "__main__":
    ray.init(runtime_env={"py_modules": benchmark_py_modules()})
    args = parse_args()
    main(args)
