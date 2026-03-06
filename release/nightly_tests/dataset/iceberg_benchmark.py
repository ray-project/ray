"""Iceberg release benchmark"""

import argparse
import uuid

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg import catalog as pyi_catalog, schema as pyi_schema, types as pyi_types

import ray
from benchmark import Benchmark, BenchmarkMetric
from ray.data import SaveMode
from ray.data.datatype import DataType
from ray.data.expressions import col, udf

NUM_ROWS = 50_000_000
UPSERT_ROWS = 75_000_000
OVERWRITE_ROWS = 200_000_000
EMBEDDING_DIM = 32
TOKEN_IDS_DIM = 8
LOGITS_DIM = 4
DEFAULT_WAREHOUSE_PATH = "s3://ray-benchmark-data-internal-us-west-2/iceberg_benchmark/"

_RUN_ID = uuid.uuid4().hex[:12]
_CATALOG_NAME = f"ray_catalog_{_RUN_ID}"
_DB_NAME = "ray_db"
_TABLE_NAME = f"bench_{_RUN_ID}"
_TABLE_ID = f"{_DB_NAME}.{_TABLE_NAME}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "warehouse_path",
        nargs="?",
        type=str,
        default=DEFAULT_WAREHOUSE_PATH,
        help="Iceberg warehouse path, e.g. s3://bucket/prefix or file:///tmp/warehouse",
    )
    parser.add_argument(
        "--mode",
        choices=["append", "upsert", "overwrite"],
        required=True,
        help="Write mode to benchmark",
    )
    return parser.parse_args()


def _get_catalog_kwargs(warehouse_path: str) -> dict:
    return {
        "name": _CATALOG_NAME,
        "type": "glue",
        "client.region": "us-west-2",
        "warehouse": warehouse_path,
    }


def _load_catalog(catalog_kwargs: dict):
    """Load the catalog using pyiceberg using the catalog_kwargs"""
    catalog_name = catalog_kwargs["name"]
    catalog_properties = {k: v for k, v in catalog_kwargs.items() if k != "name"}
    return pyi_catalog.load_catalog(catalog_name, **catalog_properties)


def _setup_catalog(catalog: pyi_catalog.Catalog):
    """Create catalog, namespace, and table"""
    if (_DB_NAME,) not in catalog.list_namespaces():
        catalog.create_namespace(_DB_NAME)
    catalog.create_table(
        _TABLE_ID,
        schema=pyi_schema.Schema(
            pyi_types.NestedField(
                field_id=1, name="id", field_type=pyi_types.LongType(), required=False
            ),
            pyi_types.NestedField(
                field_id=2,
                name="value",
                field_type=pyi_types.StringType(),
                required=False,
            ),
            pyi_types.NestedField(
                field_id=3,
                name="part",
                field_type=pyi_types.LongType(),
                required=False,
            ),
            pyi_types.NestedField(
                field_id=4,
                name="embedding",
                field_type=pyi_types.ListType(
                    element_id=7,
                    element_type=pyi_types.DoubleType(),
                    element_required=False,
                ),
                required=False,
            ),
            pyi_types.NestedField(
                field_id=5,
                name="token_ids",
                field_type=pyi_types.ListType(
                    element_id=8,
                    element_type=pyi_types.LongType(),
                    element_required=False,
                ),
                required=False,
            ),
            pyi_types.NestedField(
                field_id=6,
                name="logits",
                field_type=pyi_types.ListType(
                    element_id=9,
                    element_type=pyi_types.DoubleType(),
                    element_required=False,
                ),
                required=False,
            ),
            pyi_types.NestedField(
                field_id=10,
                name="score",
                field_type=pyi_types.DoubleType(),
                required=False,
            ),
            pyi_types.NestedField(
                field_id=11,
                name="confidence",
                field_type=pyi_types.DoubleType(),
                required=False,
            ),
        ),
    )
    assert (_DB_NAME, _TABLE_NAME) in catalog.list_tables(
        _DB_NAME
    ), f"Failed to create table {_TABLE_ID}"


def _teardown_catalog(catalog: pyi_catalog.Catalog):
    """Drop benchmark resources to avoid accumulating metadata and data files."""
    if (_DB_NAME,) not in catalog.list_namespaces():
        return

    table_exists = (_DB_NAME, _TABLE_NAME) in catalog.list_tables(_DB_NAME)
    if table_exists:
        catalog.purge_table(_TABLE_ID)

    # Best-effort cleanup of the namespace created by this benchmark.
    if not catalog.list_tables(_DB_NAME):
        catalog.drop_namespace(_DB_NAME)


def _make_dataset(n: int, value_prefix: str = "value_") -> ray.data.Dataset:
    """Generate a dataset using with_column + expressions."""

    prefix = pa.scalar(value_prefix)

    @udf(return_dtype=DataType.string())
    def make_value(ids: pa.Array) -> pa.Array:
        return pc.binary_join_element_wise(prefix, pc.cast(ids, pa.string()), "")

    @udf(return_dtype=DataType.int64())
    def make_part(ids: pa.Array) -> pa.Array:
        return pa.array(np.asarray(ids) % 10, type=pa.int64())

    @udf(return_dtype=DataType.fixed_size_list(DataType.float64(), EMBEDDING_DIM))
    def make_embedding(ids: pa.Array) -> pa.Array:
        ids_np = np.asarray(ids)
        flat = ((ids_np[:, None] + np.arange(EMBEDDING_DIM)) % 100).astype(
            np.float64
        ) / 100.0
        return pa.FixedSizeListArray.from_arrays(
            pa.array(flat.flatten()), EMBEDDING_DIM
        )

    @udf(return_dtype=DataType.fixed_size_list(DataType.int64(), TOKEN_IDS_DIM))
    def make_token_ids(ids: pa.Array) -> pa.Array:
        ids_np = np.asarray(ids)
        flat = (ids_np[:, None] + np.arange(TOKEN_IDS_DIM)) % 1024
        return pa.FixedSizeListArray.from_arrays(
            pa.array(flat.flatten()), TOKEN_IDS_DIM
        )

    @udf(return_dtype=DataType.fixed_size_list(DataType.float64(), LOGITS_DIM))
    def make_logits(ids: pa.Array) -> pa.Array:
        ids_np = np.asarray(ids)
        flat = (ids_np[:, None] * (np.arange(LOGITS_DIM) + 1)) % 7
        return pa.FixedSizeListArray.from_arrays(
            pa.array(flat.flatten().astype(np.float64)), LOGITS_DIM
        )

    return (
        ray.data.range(n)
        .with_column("value", make_value(col("id")))
        .with_column("part", make_part(col("id")))
        .with_column("embedding", make_embedding(col("id")))
        .with_column("token_ids", make_token_ids(col("id")))
        .with_column("logits", make_logits(col("id")))
        .with_column("score", (col("id") % 1000) / 1000.0)
        .with_column("confidence", ((col("id") % 100) + 1) / 100.0)
    )


def _seed_table(catalog_kwargs: dict):
    """Seed the table with initial data"""
    _make_dataset(NUM_ROWS).write_iceberg(
        table_identifier=_TABLE_ID,
        catalog_kwargs=catalog_kwargs.copy(),
        mode=SaveMode.APPEND,
    )


def main(args: argparse.Namespace):
    catalog_kwargs = _get_catalog_kwargs(args.warehouse_path)
    catalog = _load_catalog(catalog_kwargs)
    benchmark = Benchmark()

    try:
        _setup_catalog(catalog)
        if args.mode == "append":

            def write():
                _make_dataset(NUM_ROWS).write_iceberg(
                    table_identifier=_TABLE_ID,
                    catalog_kwargs=catalog_kwargs.copy(),
                    mode=SaveMode.APPEND,
                )
                return {BenchmarkMetric.NUM_ROWS: NUM_ROWS}

            benchmark.run_fn("append", write)

        elif args.mode == "upsert":
            # Seed the table with initial data (not part of benchmark)
            _seed_table(catalog_kwargs)

            def upsert():
                _make_dataset(UPSERT_ROWS, value_prefix="updated_").write_iceberg(
                    table_identifier=_TABLE_ID,
                    catalog_kwargs=catalog_kwargs.copy(),
                    mode=SaveMode.UPSERT,
                    upsert_kwargs={"join_cols": ["id"]},
                )
                return {BenchmarkMetric.NUM_ROWS: UPSERT_ROWS}

            benchmark.run_fn("upsert", upsert)

        elif args.mode == "overwrite":
            # Seed the table with initial data (not part of benchmark)
            _seed_table(catalog_kwargs)

            def overwrite():
                _make_dataset(OVERWRITE_ROWS).write_iceberg(
                    table_identifier=_TABLE_ID,
                    catalog_kwargs=catalog_kwargs.copy(),
                    mode=SaveMode.OVERWRITE,
                )
                return {BenchmarkMetric.NUM_ROWS: OVERWRITE_ROWS}

            benchmark.run_fn("overwrite", overwrite)

        benchmark.write_result()
    finally:
        _teardown_catalog(catalog)


if __name__ == "__main__":
    ray.init()
    args = parse_args()
    main(args)
