"""Iceberg release benchmark"""

import argparse
import uuid

from pyiceberg import catalog as pyi_catalog, schema as pyi_schema, types as pyi_types

import ray
from benchmark import Benchmark, BenchmarkMetric
from ray.data import SaveMode

NUM_ROWS = 50_000_000
UPSERT_ROWS = 4_000_000
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
    """Generate a dataset with id, value, part columns."""
    return ray.data.range(n).map(
        lambda row: {
            "id": row["id"],
            "value": f"{value_prefix}{row['id']}",
            "part": row["id"] % 10,
            "embedding": [
                float((row["id"] + i) % 100) / 100.0 for i in range(EMBEDDING_DIM)
            ],
            "token_ids": [(row["id"] + i) % 1024 for i in range(TOKEN_IDS_DIM)],
            "logits": [float((row["id"] * (i + 1)) % 7) for i in range(LOGITS_DIM)],
            "score": float(row["id"] % 1000) / 1000.0,
            "confidence": float((row["id"] % 100) + 1) / 100.0,
        }
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
    _setup_catalog(catalog)
    benchmark = Benchmark()

    try:
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
