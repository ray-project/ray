"""Iceberg release benchmark (P0).

Linear pipeline: write (create) -> read back + verify -> upsert + verify -> overwrite + verify.
Each step is timed. Data is generated distributed via ray.data.range().
Copy-on-write only for now; merge-on-read later.
"""

import argparse
import os
import uuid

from pyiceberg import schema as pyi_schema, types as pyi_types
from pyiceberg.catalog.sql import SqlCatalog

import ray
from benchmark import Benchmark, BenchmarkMetric
from ray.data import SaveMode

NUM_ROWS = 1_000_000
UPSERT_ROWS = 200_000
OVERWRITE_ROWS = 500_000
DEFAULT_WAREHOUSE_PATH = "s3://ray-benchmark-data/iceberg_benchmark/"

_RUN_ID = uuid.uuid4().hex[:12]
_CATALOG_NAME = f"ray_catalog_{_RUN_ID}"
_DB_NAME = "ray_db"
_TABLE_NAME = f"bench_{_RUN_ID}"
_TABLE_ID = f"{_DB_NAME}.{_TABLE_NAME}"
_CATALOG_DB_DIR = f"/tmp/iceberg_catalog/{_RUN_ID}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "warehouse_path",
        nargs="?",
        type=str,
        default=DEFAULT_WAREHOUSE_PATH,
        help="Iceberg warehouse path, e.g. s3://bucket/prefix or file:///tmp/warehouse",
    )
    return parser.parse_args()


def _build_catalog_kwargs(warehouse_path: str) -> dict:
    return {
        "name": _CATALOG_NAME,
        "type": "sql",
        "uri": f"sqlite:///{_CATALOG_DB_DIR}/catalog.db",
        "warehouse": warehouse_path,
    }


def _setup_catalog(catalog_kwargs: dict):
    """Create catalog, namespace, and table"""
    os.makedirs(_CATALOG_DB_DIR, exist_ok=True)
    catalog = SqlCatalog(
        _CATALOG_NAME,
        **{
            "uri": catalog_kwargs["uri"],
            "warehouse": catalog_kwargs["warehouse"],
        },
    )
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
        ),
    )
    assert (_DB_NAME, _TABLE_NAME) in catalog.list_tables(
        _DB_NAME
    ), f"Failed to create table {_TABLE_ID}"


def _make_dataset(n: int) -> ray.data.Dataset:
    """Generate a dataset with id, value, part columns."""
    return ray.data.range(n).map(
        lambda row: {
            "id": row["id"],
            "value": f"v_{row['id']}",
            "part": row["id"] % 10,
        }
    )


def _read_table(catalog_kwargs: dict) -> ray.data.Dataset:
    return ray.data.read_iceberg(
        table_identifier=_TABLE_ID, catalog_kwargs=catalog_kwargs.copy()
    )


def main(args: argparse.Namespace):
    catalog_kwargs = _build_catalog_kwargs(args.warehouse_path)
    _setup_catalog(catalog_kwargs)
    benchmark = Benchmark()

    # 1. Write (create new table)
    def write_create():
        ds = _make_dataset(NUM_ROWS)
        ds.write_iceberg(
            table_identifier=_TABLE_ID,
            catalog_kwargs=catalog_kwargs.copy(),
            mode=SaveMode.APPEND,
        )
        count = _read_table(catalog_kwargs).count()
        assert count == NUM_ROWS, f"write_create: expected {NUM_ROWS}, got {count}"
        return {BenchmarkMetric.NUM_ROWS: NUM_ROWS}

    benchmark.run_fn("write_create", write_create)

    # 2. Read back and consume
    def read():
        count = 0
        for batch in _read_table(catalog_kwargs).iter_batches(batch_format="pyarrow"):
            count += len(batch)
        assert count == NUM_ROWS, f"read: expected {NUM_ROWS}, got {count}"
        return {BenchmarkMetric.NUM_ROWS: count}

    benchmark.run_fn("read", read)

    # 3. Upsert (copy-on-write): update first UPSERT_ROWS ids
    def upsert():
        ds = ray.data.range(UPSERT_ROWS).map(
            lambda row: {
                "id": row["id"],
                "value": f"updated_{row['id']}",
                "part": row["id"] % 10,
            }
        )
        ds.write_iceberg(
            table_identifier=_TABLE_ID,
            catalog_kwargs=catalog_kwargs.copy(),
            mode=SaveMode.UPSERT,
            upsert_kwargs={"join_cols": ["id"]},
        )
        count = _read_table(catalog_kwargs).count()
        assert count == NUM_ROWS, f"upsert: expected {NUM_ROWS}, got {count}"
        return {BenchmarkMetric.NUM_ROWS: UPSERT_ROWS}

    benchmark.run_fn("upsert", upsert)

    # 4. Overwrite (copy-on-write): replace entire table
    def overwrite():
        ds = _make_dataset(OVERWRITE_ROWS)
        ds.write_iceberg(
            table_identifier=_TABLE_ID,
            catalog_kwargs=catalog_kwargs.copy(),
            mode=SaveMode.OVERWRITE,
        )
        count = _read_table(catalog_kwargs).count()
        assert count == OVERWRITE_ROWS, (
            f"overwrite: expected {OVERWRITE_ROWS}, got {count}"
        )
        return {BenchmarkMetric.NUM_ROWS: OVERWRITE_ROWS}

    benchmark.run_fn("overwrite", overwrite)

    benchmark.write_result()


if __name__ == "__main__":
    ray.init()
    args = parse_args()
    main(args)
