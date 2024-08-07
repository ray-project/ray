import os
import random

import pyarrow as pa
import pytest
import ray
from pyiceberg import (
    catalog as pyi_catalog,
    expressions as pyi_expr,
    schema as pyi_schema,
    types as pyi_types,
)
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import IdentityTransform
from ray.data import read_iceberg
from ray.data._internal.datasource.iceberg_datasource import IcebergDatasource

_CATALOG_NAME = "ray_catalog"
_DB_NAME = "ray_db"
_TABLE_NAME = "ray_test"


@pytest.fixture(autouse=True)
def pyiceberg_full_mock(monkeypatch):
    from pyiceberg.catalog.sql import SqlCatalog

    warehouse_path = "/tmp/warehouse"
    if not os.path.exists(warehouse_path):
        os.makedirs(warehouse_path)
    catalog = SqlCatalog(
        _CATALOG_NAME,
        **{
            "uri": f"sqlite:///{warehouse_path}/ray_pyiceberg_test_catalog.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )

    schema = pa.schema([
        pa.field("col_a", pa.int32()),
        pa.field("col_b", pa.string()),
        pa.field("col_c", pa.int16()),
    ])
    pya_table = pa.Table.from_pydict(
        mapping={
            "col_a": list(range(120)),
            "col_b": random.choices(["a", "b", "c", "d"], k=120),
            "col_c": random.choices(list(range(10)), k=120),
        },
        schema=schema,
    )

    if (_DB_NAME,) not in catalog.list_namespaces():
        catalog.create_namespace(_DB_NAME)
    if (_DB_NAME, _TABLE_NAME) in catalog.list_tables(_DB_NAME):
        catalog.drop_table(f"{_DB_NAME}.{_TABLE_NAME}")

    # Create the table, and add data to it
    table = catalog.create_table(
        f"{_DB_NAME}.{_TABLE_NAME}",
        schema=pyi_schema.Schema(
            pyi_types.NestedField(
                field_id=1,
                name="col_a",
                field_type=pyi_types.IntegerType(),
                required=False,
            ),
            pyi_types.NestedField(
                field_id=2,
                name="col_b",
                field_type=pyi_types.StringType(),
                required=False,
            ),
            pyi_types.NestedField(
                field_id=3,
                name="col_c",
                field_type=pyi_types.IntegerType(),
                required=False,
            ),
        ),
        partition_spec=PartitionSpec(
            PartitionField(source_id=3, field_id=3, transform=IdentityTransform(), name="col_c")
        ),
    )
    table.append(pya_table)

    # Delete some data so there are delete file(s)
    table.delete(delete_filter=pyi_expr.GreaterThanOrEqual("col_a", 101))

    def pyi_get_catalog_mock(catalog_name, *args, **kwargs):
        if catalog_name != _CATALOG_NAME:
            raise ValueError(f"Catalog {catalog_name} not found!")
        return catalog

    monkeypatch.setattr(pyi_catalog, "load_catalog", pyi_get_catalog_mock)
    # monkeypatch.setattr(DataScan, "plan_files", lambda self: list(plan_files) * 10)


class TestReadIceberg:
    def test_get_catalog(self):
        iceberg_ds = IcebergDatasource(
            table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
            catalog_kwargs={"name": _CATALOG_NAME},
        )
        catalog = iceberg_ds._get_catalog()
        assert catalog.name == _CATALOG_NAME

    def test_plan_files(self):
        iceberg_ds = IcebergDatasource(
            table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
            catalog_kwargs={"name": _CATALOG_NAME},
        )
        plan_files = iceberg_ds.plan_files
        assert len(plan_files) == 10

    def test_chunk_plan_files(self):
        iceberg_ds = IcebergDatasource(
            table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
            catalog_kwargs={"name": _CATALOG_NAME},
        )

        chunks = iceberg_ds._distribute_tasks_into_equal_chunks(iceberg_ds.plan_files, 5)
        assert (len(c) == 2 for c in chunks)

        chunks = iceberg_ds._distribute_tasks_into_equal_chunks(iceberg_ds.plan_files, 20)
        assert sum(len(c) == 1 for c in chunks) == 10 and sum(len(c) == 0 for c in chunks) == 10

    def test_get_read_tasks(self):
        iceberg_ds = IcebergDatasource(
            table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
            catalog_kwargs={"name": _CATALOG_NAME, "type": "sql"},
        )
        read_tasks = iceberg_ds.get_read_tasks(5)
        assert len(read_tasks) == 5
        assert all(len(rt.metadata.input_files) == 2 for rt in read_tasks)

    def test_filtered_read(self):
        from pyiceberg import expressions as pyi_expr

        iceberg_ds = IcebergDatasource(
            table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
            row_filter=pyi_expr.In("col_c", {1, 2, 3, 4}),
            selected_fields=("col_b",),
            catalog_kwargs={"name": _CATALOG_NAME, "type": "sql"},
        )
        read_tasks = iceberg_ds.get_read_tasks(5)
        # Should be capped to 4, as there will be only 4 files
        assert len(read_tasks) == 4, read_tasks
        assert all(len(rt.metadata.input_files) == 1 for rt in read_tasks)


def test_read_basic(pyiceberg_full_mock):
    ray_ds = read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        row_filter=pyi_expr.In("col_c", {1, 2, 3, 4, 5, 6, 7, 8}),
        selected_fields=(
            "col_a",
            "col_b",
        ),
        catalog_kwargs={"name": _CATALOG_NAME, "type": "sql"},
    )
    table: pa.Table = pa.concat_tables((ray.get(ref) for ref in ray_ds.to_arrow_refs()))

    expected_schema = pa.schema([pa.field("col_b", pa.int32()), pa.field("col_b", pa.string())])
    assert table.schema.equals(expected_schema)
