import os

import numpy as np
import pyarrow as pa
import pytest
from pyiceberg import catalog as pyi_catalog
from ray.data.datasource.iceberg_datasource import IcebergDatasource

_CATALOG_NAME = "ray_catalog"
_DB_NAME = "ray_db"
_TABLE_NAME = "ray_test"


@pytest.fixture(autouse=True)
def pyiceberg_full_mock(monkeypatch):
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.table import DataScan

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

    pya_table = pa.Table.from_pydict(
        mapping={
            "col_a": list(range(100)),
            "col_b": np.random.choice(["a", "b", "c", "d"], size=100, replace=True),
        },
    )

    if (_DB_NAME,) not in catalog.list_namespaces():
        catalog.create_namespace(_DB_NAME)
    if (_DB_NAME, _TABLE_NAME) in catalog.list_tables(_DB_NAME):
        catalog.drop_table(f"{_DB_NAME}.{_TABLE_NAME}")
    table = catalog.create_table(
        f"{_DB_NAME}.{_TABLE_NAME}",
        schema=pya_table.schema,
    )
    table.append(pya_table)

    plan_files = table.scan().plan_files()

    def pyi_get_catalog_mock(catalog_name, *args, **kwargs):
        if catalog_name != _CATALOG_NAME:
            raise ValueError(f"Catalog {catalog_name} not found!")
        return catalog

    monkeypatch.setattr(pyi_catalog, "load_catalog", pyi_get_catalog_mock)
    monkeypatch.setattr(DataScan, "plan_files", lambda self: list(plan_files) * 10)


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
        import pyarrow as pa
        from pyiceberg import expressions as pyi_expr

        pass

        iceberg_ds = IcebergDatasource(
            table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
            row_filter=pyi_expr.EqualTo("col_a", 1),
            selected_fields=("col_b",),
            catalog_kwargs={"name": _CATALOG_NAME, "type": "sql"},
        )
        read_tasks = iceberg_ds.get_read_tasks(5)
        assert len(read_tasks) == 5
        assert all(len(rt.metadata.input_files) == 2 for rt in read_tasks)

        expected_schema = pa.schema([pa.field("col_b", pa.large_string())])
        assert all(rt.metadata.schema.equals(expected_schema) for rt in read_tasks), (
            [rt.metadata.schema for rt in read_tasks],
            expected_schema,
        )
