import os
import random

import pyarrow as pa
import pytest
from packaging.version import Version
from pkg_resources import parse_version
from pyiceberg import catalog as pyi_catalog
from pyiceberg import expressions as pyi_expr
from pyiceberg import schema as pyi_schema
from pyiceberg import types as pyi_types
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import IdentityTransform

import ray
from ray._private.utils import _get_pyarrow_version
from ray.data import read_iceberg
from ray.data._internal.datasource.iceberg_datasource import IcebergDatasource

_CATALOG_NAME = "ray_catalog"
_DB_NAME = "ray_db"
_TABLE_NAME = "ray_test"
_WAREHOUSE_PATH = "/tmp/warehouse"

_CATALOG_KWARGS = {
    "name": _CATALOG_NAME,
    "type": "sql",
    "uri": f"sqlite:///{_WAREHOUSE_PATH}/ray_pyiceberg_test_catalog.db",
    "warehouse": f"file://{_WAREHOUSE_PATH}",
}


@pytest.fixture(autouse=True, scope="session")
def pyiceberg_table():
    from pyiceberg.catalog.sql import SqlCatalog

    if not os.path.exists(_WAREHOUSE_PATH):
        os.makedirs(_WAREHOUSE_PATH)
    dummy_catalog = SqlCatalog(
        _CATALOG_NAME,
        **{
            "uri": f"sqlite:///{_WAREHOUSE_PATH}/ray_pyiceberg_test_catalog.db",
            "warehouse": f"file://{_WAREHOUSE_PATH}",
        },
    )

    schema = pa.schema(
        [
            pa.field("col_a", pa.int32()),
            pa.field("col_b", pa.string()),
            pa.field("col_c", pa.int16()),
        ]
    )
    pya_table = pa.Table.from_pydict(
        mapping={
            "col_a": list(range(120)),
            "col_b": random.choices(["a", "b", "c", "d"], k=120),
            "col_c": random.choices(list(range(10)), k=120),
        },
        schema=schema,
    )

    if (_DB_NAME,) not in dummy_catalog.list_namespaces():
        dummy_catalog.create_namespace(_DB_NAME)
    if (_DB_NAME, _TABLE_NAME) in dummy_catalog.list_tables(_DB_NAME):
        dummy_catalog.drop_table(f"{_DB_NAME}.{_TABLE_NAME}")

    # Create the table, and add data to it
    table = dummy_catalog.create_table(
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
            PartitionField(
                source_id=3, field_id=3, transform=IdentityTransform(), name="col_c"
            )
        ),
    )
    table.append(pya_table)

    # Delete some data so there are delete file(s)
    table.delete(delete_filter=pyi_expr.GreaterThanOrEqual("col_a", 101))


@pytest.mark.skipif(
    Version(pa.__version__) < Version("9.0.0"),
    reason="PyIceberg depends on pyarrow>=9.0.0",
)
def test_get_catalog():
    # NOTE: Iceberg only works with PyArrow 9 or above.
    pyarrow_version = _get_pyarrow_version()
    if pyarrow_version is not None:
        pyarrow_version = parse_version(pyarrow_version)
    if pyarrow_version is not None and pyarrow_version < parse_version("9.0.0"):
        return

    iceberg_ds = IcebergDatasource(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    catalog = iceberg_ds._get_catalog()
    assert catalog.name == _CATALOG_NAME


@pytest.mark.skipif(
    Version(pa.__version__) < Version("9.0.0"),
    reason="PyIceberg depends on pyarrow>=9.0.0",
)
def test_plan_files():
    # NOTE: Iceberg only works with PyArrow 9 or above.
    pyarrow_version = _get_pyarrow_version()
    if pyarrow_version is not None:
        pyarrow_version = parse_version(pyarrow_version)
    if pyarrow_version is not None and pyarrow_version < parse_version("9.0.0"):
        return

    iceberg_ds = IcebergDatasource(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    plan_files = iceberg_ds.plan_files
    assert len(plan_files) == 10


@pytest.mark.skipif(
    Version(pa.__version__) < Version("9.0.0"),
    reason="PyIceberg depends on pyarrow>=9.0.0",
)
def test_chunk_plan_files():
    # NOTE: Iceberg only works with PyArrow 9 or above.
    pyarrow_version = _get_pyarrow_version()
    if pyarrow_version is not None:
        pyarrow_version = parse_version(pyarrow_version)
    if pyarrow_version is not None and pyarrow_version < parse_version("9.0.0"):
        return

    iceberg_ds = IcebergDatasource(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )

    chunks = iceberg_ds._distribute_tasks_into_equal_chunks(iceberg_ds.plan_files, 5)
    assert (len(c) == 2 for c in chunks), chunks

    chunks = iceberg_ds._distribute_tasks_into_equal_chunks(iceberg_ds.plan_files, 20)
    assert (
        sum(len(c) == 1 for c in chunks) == 10
        and sum(len(c) == 0 for c in chunks) == 10
    ), chunks


@pytest.mark.skipif(
    Version(pa.__version__) < Version("9.0.0"),
    reason="PyIceberg depends on pyarrow>=9.0.0",
)
def test_get_read_tasks():
    # NOTE: Iceberg only works with PyArrow 9 or above.
    pyarrow_version = _get_pyarrow_version()
    if pyarrow_version is not None:
        pyarrow_version = parse_version(pyarrow_version)
    if pyarrow_version is not None and pyarrow_version < parse_version("9.0.0"):
        return

    iceberg_ds = IcebergDatasource(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    read_tasks = iceberg_ds.get_read_tasks(5)
    assert len(read_tasks) == 5
    assert all(len(rt.metadata.input_files) == 2 for rt in read_tasks)


@pytest.mark.skipif(
    Version(pa.__version__) < Version("9.0.0"),
    reason="PyIceberg depends on pyarrow>=9.0.0",
)
def test_filtered_read():
    # NOTE: Iceberg only works with PyArrow 9 or above.
    pyarrow_version = _get_pyarrow_version()
    if pyarrow_version is not None:
        pyarrow_version = parse_version(pyarrow_version)
    if pyarrow_version is not None and pyarrow_version < parse_version("9.0.0"):
        return

    from pyiceberg import expressions as pyi_expr

    iceberg_ds = IcebergDatasource(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        row_filter=pyi_expr.In("col_c", {1, 2, 3, 4}),
        selected_fields=("col_b",),
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    read_tasks = iceberg_ds.get_read_tasks(5)
    # Should be capped to 4, as there will be only 4 files
    assert len(read_tasks) == 4, read_tasks
    assert all(len(rt.metadata.input_files) == 1 for rt in read_tasks)


@pytest.mark.skipif(
    Version(pa.__version__) < Version("9.0.0"),
    reason="PyIceberg depends on pyarrow>=9.0.0",
)
def test_read_basic():
    # NOTE: Iceberg only works with PyArrow 9 or above.
    pyarrow_version = _get_pyarrow_version()
    if pyarrow_version is not None:
        pyarrow_version = parse_version(pyarrow_version)
    if pyarrow_version is not None and pyarrow_version < parse_version("9.0.0"):
        return

    row_filter = pyi_expr.In("col_c", {1, 2, 3, 4, 5, 6, 7, 8})

    ray_ds = read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        row_filter=row_filter,
        selected_fields=("col_a", "col_b"),
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    table: pa.Table = pa.concat_tables((ray.get(ref) for ref in ray_ds.to_arrow_refs()))

    # string -> large_string because pyiceberg by default chooses large_string
    expected_schema = pa.schema(
        [pa.field("col_a", pa.int32()), pa.field("col_b", pa.large_string())]
    )
    assert table.schema.equals(expected_schema)

    # Read the raw table from PyIceberg
    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    orig_table_p = (
        sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
        .scan(row_filter=row_filter, selected_fields=("col_a", "col_b"))
        .to_pandas()
        .sort_values(["col_a", "col_b"])
        .reset_index(drop=True)
    )

    # Actually compare the tables now
    table_p = ray_ds.to_pandas().sort_values(["col_a", "col_b"]).reset_index(drop=True)
    assert orig_table_p.equals(table_p)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
