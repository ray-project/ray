import os
import random

import pyarrow as pa
import pytest
from pkg_resources import parse_version
from pyiceberg import (
    catalog as pyi_catalog,
    expressions as pyi_expr,
    schema as pyi_schema,
    types as pyi_types,
)
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import IdentityTransform

import ray
from ray._private.arrow_utils import get_pyarrow_version
from ray.data import read_iceberg
from ray.data._internal.datasource.iceberg import IcebergDatasource

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

_SCHEMA = pa.schema(
    [
        pa.field("col_a", pa.int32()),
        pa.field("col_b", pa.string()),
        pa.field("col_c", pa.int16()),
    ]
)


def create_pa_table():
    return pa.Table.from_pydict(
        mapping={
            "col_a": list(range(120)),
            "col_b": random.choices(["a", "b", "c", "d"], k=120),
            "col_c": random.choices(list(range(10)), k=120),
        },
        schema=_SCHEMA,
    )


@pytest.fixture(autouse=True, scope="function")
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

    pya_table = create_pa_table()

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
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_get_catalog():

    iceberg_ds = IcebergDatasource(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    catalog = iceberg_ds._get_catalog()
    assert catalog.name == _CATALOG_NAME


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_plan_files():

    iceberg_ds = IcebergDatasource(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    plan_files = iceberg_ds.plan_files
    assert len(plan_files) == 10


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_chunk_plan_files():

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
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_get_read_tasks():

    iceberg_ds = IcebergDatasource(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    read_tasks = iceberg_ds.get_read_tasks(5)
    assert len(read_tasks) == 5
    assert all(len(rt.metadata.input_files) == 2 for rt in read_tasks)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_filtered_read():

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
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_read_basic():

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


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_write_basic():

    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    table = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
    table.delete()

    ds = ray.data.from_arrow(create_pa_table())
    ds.write_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )

    # Read the raw table from PyIceberg after writing
    table = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
    orig_table_p = (
        table.scan()
        .to_pandas()
        .sort_values(["col_a", "col_b", "col_c"])
        .reset_index(drop=True)
    )

    table_p = (
        ds.to_pandas().sort_values(["col_a", "col_b", "col_c"]).reset_index(drop=True)
    )
    assert orig_table_p.equals(table_p)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_write_concurrency():

    import numpy as np
    import pandas as pd

    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    table = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
    table.delete()

    data = pd.DataFrame(
        {
            "col_a": np.array([1, 2, 3, 4], dtype=np.int32),
            "col_b": ["1", "2", "3", "4"],
            "col_c": np.array([1, 2, 3, 4], dtype=np.int32),
        }
    )
    write_ds = ray.data.from_pandas(data).repartition(2)
    write_ds.write_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
        concurrency=2,
    )
    read_ds = ray.data.read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
        selected_fields=("col_a",),
    )
    df = read_ds.to_pandas().sort_values("col_a").reset_index(drop=True)
    assert df["col_a"].tolist() == [1, 2, 3, 4]


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_write_overwrite_full_table():
    """Test overwriting an entire table."""
    import numpy as np
    import pandas as pd

    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")

    # Initial write
    initial_data = pd.DataFrame(
        {
            "col_a": np.array([1, 2, 3], dtype=np.int32),
            "col_b": ["a", "b", "c"],
            "col_c": np.array([1, 1, 1], dtype=np.int16),
        }
    )
    ray.data.from_pandas(initial_data).write_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
        mode="overwrite",
    )

    # Verify initial write
    df = (
        ray.data.read_iceberg(
            table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
            catalog_kwargs=_CATALOG_KWARGS.copy(),
        )
        .to_pandas()
        .sort_values("col_a")
        .reset_index(drop=True)
    )
    assert df["col_a"].tolist() == [1, 2, 3]

    # Overwrite with new data
    new_data = pd.DataFrame(
        {
            "col_a": np.array([10, 20], dtype=np.int32),
            "col_b": ["x", "y"],
            "col_c": np.array([2, 2], dtype=np.int16),
        }
    )
    ray.data.from_pandas(new_data).write_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
        mode="overwrite",
    )

    # Verify overwrite - should only have new data
    df = (
        ray.data.read_iceberg(
            table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
            catalog_kwargs=_CATALOG_KWARGS.copy(),
        )
        .to_pandas()
        .sort_values("col_a")
        .reset_index(drop=True)
    )
    assert df["col_a"].tolist() == [10, 20]


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_write_overwrite_with_filter():
    """Test overwriting specific partitions with a filter."""
    import numpy as np
    import pandas as pd

    pyi_catalog.load_catalog(**_CATALOG_KWARGS)

    # Initial write with multiple partitions
    initial_data = pd.DataFrame(
        {
            "col_a": np.array([1, 2, 3, 4, 5, 6], dtype=np.int32),
            "col_b": ["a", "b", "c", "d", "e", "f"],
            "col_c": np.array([1, 1, 2, 2, 3, 3], dtype=np.int16),
        }
    )
    ray.data.from_pandas(initial_data).write_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
        mode="overwrite",
    )

    # Overwrite only partition col_c=2
    new_data = pd.DataFrame(
        {
            "col_a": np.array([30, 40], dtype=np.int32),
            "col_b": ["x", "y"],
            "col_c": np.array([2, 2], dtype=np.int16),
        }
    )
    ray.data.from_pandas(new_data).write_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
        mode="overwrite",
        overwrite_filter=pyi_expr.EqualTo("col_c", 2),
    )

    # Verify: col_c=1 and col_c=3 should remain, col_c=2 should be replaced
    df = (
        ray.data.read_iceberg(
            table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
            catalog_kwargs=_CATALOG_KWARGS.copy(),
        )
        .to_pandas()
        .sort_values("col_a")
        .reset_index(drop=True)
    )
    assert sorted(df["col_a"].tolist()) == [1, 2, 5, 6, 30, 40]
    assert df[df["col_c"] == 2]["col_a"].tolist() == [30, 40]


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_write_merge_single_key():
    """Test merge/upsert with a single merge key."""
    import numpy as np
    import pandas as pd

    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")

    # Initial data
    initial_data = pd.DataFrame(
        {
            "col_a": np.array([1, 2, 3, 4], dtype=np.int32),
            "col_b": ["a", "b", "c", "d"],
            "col_c": np.array([1, 1, 2, 2], dtype=np.int16),
        }
    )
    ray.data.from_pandas(initial_data).write_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
        mode="overwrite",
    )

    # Merge new data - update row with col_a=2, insert col_a=5
    merge_data = pd.DataFrame(
        {
            "col_a": np.array([2, 5], dtype=np.int32),
            "col_b": ["updated", "new"],
            "col_c": np.array([1, 3], dtype=np.int16),
        }
    )
    ray.data.from_pandas(merge_data).write_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
        mode="merge",
        merge_keys=["col_a"],
    )

    # Verify results
    df = (
        ray.data.read_iceberg(
            table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
            catalog_kwargs=_CATALOG_KWARGS.copy(),
        )
        .to_pandas()
        .sort_values("col_a")
        .reset_index(drop=True)
    )

    assert df["col_a"].tolist() == [1, 2, 3, 4, 5]
    assert df[df["col_a"] == 2]["col_b"].iloc[0] == "updated"
    assert df[df["col_a"] == 5]["col_b"].iloc[0] == "new"


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_write_merge_multi_column_keys():
    """Test merge/upsert with multiple merge keys."""
    import numpy as np
    import pandas as pd

    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")

    # Initial data
    initial_data = pd.DataFrame(
        {
            "col_a": np.array([1, 1, 2, 2], dtype=np.int32),
            "col_b": ["a", "b", "a", "b"],
            "col_c": np.array([100, 200, 300, 400], dtype=np.int16),
        }
    )
    ray.data.from_pandas(initial_data).write_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
        mode="overwrite",
    )

    # Merge with multi-column key (col_a, col_b)
    merge_data = pd.DataFrame(
        {
            "col_a": np.array(
                [1, 3], dtype=np.int32
            ),  # Update (1, "a"), insert (3, "c")
            "col_b": ["a", "c"],
            "col_c": np.array([999, 500], dtype=np.int16),
        }
    )
    ray.data.from_pandas(merge_data).write_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
        mode="merge",
        merge_keys=["col_a", "col_b"],
    )

    # Verify results
    df = (
        ray.data.read_iceberg(
            table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
            catalog_kwargs=_CATALOG_KWARGS.copy(),
        )
        .to_pandas()
        .sort_values(["col_a", "col_b"])
        .reset_index(drop=True)
    )

    assert len(df) == 5
    # Row (1, "a") should be updated to 999
    assert df[(df["col_a"] == 1) & (df["col_b"] == "a")]["col_c"].iloc[0] == 999
    # Row (1, "b") should remain unchanged at 200
    assert df[(df["col_a"] == 1) & (df["col_b"] == "b")]["col_c"].iloc[0] == 200
    # Row (3, "c") should be newly inserted with 500
    assert df[(df["col_a"] == 3) & (df["col_b"] == "c")]["col_c"].iloc[0] == 500


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_write_merge_with_update_filter():
    """Test merge with update_filter to conditionally update rows."""
    import numpy as np
    import pandas as pd

    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")

    # Initial data
    initial_data = pd.DataFrame(
        {
            "col_a": np.array([1, 2, 3], dtype=np.int32),
            "col_b": ["a", "b", "c"],
            "col_c": np.array([10, 20, 30], dtype=np.int16),
        }
    )
    ray.data.from_pandas(initial_data).write_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
        mode="overwrite",
    )

    # Merge with update_filter: only update rows where col_c >= 20
    merge_data = pd.DataFrame(
        {
            "col_a": np.array([1, 2, 3], dtype=np.int32),
            "col_b": ["updated1", "updated2", "updated3"],
            "col_c": np.array([100, 200, 300], dtype=np.int16),
        }
    )
    ray.data.from_pandas(merge_data).write_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
        mode="merge",
        merge_keys=["col_a"],
        update_filter=pyi_expr.GreaterThanOrEqual("col_c", 20),
    )

    # Verify: col_a=1 should NOT be updated (col_c was 10, fails filter)
    # col_a=2 and col_a=3 should be updated (col_c was 20 and 30)
    df = (
        ray.data.read_iceberg(
            table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
            catalog_kwargs=_CATALOG_KWARGS.copy(),
        )
        .to_pandas()
        .sort_values("col_a")
        .reset_index(drop=True)
    )

    assert len(df) == 3
    # col_a=1 should have original value
    assert df[df["col_a"] == 1]["col_b"].iloc[0] == "a"
    assert df[df["col_a"] == 1]["col_c"].iloc[0] == 10
    # col_a=2 should be updated
    assert df[df["col_a"] == 2]["col_b"].iloc[0] == "updated2"
    assert df[df["col_a"] == 2]["col_c"].iloc[0] == 200
    # col_a=3 should be updated
    assert df[df["col_a"] == 3]["col_b"].iloc[0] == "updated3"
    assert df[df["col_a"] == 3]["col_c"].iloc[0] == 300


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
