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

    expected_schema = pa.schema(
        [pa.field("col_a", pa.int32()), pa.field("col_b", pa.string())]
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
def test_append_basic():
    """Test basic APPEND mode - add new rows without schema changes."""
    import numpy as np
    import pandas as pd

    from ray.data import SaveMode

    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    table = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
    table.delete()

    # Write initial data
    initial_data = pd.DataFrame(
        {
            "col_a": np.array([1, 2], dtype=np.int32),
            "col_b": ["row_1", "row_2"],
            "col_c": np.array([1, 2], dtype=np.int32),
        }
    )
    ds_initial = ray.data.from_pandas(initial_data)
    ds_initial.write_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )

    # Verify initial schema
    table_before = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
    schema_before = {
        field.name: field.field_type for field in table_before.schema().fields
    }
    assert len(schema_before) == 3
    assert set(schema_before.keys()) == {"col_a", "col_b", "col_c"}

    # Append more data with same schema
    append_data = pd.DataFrame(
        {
            "col_a": np.array([3, 4], dtype=np.int32),
            "col_b": ["row_3", "row_4"],
            "col_c": np.array([3, 4], dtype=np.int32),
        }
    )
    ds_append = ray.data.from_pandas(append_data)
    ds_append.write_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
        mode=SaveMode.APPEND,
    )

    # Verify data after append
    read_ds = ray.data.read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    result_df = read_ds.to_pandas().sort_values("col_a").reset_index(drop=True)

    # Expected: all rows present (original + appended)
    expected_data = [
        {"col_a": 1, "col_b": "row_1", "col_c": 1},
        {"col_a": 2, "col_b": "row_2", "col_c": 2},
        {"col_a": 3, "col_b": "row_3", "col_c": 3},
        {"col_a": 4, "col_b": "row_4", "col_c": 4},
    ]
    expected_df = pd.DataFrame(expected_data)
    expected_df["col_a"] = expected_df["col_a"].astype(np.int32)
    expected_df["col_c"] = expected_df["col_c"].astype(np.int32)

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)

    # Verify schema remains unchanged
    table_after = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
    schema_after = {
        field.name: field.field_type for field in table_after.schema().fields
    }

    assert len(schema_after) == 3, f"Expected 3 fields, got {len(schema_after)}"
    assert "col_a" in schema_after and isinstance(
        schema_after["col_a"], pyi_types.IntegerType
    )
    assert "col_b" in schema_after and isinstance(
        schema_after["col_b"], pyi_types.StringType
    )
    assert "col_c" in schema_after and isinstance(
        schema_after["col_c"], pyi_types.IntegerType
    )


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_upsert_basic():
    """Test basic upsert functionality - update existing rows and insert new ones."""
    import numpy as np
    import pandas as pd

    from ray.data import SaveMode

    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    table = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
    table.delete()

    # Write initial data
    initial_data = pd.DataFrame(
        {
            "col_a": np.array([1, 2, 3], dtype=np.int32),
            "col_b": ["initial_1", "initial_2", "initial_3"],
            "col_c": np.array([1, 2, 3], dtype=np.int32),
        }
    )
    ds_initial = ray.data.from_pandas(initial_data)
    ds_initial.write_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )

    # Verify initial write
    read_ds = ray.data.read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    result_df = read_ds.to_pandas().sort_values("col_a").reset_index(drop=True)
    assert result_df["col_a"].tolist() == [1, 2, 3]
    assert result_df["col_b"].tolist() == ["initial_1", "initial_2", "initial_3"]

    # Upsert: update rows 2 and 3, insert row 4
    upsert_data = pd.DataFrame(
        {
            "col_a": np.array([2, 3, 4], dtype=np.int32),
            "col_b": ["updated_2", "updated_3", "new_4"],
            "col_c": np.array([2, 3, 4], dtype=np.int32),
        }
    )
    ds_upsert = ray.data.from_pandas(upsert_data)
    ds_upsert.write_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
        mode=SaveMode.UPSERT,
        upsert_kwargs={"join_cols": ["col_a"]},
    )

    # Verify upsert results
    read_ds_after = ray.data.read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    result_df_after = (
        read_ds_after.to_pandas().sort_values("col_a").reset_index(drop=True)
    )

    # Expected data after upsert: row 1 unchanged, rows 2 & 3 updated, row 4 inserted
    expected_df = pd.DataFrame(
        {
            "col_a": np.array([1, 2, 3, 4], dtype=np.int32),
            "col_b": ["initial_1", "updated_2", "updated_3", "new_4"],
            "col_c": np.array([1, 2, 3, 4], dtype=np.int32),
        }
    )
    pd.testing.assert_frame_equal(
        result_df_after[["col_a", "col_b", "col_c"]], expected_df, check_dtype=False
    )


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_upsert_composite_key():
    """Test upsert with composite key (multiple identifier fields)."""
    import numpy as np
    import pandas as pd

    from ray.data import SaveMode

    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    table = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
    table.delete()

    # Write initial data
    initial_data = pd.DataFrame(
        {
            "col_a": np.array([1, 1, 2, 2], dtype=np.int32),
            "col_b": ["A", "B", "A", "B"],
            "col_c": np.array([10, 20, 30, 40], dtype=np.int32),
        }
    )
    ds_initial = ray.data.from_pandas(initial_data)
    ds_initial.write_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )

    # Upsert using composite key (col_a, col_b)
    # Update (1, "B") and (2, "A"), insert (3, "A")
    upsert_data = pd.DataFrame(
        {
            "col_a": np.array([1, 2, 3], dtype=np.int32),
            "col_b": ["B", "A", "A"],
            "col_c": np.array([999, 888, 777], dtype=np.int32),
        }
    )
    ds_upsert = ray.data.from_pandas(upsert_data)
    ds_upsert.write_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
        mode=SaveMode.UPSERT,
        upsert_kwargs={"join_cols": ["col_a", "col_b"]},
    )

    # Verify results
    read_ds = ray.data.read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    result_df = (
        read_ds.to_pandas().sort_values(["col_a", "col_b"]).reset_index(drop=True)
    )

    # Expected data after upsert with composite key
    expected_data = [
        {"col_a": 1, "col_b": "A", "col_c": 10},  # Unchanged
        {"col_a": 1, "col_b": "B", "col_c": 999},  # Updated
        {"col_a": 2, "col_b": "A", "col_c": 888},  # Updated
        {"col_a": 2, "col_b": "B", "col_c": 40},  # Unchanged
        {"col_a": 3, "col_b": "A", "col_c": 777},  # Inserted
    ]
    expected_df = pd.DataFrame(expected_data)
    expected_df["col_a"] = expected_df["col_a"].astype(np.int32)
    expected_df["col_c"] = expected_df["col_c"].astype(np.int32)

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_overwrite_full_table():
    """Test full table overwrite - replace all data."""
    import numpy as np
    import pandas as pd

    from ray.data import SaveMode

    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    table = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
    table.delete()

    # Write initial data
    initial_data = pd.DataFrame(
        {
            "col_a": np.array([1, 2, 3, 4, 5], dtype=np.int32),
            "col_b": ["old_1", "old_2", "old_3", "old_4", "old_5"],
            "col_c": np.array([1, 2, 3, 4, 5], dtype=np.int32),
        }
    )
    ds_initial = ray.data.from_pandas(initial_data)
    ds_initial.write_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )

    # Verify initial data
    read_ds = ray.data.read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    assert len(read_ds.to_pandas()) == 5

    # Overwrite entire table with new data
    new_data = pd.DataFrame(
        {
            "col_a": np.array([10, 20, 30], dtype=np.int32),
            "col_b": ["new_10", "new_20", "new_30"],
            "col_c": np.array([100, 200, 300], dtype=np.int32),
        }
    )
    ds_overwrite = ray.data.from_pandas(new_data)
    ds_overwrite.write_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
        mode=SaveMode.OVERWRITE,
    )

    # Verify all old data is replaced
    read_ds_after = ray.data.read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    result_df = read_ds_after.to_pandas().sort_values("col_a").reset_index(drop=True)

    # Expected: completely new data, all old data gone
    expected_df = pd.DataFrame(
        {
            "col_a": np.array([10, 20, 30], dtype=np.int32),
            "col_b": ["new_10", "new_20", "new_30"],
            "col_c": np.array([100, 200, 300], dtype=np.int32),
        }
    )
    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)

    # Verify schema remains unchanged (no new columns added)
    table_after = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
    schema_after = {
        field.name: field.field_type for field in table_after.schema().fields
    }

    assert len(schema_after) == 3, f"Expected 3 fields, got {len(schema_after)}"
    assert "col_a" in schema_after and isinstance(
        schema_after["col_a"], pyi_types.IntegerType
    )
    assert "col_b" in schema_after and isinstance(
        schema_after["col_b"], pyi_types.StringType
    )
    assert "col_c" in schema_after and isinstance(
        schema_after["col_c"], pyi_types.IntegerType
    )


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_overwrite_with_filter():
    """Test partial overwrite using filter expression - replace only matching rows."""
    import numpy as np
    import pandas as pd

    from ray.data import SaveMode
    from ray.data.expressions import col

    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    table = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
    table.delete()

    # Write initial data with different col_c values
    initial_data = pd.DataFrame(
        {
            "col_a": np.array([1, 2, 3, 4, 5], dtype=np.int32),
            "col_b": ["data_1", "data_2", "data_3", "data_4", "data_5"],
            "col_c": np.array([1, 1, 2, 2, 3], dtype=np.int32),
        }
    )
    ds_initial = ray.data.from_pandas(initial_data)
    ds_initial.write_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )

    # Partial overwrite: only replace rows where col_c == 2
    overwrite_data = pd.DataFrame(
        {
            "col_a": np.array([10, 20], dtype=np.int32),
            "col_b": ["replaced_10", "replaced_20"],
            "col_c": np.array([2, 2], dtype=np.int32),
        }
    )
    ds_overwrite = ray.data.from_pandas(overwrite_data)
    ds_overwrite.write_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
        mode=SaveMode.OVERWRITE,
        overwrite_filter=col("col_c") == 2,
    )

    # Verify partial overwrite
    read_ds_after = ray.data.read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    result_df = read_ds_after.to_pandas().sort_values("col_a").reset_index(drop=True)

    # Expected: rows with col_c=1,3 preserved; rows with col_c=2 replaced
    expected_data = [
        {"col_a": 1, "col_b": "data_1", "col_c": 1},  # Preserved (col_c=1)
        {"col_a": 2, "col_b": "data_2", "col_c": 1},  # Preserved (col_c=1)
        {"col_a": 5, "col_b": "data_5", "col_c": 3},  # Preserved (col_c=3)
        {"col_a": 10, "col_b": "replaced_10", "col_c": 2},  # Replaced (col_c=2)
        {"col_a": 20, "col_b": "replaced_20", "col_c": 2},  # Replaced (col_c=2)
    ]
    expected_df = pd.DataFrame(expected_data)
    expected_df["col_a"] = expected_df["col_a"].astype(np.int32)
    expected_df["col_c"] = expected_df["col_c"].astype(np.int32)

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)

    # Verify schema remains unchanged (no new columns added)
    table_after = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
    schema_after = {
        field.name: field.field_type for field in table_after.schema().fields
    }

    assert len(schema_after) == 3, f"Expected 3 fields, got {len(schema_after)}"
    assert "col_a" in schema_after and isinstance(
        schema_after["col_a"], pyi_types.IntegerType
    )
    assert "col_b" in schema_after and isinstance(
        schema_after["col_b"], pyi_types.StringType
    )
    assert "col_c" in schema_after and isinstance(
        schema_after["col_c"], pyi_types.IntegerType
    )


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_overwrite_with_schema_evolution():
    """Test schema evolution with OVERWRITE mode - automatically add columns to existing table."""
    import numpy as np
    import pandas as pd

    from ray.data import SaveMode

    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    table = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
    table.delete()

    # Write initial data with 3 columns
    initial_data = pd.DataFrame(
        {
            "col_a": np.array([1, 2, 3], dtype=np.int32),
            "col_b": ["row_1", "row_2", "row_3"],
            "col_c": np.array([1, 2, 3], dtype=np.int32),
        }
    )
    ds_initial = ray.data.from_pandas(initial_data)
    ds_initial.write_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )

    # Verify initial schema has 3 columns
    table_before = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
    schema_before = {
        field.name: field.field_type for field in table_before.schema().fields
    }
    assert len(schema_before) == 3
    assert set(schema_before.keys()) == {"col_a", "col_b", "col_c"}

    # Overwrite with new column "col_d" - schema evolution happens automatically
    overwrite_data = pd.DataFrame(
        {
            "col_a": np.array([10, 20], dtype=np.int32),
            "col_b": ["new_10", "new_20"],
            "col_c": np.array([100, 200], dtype=np.int32),
            "col_d": ["extra_10", "extra_20"],  # New column - automatically added
        }
    )
    ds_overwrite = ray.data.from_pandas(overwrite_data)
    ds_overwrite.write_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
        mode=SaveMode.OVERWRITE,
    )

    # Verify data after overwrite
    read_ds = ray.data.read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    result_df = read_ds.to_pandas().sort_values("col_a").reset_index(drop=True)

    # Expected: only new data (old data replaced), with col_d values
    expected_data = [
        {"col_a": 10, "col_b": "new_10", "col_c": 100, "col_d": "extra_10"},
        {"col_a": 20, "col_b": "new_20", "col_c": 200, "col_d": "extra_20"},
    ]
    expected_df = pd.DataFrame(expected_data)
    expected_df["col_a"] = expected_df["col_a"].astype(np.int32)
    expected_df["col_c"] = expected_df["col_c"].astype(np.int32)

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)

    # Verify schema was updated with new column
    table_after = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
    schema_after = {
        field.name: field.field_type for field in table_after.schema().fields
    }

    assert (
        len(schema_after) == 4
    ), f"Expected 4 fields after schema evolution, got {len(schema_after)}"
    assert "col_a" in schema_after and isinstance(
        schema_after["col_a"], pyi_types.IntegerType
    )
    assert "col_b" in schema_after and isinstance(
        schema_after["col_b"], pyi_types.StringType
    )
    assert "col_c" in schema_after and isinstance(
        schema_after["col_c"], pyi_types.IntegerType
    )
    assert "col_d" in schema_after and isinstance(
        schema_after["col_d"], pyi_types.StringType
    ), "col_d should have been added as StringType"


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_append_with_schema_evolution():
    """Test schema evolution with UPSERT mode - automatically add columns to existing table."""
    import numpy as np
    import pandas as pd

    from ray.data import SaveMode

    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    table = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
    table.delete()

    # Write initial data with 3 columns
    initial_data = pd.DataFrame(
        {
            "col_a": np.array([1, 2], dtype=np.int32),
            "col_b": ["row_1", "row_2"],
            "col_c": np.array([1, 2], dtype=np.int32),
        }
    )
    ds_initial = ray.data.from_pandas(initial_data)
    ds_initial.write_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )

    # Upsert with new column "col_d" - schema evolution happens automatically
    # Note: APPEND mode uses transaction API which doesn't support schema evolution
    # Use UPSERT mode for schema evolution with new rows
    append_data = pd.DataFrame(
        {
            "col_a": np.array([3, 4], dtype=np.int32),
            "col_b": ["row_3", "row_4"],
            "col_c": np.array([3, 4], dtype=np.int32),
            "col_d": ["extra_3", "extra_4"],  # New column - automatically added
        }
    )
    ds_append = ray.data.from_pandas(append_data)
    ds_append.write_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
        mode=SaveMode.UPSERT,
        upsert_kwargs={"join_cols": ["col_a"]},  # Use UPSERT for schema evolution
    )

    # Verify schema evolution and data
    read_ds = ray.data.read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    result_df = read_ds.to_pandas().sort_values("col_a").reset_index(drop=True)

    # Expected: original rows with null col_d, new rows with col_d values
    expected_data = [
        {"col_a": 1, "col_b": "row_1", "col_c": 1, "col_d": None},
        {"col_a": 2, "col_b": "row_2", "col_c": 2, "col_d": None},
        {"col_a": 3, "col_b": "row_3", "col_c": 3, "col_d": "extra_3"},
        {"col_a": 4, "col_b": "row_4", "col_c": 4, "col_d": "extra_4"},
    ]
    expected_df = pd.DataFrame(expected_data)
    expected_df["col_a"] = expected_df["col_a"].astype(np.int32)
    expected_df["col_c"] = expected_df["col_c"].astype(np.int32)

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)

    # Explicitly verify that the schema was updated in the Iceberg table
    table_after_upsert = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
    actual_schema = table_after_upsert.schema()

    # Verify schema evolution: check field names and types
    schema_fields = {field.name: field.field_type for field in actual_schema.fields}

    assert len(schema_fields) == 4, f"Expected 4 fields, got {len(schema_fields)}"
    assert "col_a" in schema_fields and isinstance(
        schema_fields["col_a"], pyi_types.IntegerType
    )
    assert "col_b" in schema_fields and isinstance(
        schema_fields["col_b"], pyi_types.StringType
    )
    assert "col_c" in schema_fields and isinstance(
        schema_fields["col_c"], pyi_types.IntegerType
    )
    assert "col_d" in schema_fields and isinstance(
        schema_fields["col_d"], pyi_types.StringType
    ), "col_d should have been added as StringType"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
