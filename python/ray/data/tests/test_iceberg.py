import os
import random
from typing import Any, Dict, Generator, List, Tuple, Type

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from pkg_resources import parse_version
from pyiceberg import (
    catalog as pyi_catalog,
    expressions as pyi_expr,
    schema as pyi_schema,
    types as pyi_types,
)
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.table import Table
from pyiceberg.transforms import IdentityTransform

import ray
from ray._private.arrow_utils import get_pyarrow_version
from ray.data import read_iceberg
from ray.data._internal.datasource.iceberg_datasource import IcebergDatasource
from ray.data._internal.logical.operators.map_operator import Filter, Project
from ray.data._internal.logical.optimizers import LogicalOptimizer
from ray.data._internal.util import rows_same
from ray.data.expressions import col
from ray.data.tests.test_util import (
    get_operator_types as _get_operator_types,
    plan_has_operator as _has_operator_type,
)

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
def test_predicate_pushdown():
    """Test that predicate pushdown works correctly with Iceberg datasource."""
    # Read the table and apply filters using Ray Data expressions
    ds = ray.data.read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )

    # Apply filter using Ray Data expression syntax
    filtered_ds = ds.filter(expr=col("col_c") >= 5)

    # Verify the filter is pushed down to the read operation
    # by checking the optimized logical plan
    logical_plan = filtered_ds._plan._logical_plan
    optimized_plan = LogicalOptimizer().optimize(logical_plan)

    # The plan should only contain the Read operator, with no Filter operator
    # This indicates the filter was pushed down to the datasource
    assert not _has_operator_type(
        optimized_plan, Filter
    ), f"Filter should be pushed down to read, got operators: {_get_operator_types(optimized_plan)}"

    # Verify the results are correct
    result = filtered_ds.to_pandas()
    assert all(result["col_c"] >= 5), "All rows should have col_c >= 5"
    assert len(result) > 0, "Should have some results"

    # Verify against direct PyIceberg read with the same filter
    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    expected_table = (
        sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
        .scan(row_filter=pyi_expr.GreaterThanOrEqual("col_c", 5))
        .to_pandas()
    )

    assert rows_same(result, expected_table)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_predicate_pushdown_with_initial_filter():
    """Test that predicate pushdown works when combined with initial row_filter."""
    # Read with an initial PyIceberg filter
    initial_filter = pyi_expr.LessThan("col_a", 50)

    # Expect deprecation warning for row_filter
    with pytest.warns(DeprecationWarning, match="row_filter.*deprecated"):
        ds = ray.data.read_iceberg(
            table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
            row_filter=initial_filter,
            catalog_kwargs=_CATALOG_KWARGS.copy(),
        )

    # Apply additional filter using Ray Data expression
    filtered_ds = ds.filter(expr=col("col_c") >= 5)

    # Verify both filters are pushed down
    logical_plan = filtered_ds._plan._logical_plan
    optimized_plan = LogicalOptimizer().optimize(logical_plan)

    # No Filter operator should remain in the plan
    assert not _has_operator_type(
        optimized_plan, Filter
    ), f"Filters should be pushed down to read, got operators: {_get_operator_types(optimized_plan)}"

    # Verify the results satisfy both conditions
    result = filtered_ds.to_pandas()
    assert all(result["col_a"] < 50), "All rows should have col_a < 50"
    assert all(result["col_c"] >= 5), "All rows should have col_c >= 5"
    assert len(result) > 0, "Should have some results"

    # Verify against direct PyIceberg read with combined filter
    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    combined_filter = pyi_expr.And(
        pyi_expr.LessThan("col_a", 50), pyi_expr.GreaterThanOrEqual("col_c", 5)
    )
    expected_table = (
        sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
        .scan(row_filter=combined_filter)
        .to_pandas()
    )

    assert rows_same(result, expected_table)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_projection_pushdown():
    """Test that projection pushdown works correctly with Iceberg datasource."""
    # Read the table and apply projection using select
    ds = ray.data.read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )

    # Select only specific columns
    projected_ds = ds.select_columns(["col_a", "col_c"])

    # Verify the projection is pushed down to the read operation
    logical_plan = projected_ds._plan._logical_plan
    optimized_plan = LogicalOptimizer().optimize(logical_plan)

    # The plan should only contain the Read operator, with no Project operator
    # This indicates the projection was pushed down to the datasource
    assert not _has_operator_type(
        optimized_plan, Project
    ), f"Projection should be pushed down to read, got operators: {_get_operator_types(optimized_plan)}"

    # Verify the results only contain the selected columns
    result = projected_ds.to_pandas()
    assert set(result.columns) == {
        "col_a",
        "col_c",
    }, f"Expected only col_a and col_c, got: {result.columns}"
    assert len(result) > 0, "Should have some results"

    # Verify against direct PyIceberg read with the same projection
    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    expected_table = (
        sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
        .scan(selected_fields=("col_a", "col_c"))
        .to_pandas()
    )

    assert rows_same(result, expected_table)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
@pytest.mark.parametrize(
    "selected_cols,filter_expr,pyi_filter,expected_cols",
    [
        # Test 1: Projection only on col_a
        (["col_a"], None, None, {"col_a"}),
        # Test 2: Projection on col_a and col_b with filter on col_c
        (
            ["col_a", "col_b"],
            col("col_c") >= 5,
            pyi_expr.GreaterThanOrEqual("col_c", 5),
            {"col_a", "col_b"},
        ),
        # Test 3: Projection on all columns with filter
        (
            ["col_a", "col_b", "col_c"],
            col("col_a") < 50,
            pyi_expr.LessThan("col_a", 50),
            {"col_a", "col_b", "col_c"},
        ),
        # Test 4: Single column projection with complex filter
        (
            ["col_b"],
            (col("col_c") >= 3) & (col("col_c") <= 7),
            pyi_expr.And(
                pyi_expr.GreaterThanOrEqual("col_c", 3),
                pyi_expr.LessThanOrEqual("col_c", 7),
            ),
            {"col_b"},
        ),
    ],
)
def test_projection_and_predicate_pushdown(
    selected_cols, filter_expr, pyi_filter, expected_cols
):
    """Test that both projection and predicate pushdown work together."""
    # Read the table
    ds = ray.data.read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )

    # Apply projection
    projected_ds = ds.select_columns(selected_cols)

    # Apply filter if provided
    if filter_expr is not None:
        filtered_ds = projected_ds.filter(expr=filter_expr)
    else:
        filtered_ds = projected_ds

    # Verify both optimizations are applied
    logical_plan = filtered_ds._plan._logical_plan
    optimized_plan = LogicalOptimizer().optimize(logical_plan)

    # Both Filter and Project should be pushed down
    assert not _has_operator_type(
        optimized_plan, Filter
    ), f"Filter should be pushed down, got operators: {_get_operator_types(optimized_plan)}"
    assert not _has_operator_type(
        optimized_plan, Project
    ), f"Projection should be pushed down, got operators: {_get_operator_types(optimized_plan)}"

    # Verify the results
    result = filtered_ds.to_pandas()
    assert (
        set(result.columns) == expected_cols
    ), f"Expected columns {expected_cols}, got: {result.columns}"

    # Verify results match direct PyIceberg query
    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    table = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")

    if pyi_filter is not None:
        expected_table = table.scan(
            row_filter=pyi_filter, selected_fields=tuple(selected_cols)
        ).to_pandas()
    else:
        expected_table = table.scan(selected_fields=tuple(selected_cols)).to_pandas()

    assert rows_same(result, expected_table)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
@pytest.mark.parametrize(
    "rename_map,select_cols,filter_expr,pyi_filter,expected_cols",
    [
        # Test 1: Just rename
        (
            {"col_a": "column_a", "col_b": "column_b"},
            None,
            None,
            None,
            {"column_a", "column_b", "col_c"},
        ),
        # Test 2: Just select (no rename, no filter)
        (
            None,
            ["col_a", "col_c"],
            None,
            None,
            {"col_a", "col_c"},
        ),
        # Test 3: Just filter (no rename, no select)
        (
            None,
            None,
            col("col_c") >= 5,
            pyi_expr.GreaterThanOrEqual("col_c", 5),
            {"col_a", "col_b", "col_c"},
        ),
        # Test 4: Rename + Select
        (
            {"col_a": "column_a"},
            ["column_a", "col_b"],
            None,
            None,
            {"column_a", "col_b"},
        ),
        # Test 5: Rename + Filter
        (
            {"col_a": "column_a", "col_c": "column_c"},
            None,
            col("column_c") >= 5,
            pyi_expr.GreaterThanOrEqual("col_c", 5),
            {"column_a", "col_b", "column_c"},
        ),
        # Test 6: Select + Filter (different columns)
        (
            None,
            ["col_a", "col_b"],
            col("col_c") >= 5,
            pyi_expr.GreaterThanOrEqual("col_c", 5),
            {"col_a", "col_b"},
        ),
        # Test 7: Rename + Select + Filter (all three together)
        (
            {"col_a": "column_a", "col_b": "column_b"},
            ["column_a", "column_b"],
            col("col_c") >= 5,
            pyi_expr.GreaterThanOrEqual("col_c", 5),
            {"column_a", "column_b"},
        ),
        # Test 8: Complex rename + select with multiple renames
        (
            {"col_a": "id", "col_b": "name", "col_c": "value"},
            ["id", "value"],
            None,
            None,
            {"id", "value"},
        ),
        # Test 9: Rename + Select + Complex filter
        (
            {"col_a": "id", "col_c": "value"},
            ["id", "value"],
            (col("value") >= 3) & (col("value") <= 7),
            pyi_expr.And(
                pyi_expr.GreaterThanOrEqual("col_c", 3),
                pyi_expr.LessThanOrEqual("col_c", 7),
            ),
            {"id", "value"},
        ),
    ],
)
def test_rename_select_filter_combinations(
    rename_map, select_cols, filter_expr, pyi_filter, expected_cols
):
    """Test all combinations of rename_columns, select_columns, and filter operations."""
    # Read the table
    ds = ray.data.read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )

    # Apply rename if provided
    if rename_map is not None:
        ds = ds.rename_columns(rename_map)

    # Apply select if provided
    if select_cols is not None:
        ds = ds.select_columns(select_cols)

    # Apply filter if provided
    if filter_expr is not None:
        ds = ds.filter(expr=filter_expr)

    # Verify optimizations are applied
    logical_plan = ds._plan._logical_plan
    optimized_plan = LogicalOptimizer().optimize(logical_plan)

    # Both Filter and Project should be pushed down (when applicable)
    if filter_expr is not None:
        assert not _has_operator_type(
            optimized_plan, Filter
        ), f"Filter should be pushed down, got operators: {_get_operator_types(optimized_plan)}"
    if select_cols is not None or rename_map is not None:
        assert not _has_operator_type(
            optimized_plan, Project
        ), f"Projection should be pushed down, got operators: {_get_operator_types(optimized_plan)}"

    # Verify the results
    result = ds.to_pandas()
    assert (
        set(result.columns) == expected_cols
    ), f"Expected columns {expected_cols}, got: {result.columns}"
    assert len(result) > 0, "Should have some results"

    # Verify results match direct PyIceberg query
    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    table = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")

    # Build the column mapping for verification
    # We need to map renamed columns back to original names for PyIceberg
    reverse_rename_map = {}
    if rename_map is not None:
        reverse_rename_map = {v: k for k, v in rename_map.items()}

    # Determine which original columns to select
    if select_cols is not None:
        # Map selected columns back to original names
        original_cols = tuple(reverse_rename_map.get(col, col) for col in select_cols)
    else:
        # All columns
        original_cols = ("col_a", "col_b", "col_c")

    # Get expected data from PyIceberg
    if pyi_filter is not None:
        expected_table = table.scan(
            row_filter=pyi_filter, selected_fields=original_cols
        ).to_pandas()
    else:
        expected_table = table.scan(selected_fields=original_cols).to_pandas()

    # Apply renames to expected table to match result
    if rename_map is not None:
        cols_to_rename = {
            orig: new
            for orig, new in rename_map.items()
            if orig in expected_table.columns
        }
        expected_table = expected_table.rename(columns=cols_to_rename)

    assert rows_same(result, expected_table)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_predicate_pushdown_complex_expression():
    """Test predicate pushdown with complex expressions."""
    # Apply a complex filter expression
    ds = ray.data.read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )

    # Complex expression: (col_c >= 3) & (col_c <= 7) & (col_a <= 50)
    filtered_ds = (
        ds.filter(expr=(col("col_c") >= 3))
        .filter(expr=col("col_c") <= 7)
        .filter(expr=col("col_a") <= 50)
    )

    # Verify the results
    result = filtered_ds.to_pandas()

    # Verify optimizations are applied
    logical_plan = filtered_ds._plan._logical_plan
    optimized_plan = LogicalOptimizer().optimize(logical_plan)

    assert not _has_operator_type(
        optimized_plan, Filter
    ), f"Filter should be pushed down, got operators: {_get_operator_types(optimized_plan)}"
    assert not _has_operator_type(
        optimized_plan, Project
    ), f"Projection should be pushed down, got operators: {_get_operator_types(optimized_plan)}"

    assert all(result["col_c"] >= 3), "All rows should have col_c >= 3"
    assert all(result["col_c"] <= 7), "All rows should have col_c <= 7"
    assert all(result["col_a"] <= 50), "All rows should have col_a <= 50"
    assert len(result) > 0, "Should have some results"

    # Verify against direct PyIceberg read
    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    combined_filter = pyi_expr.And(
        pyi_expr.And(
            pyi_expr.GreaterThanOrEqual("col_c", 3),
            pyi_expr.LessThanOrEqual("col_c", 7),
        ),
        pyi_expr.LessThanOrEqual("col_a", 50),
    )
    expected_table = (
        sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
        .scan(row_filter=combined_filter)
        .to_pandas()
    )

    assert rows_same(result, expected_table)


# Helper functions and fixtures for schema evolution tests


@pytest.fixture
def clean_table() -> Generator[Tuple[Catalog, Table], None, None]:
    """Pytest fixture to get a clean Iceberg table by deleting all data."""
    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    table = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
    table.delete()
    yield sql_catalog, table


def _create_typed_dataframe(data_dict: Dict[str, List[Any]]) -> pd.DataFrame:
    """Create a pandas DataFrame with proper int32 dtypes for col_a and col_c."""
    df = pd.DataFrame(data_dict)
    if "col_a" in df.columns:
        # Use nullable Int32 to support NaN values
        df["col_a"] = df["col_a"].astype("Int32")
    if "col_c" in df.columns:
        # Use nullable Int32 to support NaN values
        df["col_c"] = df["col_c"].astype("Int32")
    return df


def _write_to_iceberg(df: pd.DataFrame, **kwargs: Any) -> None:
    """Write a DataFrame to the test Iceberg table."""
    ds = ray.data.from_pandas(df)
    write_kwargs: Dict[str, Any] = {
        "table_identifier": f"{_DB_NAME}.{_TABLE_NAME}",
        "catalog_kwargs": _CATALOG_KWARGS.copy(),
    }
    write_kwargs.update(kwargs)
    ds.write_iceberg(**write_kwargs)


def _read_from_iceberg(
    sort_by: "str | List[str] | None" = None,
) -> pd.DataFrame:
    """Read data from the test Iceberg table and optionally sort."""
    ds = ray.data.read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    result_df = ds.to_pandas()
    if sort_by:
        result_df = result_df.sort_values(sort_by).reset_index(drop=True)
    return result_df


def _verify_schema(expected_fields: Dict[str, Type[pyi_types.IcebergType]]) -> None:
    """
    Verify the Iceberg table schema matches expected fields.

    Args:
        expected_fields: Dict mapping field names to PyIceberg type classes
                        e.g., {"col_a": pyi_types.IntegerType, "col_b": pyi_types.StringType}
    """
    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    table = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
    schema = {field.name: field.field_type for field in table.schema().fields}

    assert len(schema) == len(
        expected_fields
    ), f"Expected {len(expected_fields)} fields, got {len(schema)}"

    for field_name, expected_type in expected_fields.items():
        assert field_name in schema, f"Field {field_name} not found in schema"
        assert isinstance(schema[field_name], expected_type), (
            f"Field {field_name} expected type {expected_type}, "
            f"got {type(schema[field_name])}"
        )


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
class TestSchemaEvolution:
    """Test schema evolution during writes."""

    def test_schema_evolution_add_column(self, clean_table):
        """Test adding new columns works."""
        initial_data = _create_typed_dataframe(
            {"col_a": [1, 2], "col_b": ["row_1", "row_2"], "col_c": [1, 2]}
        )
        _write_to_iceberg(initial_data)

        new_data = _create_typed_dataframe(
            {
                "col_a": [3, 4],
                "col_b": ["row_3", "row_4"],
                "col_c": [3, 4],
                "col_d": ["extra_3", "extra_4"],
            }
        )
        _write_to_iceberg(new_data)

        _verify_schema(
            {
                "col_a": pyi_types.IntegerType,
                "col_b": pyi_types.StringType,
                "col_c": pyi_types.IntegerType,
                "col_d": pyi_types.StringType,
            }
        )

        result_df = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4],
                "col_b": ["row_1", "row_2", "row_3", "row_4"],
                "col_c": [1, 2, 3, 4],
                "col_d": [None, None, "extra_3", "extra_4"],
            }
        )
        assert rows_same(result_df, expected)

    def test_multiple_schema_evolutions(self, clean_table):
        """Test multiple sequential schema evolutions."""
        initial_data = _create_typed_dataframe(
            {"col_a": [1], "col_b": ["row_1"], "col_c": [10]}
        )
        _write_to_iceberg(initial_data)

        # First evolution: add col_d
        data_with_d = _create_typed_dataframe(
            {"col_a": [2], "col_b": ["row_2"], "col_c": [20], "col_d": ["extra_2"]}
        )
        _write_to_iceberg(data_with_d)

        _verify_schema(
            {
                "col_a": pyi_types.IntegerType,
                "col_b": pyi_types.StringType,
                "col_c": pyi_types.IntegerType,
                "col_d": pyi_types.StringType,
            }
        )

        # Second evolution: add col_e
        data_with_e = _create_typed_dataframe(
            {
                "col_a": [3],
                "col_b": ["row_3"],
                "col_c": [30],
                "col_d": ["extra_3"],
                "col_e": ["bonus_3"],
            }
        )
        _write_to_iceberg(data_with_e)

        _verify_schema(
            {
                "col_a": pyi_types.IntegerType,
                "col_b": pyi_types.StringType,
                "col_c": pyi_types.IntegerType,
                "col_d": pyi_types.StringType,
                "col_e": pyi_types.StringType,
            }
        )

        result_df = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3],
                "col_b": ["row_1", "row_2", "row_3"],
                "col_c": [10, 20, 30],
                "col_d": [None, "extra_2", "extra_3"],
                "col_e": [None, None, "bonus_3"],
            }
        )
        assert rows_same(result_df, expected)

    def test_column_order_independence(self, clean_table):
        """Test writing data with columns in different order works."""
        initial_data = _create_typed_dataframe(
            {"col_a": [1, 2], "col_b": ["row_1", "row_2"], "col_c": [1, 2]}
        )
        _write_to_iceberg(initial_data)

        # Append data with columns in different order
        reordered_data = _create_typed_dataframe(
            {"col_c": [3, 4], "col_a": [3, 4], "col_b": ["row_3", "row_4"]}
        )
        _write_to_iceberg(reordered_data)

        result_df = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4],
                "col_b": ["row_1", "row_2", "row_3", "row_4"],
                "col_c": [1, 2, 3, 4],
            }
        )
        assert rows_same(result_df, expected)

    @pytest.mark.parametrize(
        "initial_dtype,promoted_dtype,promoted_iceberg_type",
        [
            (np.int32, np.int64, pyi_types.LongType),
            (np.float32, np.float64, pyi_types.DoubleType),
        ],
        ids=["int32_to_int64", "float32_to_float64"],
    )
    def test_schema_evolution_type_promotion(
        self, clean_table, initial_dtype, promoted_dtype, promoted_iceberg_type
    ):
        """Test type promotion (int32 -> int64, float32 -> float64) works."""
        initial_data = _create_typed_dataframe(
            {"col_a": [1, 2], "col_b": ["a", "b"], "col_c": [1, 2]}
        )
        initial_data["col_new"] = np.array([10, 20], dtype=initial_dtype)
        _write_to_iceberg(initial_data)

        new_data = _create_typed_dataframe(
            {"col_a": [3, 4], "col_b": ["c", "d"], "col_c": [3, 4]}
        )
        new_data["col_new"] = np.array([30, 40], dtype=promoted_dtype)
        _write_to_iceberg(new_data)

        _verify_schema(
            {
                "col_a": pyi_types.IntegerType,
                "col_b": pyi_types.StringType,
                "col_c": pyi_types.IntegerType,
                "col_new": promoted_iceberg_type,
            }
        )

        result_df = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4],
                "col_b": ["a", "b", "c", "d"],
                "col_c": [1, 2, 3, 4],
            }
        )
        expected["col_new"] = np.array([10, 20, 30, 40], dtype=promoted_dtype)
        assert rows_same(result_df, expected)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
class TestOverwriteMode:
    """Test overwrite mode functionality."""

    def test_write_overwrite_full(self, clean_table):
        """Test full table overwrite replaces all data."""
        from ray.data import SaveMode

        # Write initial data
        initial_data = _create_typed_dataframe(
            {"col_a": [1, 2, 3], "col_b": ["a", "b", "c"], "col_c": [10, 20, 30]}
        )
        _write_to_iceberg(initial_data)

        # Verify initial write
        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {"col_a": [1, 2, 3], "col_b": ["a", "b", "c"], "col_c": [10, 20, 30]}
        )
        assert rows_same(result, expected)

        # Overwrite with new data
        new_data = _create_typed_dataframe(
            {"col_a": [100, 200], "col_b": ["x", "y"], "col_c": [1, 2]}
        )
        _write_to_iceberg(new_data, mode=SaveMode.OVERWRITE)

        # Verify overwrite replaced all data
        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {"col_a": [100, 200], "col_b": ["x", "y"], "col_c": [1, 2]}
        )
        assert rows_same(result, expected)

    def test_write_overwrite_with_filter(self, clean_table):
        """Test partial overwrite using a filter expression."""
        from ray.data import SaveMode

        # Write initial data with different col_c values
        initial_data = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4, 5, 6],
                "col_b": ["a", "b", "c", "d", "e", "f"],
                "col_c": [1, 1, 2, 2, 3, 3],
            }
        )
        _write_to_iceberg(initial_data)

        # Overwrite only rows where col_c == 2
        new_data = _create_typed_dataframe(
            {"col_a": [30, 40], "col_b": ["new_c", "new_d"], "col_c": [2, 2]}
        )
        _write_to_iceberg(
            new_data, mode=SaveMode.OVERWRITE, overwrite_filter=col("col_c") == 2
        )

        # Verify: rows with col_c == 2 should be replaced, others unchanged
        result = _read_from_iceberg(sort_by="col_a")

        # Expected: rows with col_c in {1, 3} unchanged, col_c == 2 rows replaced
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 5, 6, 30, 40],
                "col_b": ["a", "b", "e", "f", "new_c", "new_d"],
                "col_c": [1, 1, 3, 3, 2, 2],
            }
        )
        assert rows_same(result, expected)

    def test_write_overwrite_with_complex_filter(self, clean_table):
        """Test partial overwrite with a complex filter expression."""
        from ray.data import SaveMode

        # Write initial data
        initial_data = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4, 5],
                "col_b": ["a", "b", "c", "d", "e"],
                "col_c": [1, 2, 3, 4, 5],
            }
        )
        _write_to_iceberg(initial_data)

        # Overwrite rows where col_c >= 3 AND col_c <= 4
        new_data = _create_typed_dataframe(
            {"col_a": [30, 40], "col_b": ["new_c", "new_d"], "col_c": [3, 4]}
        )
        _write_to_iceberg(
            new_data,
            mode=SaveMode.OVERWRITE,
            overwrite_filter=(col("col_c") >= 3) & (col("col_c") <= 4),
        )

        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 5, 30, 40],
                "col_b": ["a", "b", "e", "new_c", "new_d"],
                "col_c": [1, 2, 5, 3, 4],
            }
        )
        assert rows_same(result, expected)

    def test_write_overwrite_empty_result(self, clean_table):
        """Test overwrite with filter that matches no rows just appends."""
        from ray.data import SaveMode

        # Write initial data
        initial_data = _create_typed_dataframe(
            {"col_a": [1, 2, 3], "col_b": ["a", "b", "c"], "col_c": [1, 2, 3]}
        )
        _write_to_iceberg(initial_data)

        # Overwrite with filter that matches nothing
        new_data = _create_typed_dataframe(
            {"col_a": [100], "col_b": ["new"], "col_c": [999]}
        )
        _write_to_iceberg(
            new_data, mode=SaveMode.OVERWRITE, overwrite_filter=col("col_c") == 999
        )

        # Original data should remain plus new data
        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 100],
                "col_b": ["a", "b", "c", "new"],
                "col_c": [1, 2, 3, 999],
            }
        )
        assert rows_same(result, expected)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
class TestUpsertMode:
    """Test upsert mode functionality."""

    def test_write_upsert_basic(self, clean_table):
        """Test basic upsert with explicit join columns."""
        from ray.data import SaveMode

        # Write initial data
        initial_data = _create_typed_dataframe(
            {"col_a": [1, 2, 3], "col_b": ["a", "b", "c"], "col_c": [10, 20, 30]}
        )
        _write_to_iceberg(initial_data)

        # Upsert: update col_a=2, insert col_a=4
        upsert_data = _create_typed_dataframe(
            {"col_a": [2, 4], "col_b": ["updated_b", "new_d"], "col_c": [25, 40]}
        )
        _write_to_iceberg(
            upsert_data, mode=SaveMode.UPSERT, upsert_kwargs={"join_cols": ["col_a"]}
        )

        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4],
                "col_b": ["a", "updated_b", "c", "new_d"],
                "col_c": [10, 25, 30, 40],
            }
        )
        assert rows_same(result, expected)

    def test_write_upsert_multiple_join_cols(self, clean_table):
        """Test upsert with multiple join columns."""
        from ray.data import SaveMode

        # Write initial data
        initial_data = _create_typed_dataframe(
            {
                "col_a": [1, 1, 2, 2],
                "col_b": ["a", "b", "a", "b"],
                "col_c": [10, 20, 30, 40],
            }
        )
        _write_to_iceberg(initial_data)

        # Upsert using (col_a, col_b) as composite key
        upsert_data = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3],
                "col_b": ["b", "a", "c"],
                "col_c": [25, 35, 50],  # Update (1,b) and (2,a), insert (3,c)
            }
        )
        _write_to_iceberg(
            upsert_data,
            mode=SaveMode.UPSERT,
            upsert_kwargs={"join_cols": ["col_a", "col_b"]},
        )

        result = _read_from_iceberg(sort_by=["col_a", "col_b"])
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 1, 2, 2, 3],
                "col_b": ["a", "b", "a", "b", "c"],
                "col_c": [10, 25, 35, 40, 50],
            }
        )
        assert rows_same(result, expected)

    def test_write_upsert_all_updates(self, clean_table):
        """Test upsert where all rows are updates (no inserts)."""
        from ray.data import SaveMode

        # Write initial data
        initial_data = _create_typed_dataframe(
            {"col_a": [1, 2, 3], "col_b": ["a", "b", "c"], "col_c": [10, 20, 30]}
        )
        _write_to_iceberg(initial_data)

        # Upsert all existing rows
        upsert_data = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3],
                "col_b": ["updated_a", "updated_b", "updated_c"],
                "col_c": [100, 200, 300],
            }
        )
        _write_to_iceberg(
            upsert_data, mode=SaveMode.UPSERT, upsert_kwargs={"join_cols": ["col_a"]}
        )

        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3],
                "col_b": ["updated_a", "updated_b", "updated_c"],
                "col_c": [100, 200, 300],
            }
        )
        assert rows_same(result, expected)

    def test_write_upsert_all_inserts(self, clean_table):
        """Test upsert where all rows are inserts (no updates)."""
        from ray.data import SaveMode

        # Write initial data
        initial_data = _create_typed_dataframe(
            {"col_a": [1, 2, 3], "col_b": ["a", "b", "c"], "col_c": [10, 20, 30]}
        )
        _write_to_iceberg(initial_data)

        # Upsert all new rows
        upsert_data = _create_typed_dataframe(
            {"col_a": [4, 5, 6], "col_b": ["d", "e", "f"], "col_c": [40, 50, 60]}
        )
        _write_to_iceberg(
            upsert_data, mode=SaveMode.UPSERT, upsert_kwargs={"join_cols": ["col_a"]}
        )

        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4, 5, 6],
                "col_b": ["a", "b", "c", "d", "e", "f"],
                "col_c": [10, 20, 30, 40, 50, 60],
            }
        )
        assert rows_same(result, expected)

    def test_write_upsert_empty_table(self, clean_table):
        """Test upsert into empty table is equivalent to insert."""
        from ray.data import SaveMode

        # Table is already empty from fixture
        # Upsert data into empty table
        upsert_data = _create_typed_dataframe(
            {"col_a": [1, 2, 3], "col_b": ["a", "b", "c"], "col_c": [10, 20, 30]}
        )
        _write_to_iceberg(
            upsert_data, mode=SaveMode.UPSERT, upsert_kwargs={"join_cols": ["col_a"]}
        )

        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {"col_a": [1, 2, 3], "col_b": ["a", "b", "c"], "col_c": [10, 20, 30]}
        )
        assert rows_same(result, expected)


@pytest.fixture
def table_with_identifier_fields() -> Generator[Tuple[Catalog, Table], None, None]:
    """Pytest fixture to create a table with identifier fields for upsert tests."""
    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)

    # Drop existing table if it exists
    identifier_table_name = "identifier_test"
    if (_DB_NAME, identifier_table_name) in sql_catalog.list_tables(_DB_NAME):
        sql_catalog.drop_table(f"{_DB_NAME}.{identifier_table_name}")

    # Create table with identifier fields (col_a is the key)
    table = sql_catalog.create_table(
        f"{_DB_NAME}.{identifier_table_name}",
        schema=pyi_schema.Schema(
            pyi_types.NestedField(
                field_id=1,
                name="col_a",
                field_type=pyi_types.IntegerType(),
                required=True,
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
            identifier_field_ids=[1],  # col_a is the identifier field
        ),
    )

    yield sql_catalog, table, identifier_table_name

    # Cleanup
    if (_DB_NAME, identifier_table_name) in sql_catalog.list_tables(_DB_NAME):
        sql_catalog.drop_table(f"{_DB_NAME}.{identifier_table_name}")


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
class TestUpsertWithIdentifierFields:
    """Test upsert using table's identifier fields."""

    def test_upsert_uses_table_identifier_fields(self, table_with_identifier_fields):
        """Test upsert without explicit join_cols uses table's identifier fields."""
        from ray.data import SaveMode

        sql_catalog, table, table_name = table_with_identifier_fields
        table_identifier = f"{_DB_NAME}.{table_name}"

        # Write initial data
        initial_data = _create_typed_dataframe(
            {"col_a": [1, 2, 3], "col_b": ["a", "b", "c"], "col_c": [10, 20, 30]}
        )
        ds = ray.data.from_pandas(initial_data)
        ds.write_iceberg(
            table_identifier=table_identifier,
            catalog_kwargs=_CATALOG_KWARGS.copy(),
        )

        # Upsert without join_cols - should use identifier fields from table schema
        upsert_data = _create_typed_dataframe(
            {"col_a": [2, 4], "col_b": ["updated_b", "new_d"], "col_c": [25, 40]}
        )
        ds_upsert = ray.data.from_pandas(upsert_data)
        ds_upsert.write_iceberg(
            table_identifier=table_identifier,
            catalog_kwargs=_CATALOG_KWARGS.copy(),
            mode=SaveMode.UPSERT,
        )

        # Read back and verify
        result = ray.data.read_iceberg(
            table_identifier=table_identifier,
            catalog_kwargs=_CATALOG_KWARGS.copy(),
        ).to_pandas()
        result = result.sort_values("col_a").reset_index(drop=True)

        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4],
                "col_b": ["a", "updated_b", "c", "new_d"],
                "col_c": [10, 25, 30, 40],
            }
        )
        assert rows_same(result, expected)

    def test_upsert_explicit_join_cols_override_identifier_fields(
        self, table_with_identifier_fields
    ):
        """Test that explicit join_cols override table's identifier fields."""
        from ray.data import SaveMode

        sql_catalog, table, table_name = table_with_identifier_fields
        table_identifier = f"{_DB_NAME}.{table_name}"

        # Write initial data
        initial_data = _create_typed_dataframe(
            {"col_a": [1, 2, 3], "col_b": ["a", "b", "c"], "col_c": [10, 20, 30]}
        )
        ds = ray.data.from_pandas(initial_data)
        ds.write_iceberg(
            table_identifier=table_identifier,
            catalog_kwargs=_CATALOG_KWARGS.copy(),
        )

        # Upsert with explicit join_cols on col_b instead of table's col_a identifier
        upsert_data = _create_typed_dataframe(
            {"col_a": [100, 200], "col_b": ["b", "d"], "col_c": [25, 40]}
        )
        ds_upsert = ray.data.from_pandas(upsert_data)
        ds_upsert.write_iceberg(
            table_identifier=table_identifier,
            catalog_kwargs=_CATALOG_KWARGS.copy(),
            mode=SaveMode.UPSERT,
            upsert_kwargs={"join_cols": ["col_b"]},
        )

        # Read back and verify - join on col_b means row with col_b="b" was updated
        result = ray.data.read_iceberg(
            table_identifier=table_identifier,
            catalog_kwargs=_CATALOG_KWARGS.copy(),
        ).to_pandas()
        result = result.sort_values("col_a").reset_index(drop=True)

        expected = _create_typed_dataframe(
            {
                "col_a": [1, 3, 100, 200],
                "col_b": ["a", "c", "b", "d"],
                "col_c": [10, 30, 25, 40],
            }
        )
        assert rows_same(result, expected)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
class TestUpsertAndOverwriteMissingJoinCols:
    """Test error handling for upsert without join columns."""

    def test_upsert_without_join_cols_or_identifier_fields_raises(self, clean_table):
        """Test that upsert without join_cols on table without identifier fields raises."""
        from ray.data import SaveMode

        # Write some initial data
        initial_data = _create_typed_dataframe(
            {"col_a": [1, 2], "col_b": ["a", "b"], "col_c": [10, 20]}
        )
        _write_to_iceberg(initial_data)

        # Upsert without join_cols should raise since table has no identifier fields
        upsert_data = _create_typed_dataframe(
            {"col_a": [2, 3], "col_b": ["updated_b", "c"], "col_c": [25, 30]}
        )

        with pytest.raises(ValueError, match="join_cols"):
            _write_to_iceberg(upsert_data, mode=SaveMode.UPSERT)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
class TestSchemaEvolutionWithModes:
    """Test schema evolution with different write modes."""

    def test_schema_evolution_with_overwrite(self, clean_table):
        """Test schema evolution works with overwrite mode."""
        from ray.data import SaveMode

        # Write initial data
        initial_data = _create_typed_dataframe(
            {"col_a": [1, 2], "col_b": ["a", "b"], "col_c": [10, 20]}
        )
        _write_to_iceberg(initial_data)

        # Overwrite with data that has a new column
        new_data = _create_typed_dataframe(
            {"col_a": [3, 4], "col_b": ["c", "d"], "col_c": [30, 40]}
        )
        new_data["col_d"] = ["extra_3", "extra_4"]
        _write_to_iceberg(new_data, mode=SaveMode.OVERWRITE)

        # Verify schema evolved
        _verify_schema(
            {
                "col_a": pyi_types.IntegerType,
                "col_b": pyi_types.StringType,
                "col_c": pyi_types.IntegerType,
                "col_d": pyi_types.StringType,
            }
        )

        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {"col_a": [3, 4], "col_b": ["c", "d"], "col_c": [30, 40]}
        )
        expected["col_d"] = ["extra_3", "extra_4"]
        assert rows_same(result, expected)

    def test_schema_evolution_with_upsert(self, clean_table):
        """Test schema evolution works with upsert mode."""
        from ray.data import SaveMode

        # Write initial data
        initial_data = _create_typed_dataframe(
            {"col_a": [1, 2], "col_b": ["a", "b"], "col_c": [10, 20]}
        )
        _write_to_iceberg(initial_data)

        # Upsert with data that has a new column
        upsert_data = _create_typed_dataframe(
            {"col_a": [2, 3], "col_b": ["updated_b", "c"], "col_c": [25, 30]}
        )
        upsert_data["col_d"] = ["extra_2", "extra_3"]
        _write_to_iceberg(
            upsert_data, mode=SaveMode.UPSERT, upsert_kwargs={"join_cols": ["col_a"]}
        )

        # Verify schema evolved
        _verify_schema(
            {
                "col_a": pyi_types.IntegerType,
                "col_b": pyi_types.StringType,
                "col_c": pyi_types.IntegerType,
                "col_d": pyi_types.StringType,
            }
        )

        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3],
                "col_b": ["a", "updated_b", "c"],
                "col_c": [10, 25, 30],
                "col_d": [None, "extra_2", "extra_3"],
            }
        )
        assert rows_same(result, expected)

    def test_schema_evolution_with_partial_overwrite(self, clean_table):
        """Test schema evolution works with partial overwrite."""
        from ray.data import SaveMode

        # Write initial data
        initial_data = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4],
                "col_b": ["a", "b", "c", "d"],
                "col_c": [1, 1, 2, 2],
            }
        )
        _write_to_iceberg(initial_data)

        # Partial overwrite with new column for col_c == 2
        overwrite_data = _create_typed_dataframe(
            {"col_a": [30, 40], "col_b": ["new_c", "new_d"], "col_c": [2, 2]}
        )
        overwrite_data["col_d"] = ["extra_c", "extra_d"]
        _write_to_iceberg(
            overwrite_data,
            mode=SaveMode.OVERWRITE,
            overwrite_filter=col("col_c") == 2,
        )

        # Verify schema evolved
        _verify_schema(
            {
                "col_a": pyi_types.IntegerType,
                "col_b": pyi_types.StringType,
                "col_c": pyi_types.IntegerType,
                "col_d": pyi_types.StringType,
            }
        )

        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 30, 40],
                "col_b": ["a", "b", "new_c", "new_d"],
                "col_c": [1, 1, 2, 2],
                "col_d": [None, None, "extra_c", "extra_d"],
            }
        )
        assert rows_same(result, expected)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
