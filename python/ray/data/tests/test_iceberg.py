import os
import random
from typing import Any, Dict, Generator, List, Optional, Tuple, Type, Union

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
from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data._internal.logical.operators.map_operator import Filter, Project
from ray.data._internal.logical.optimizers import LogicalOptimizer
from ray.data._internal.savemode import SaveMode
from ray.data._internal.util import rows_same
from ray.data.expressions import col

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


def _get_operator_types(plan: "LogicalPlan") -> list:
    return [type(op).__name__ for op in plan.dag.post_order_iter()]


def _has_operator_type(plan: "LogicalPlan", operator_class: type) -> bool:
    return any(isinstance(op, operator_class) for op in plan.dag.post_order_iter())


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


# Helper functions and fixtures for write mode tests
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
        df["col_a"] = df["col_a"].astype(np.int32)
    if "col_c" in df.columns:
        df["col_c"] = df["col_c"].astype(np.int32)
    return df


def _write_to_iceberg(
    df: pd.DataFrame, mode: Optional[SaveMode] = None, **kwargs: Any
) -> None:
    """Write a DataFrame to the test Iceberg table."""
    ds = ray.data.from_pandas(df)
    write_kwargs: Dict[str, Any] = {
        "table_identifier": f"{_DB_NAME}.{_TABLE_NAME}",
        "catalog_kwargs": _CATALOG_KWARGS.copy(),
    }
    if mode is not None:
        write_kwargs["mode"] = mode
    write_kwargs.update(kwargs)
    ds.write_iceberg(**write_kwargs)


def _read_from_iceberg(sort_by: Optional[Union[str, List[str]]] = None) -> pd.DataFrame:
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
class TestBasicWriteModes:
    """Test basic write operations for APPEND, UPSERT, and OVERWRITE modes."""

    def test_append_basic(self, clean_table):
        """Test basic APPEND mode - add new rows without schema changes."""
        initial_data = _create_typed_dataframe(
            {"col_a": [1, 2], "col_b": ["row_1", "row_2"], "col_c": [1, 2]}
        )
        _write_to_iceberg(initial_data)

        append_data = _create_typed_dataframe(
            {"col_a": [3, 4], "col_b": ["row_3", "row_4"], "col_c": [3, 4]}
        )
        _write_to_iceberg(append_data, mode=SaveMode.APPEND)

        result_df = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4],
                "col_b": ["row_1", "row_2", "row_3", "row_4"],
                "col_c": [1, 2, 3, 4],
            }
        )
        assert rows_same(result_df, expected)

    def test_upsert_basic(self, clean_table):
        """Test basic upsert - update existing rows and insert new ones."""
        initial_data = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3],
                "col_b": ["initial_1", "initial_2", "initial_3"],
                "col_c": [1, 2, 3],
            }
        )
        _write_to_iceberg(initial_data)

        upsert_data = _create_typed_dataframe(
            {
                "col_a": [2, 3, 4],
                "col_b": ["updated_2", "updated_3", "new_4"],
                "col_c": [2, 3, 4],
            }
        )
        _write_to_iceberg(
            upsert_data, mode=SaveMode.UPSERT, upsert_kwargs={"join_cols": ["col_a"]}
        )

        result_df = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4],
                "col_b": ["initial_1", "updated_2", "updated_3", "new_4"],
                "col_c": [1, 2, 3, 4],
            }
        )
        assert rows_same(result_df, expected)

    def test_upsert_composite_key(self, clean_table):
        """Test upsert with composite key (multiple identifier fields)."""
        initial_data = _create_typed_dataframe(
            {
                "col_a": [1, 1, 2, 2],
                "col_b": ["A", "B", "A", "B"],
                "col_c": [10, 20, 30, 40],
            }
        )
        _write_to_iceberg(initial_data)

        # Update (1, "B") and (2, "A"), insert (3, "A")
        upsert_data = _create_typed_dataframe(
            {"col_a": [1, 2, 3], "col_b": ["B", "A", "A"], "col_c": [999, 888, 777]}
        )
        _write_to_iceberg(
            upsert_data,
            mode=SaveMode.UPSERT,
            upsert_kwargs={"join_cols": ["col_a", "col_b"]},
        )

        result_df = _read_from_iceberg(sort_by=["col_a", "col_b"])
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 1, 2, 2, 3],
                "col_b": ["A", "B", "A", "B", "A"],
                "col_c": [10, 999, 888, 40, 777],
            }
        )
        assert rows_same(result_df, expected)

    def test_overwrite_full_table(self, clean_table):
        """Test full table overwrite - replace all data."""
        initial_data = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4, 5],
                "col_b": ["old_1", "old_2", "old_3", "old_4", "old_5"],
                "col_c": [1, 2, 3, 4, 5],
            }
        )
        _write_to_iceberg(initial_data)

        new_data = _create_typed_dataframe(
            {
                "col_a": [10, 20, 30],
                "col_b": ["new_10", "new_20", "new_30"],
                "col_c": [100, 200, 300],
            }
        )
        _write_to_iceberg(new_data, mode=SaveMode.OVERWRITE)

        result_df = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [10, 20, 30],
                "col_b": ["new_10", "new_20", "new_30"],
                "col_c": [100, 200, 300],
            }
        )
        assert rows_same(result_df, expected)

    def test_overwrite_with_filter(self, clean_table):
        """Test partial overwrite using filter expression."""
        initial_data = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4, 5],
                "col_b": ["data_1", "data_2", "data_3", "data_4", "data_5"],
                "col_c": [1, 1, 2, 2, 3],
            }
        )
        _write_to_iceberg(initial_data)

        # Replace only rows where col_c == 2
        overwrite_data = _create_typed_dataframe(
            {
                "col_a": [10, 20],
                "col_b": ["replaced_10", "replaced_20"],
                "col_c": [2, 2],
            }
        )
        _write_to_iceberg(
            overwrite_data, mode=SaveMode.OVERWRITE, overwrite_filter=col("col_c") == 2
        )

        result_df = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 5, 10, 20],
                "col_b": ["data_1", "data_2", "data_5", "replaced_10", "replaced_20"],
                "col_c": [1, 1, 3, 2, 2],
            }
        )
        assert rows_same(result_df, expected)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
class TestSchemaEvolution:
    """Test schema evolution across different write modes."""

    @pytest.mark.parametrize("mode", [SaveMode.APPEND, SaveMode.OVERWRITE])
    def test_schema_evolution_by_mode(self, clean_table, mode):
        """Test adding new columns works for APPEND and OVERWRITE."""
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
        _write_to_iceberg(new_data, mode=mode)

        _verify_schema(
            {
                "col_a": pyi_types.IntegerType,
                "col_b": pyi_types.StringType,
                "col_c": pyi_types.IntegerType,
                "col_d": pyi_types.StringType,
            }
        )

        result_df = _read_from_iceberg(sort_by="col_a")
        if mode == SaveMode.APPEND:
            expected = _create_typed_dataframe(
                {
                    "col_a": [1, 2, 3, 4],
                    "col_b": ["row_1", "row_2", "row_3", "row_4"],
                    "col_c": [1, 2, 3, 4],
                    "col_d": [None, None, "extra_3", "extra_4"],
                }
            )
        else:  # OVERWRITE
            expected = _create_typed_dataframe(
                {
                    "col_a": [3, 4],
                    "col_b": ["row_3", "row_4"],
                    "col_c": [3, 4],
                    "col_d": ["extra_3", "extra_4"],
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
        _write_to_iceberg(data_with_d, mode=SaveMode.APPEND)

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
        _write_to_iceberg(data_with_e, mode=SaveMode.APPEND)

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
        _write_to_iceberg(reordered_data, mode=SaveMode.APPEND)

        result_df = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4],
                "col_b": ["row_1", "row_2", "row_3", "row_4"],
                "col_c": [1, 2, 3, 4],
            }
        )
        assert rows_same(result_df, expected)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
class TestUpsertScenarios:
    """Test various upsert matching scenarios."""

    @pytest.mark.parametrize(
        "upsert_keys,upsert_col_b,upsert_col_c,expected_data",
        [
            # No matching rows (behaves like append)
            (
                [4, 5, 6],
                ["new_4", "new_5", "new_6"],
                [40, 50, 60],
                {
                    "col_a": [1, 2, 3, 4, 5, 6],
                    "col_b": ["row_1", "row_2", "row_3", "new_4", "new_5", "new_6"],
                    "col_c": [10, 20, 30, 40, 50, 60],
                },
            ),
            # All rows match (behaves like update)
            (
                [1, 2, 3],
                ["updated_1", "updated_2", "updated_3"],
                [100, 200, 300],
                {
                    "col_a": [1, 2, 3],
                    "col_b": ["updated_1", "updated_2", "updated_3"],
                    "col_c": [100, 200, 300],
                },
            ),
            # Partial match (mixed update and insert)
            (
                [2, 3, 4],
                ["updated_2", "updated_3", "new_4"],
                [200, 300, 40],
                {
                    "col_a": [1, 2, 3, 4],
                    "col_b": ["row_1", "updated_2", "updated_3", "new_4"],
                    "col_c": [10, 200, 300, 40],
                },
            ),
        ],
    )
    def test_upsert_matching_scenarios(
        self, clean_table, upsert_keys, upsert_col_b, upsert_col_c, expected_data
    ):
        """Test upsert with different row matching patterns."""
        initial_data = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3],
                "col_b": ["row_1", "row_2", "row_3"],
                "col_c": [10, 20, 30],
            }
        )
        _write_to_iceberg(initial_data)

        upsert_data = _create_typed_dataframe(
            {"col_a": upsert_keys, "col_b": upsert_col_b, "col_c": upsert_col_c}
        )
        _write_to_iceberg(
            upsert_data, mode=SaveMode.UPSERT, upsert_kwargs={"join_cols": ["col_a"]}
        )

        result_df = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(expected_data)
        assert rows_same(result_df, expected)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
class TestOverwriteScenarios:
    """Test various overwrite filter scenarios."""

    @pytest.mark.parametrize(
        "filter_expr,overwrite_col_c,expected_data",
        [
            # Filter matches nothing (behaves like append)
            (
                col("col_c") == 999,
                [999, 999],
                {
                    "col_a": [1, 2, 3, 4, 5],
                    "col_b": ["row_1", "row_2", "row_3", "row_4", "row_5"],
                    "col_c": [10, 20, 30, 999, 999],
                },
            ),
            # Filter matches some rows
            (
                col("col_c") >= 20,
                [200, 300],
                {
                    "col_a": [1, 4, 5],
                    "col_b": ["row_1", "row_4", "row_5"],
                    "col_c": [10, 200, 300],
                },
            ),
            # Filter matches all rows (full overwrite)
            (
                col("col_c") < 100,
                [40, 50],
                {
                    "col_a": [4, 5],
                    "col_b": ["row_4", "row_5"],
                    "col_c": [40, 50],
                },
            ),
        ],
    )
    def test_overwrite_filter_scenarios(
        self, clean_table, filter_expr, overwrite_col_c, expected_data
    ):
        """Test partial overwrite with different filter matching patterns."""
        initial_data = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3],
                "col_b": ["row_1", "row_2", "row_3"],
                "col_c": [10, 20, 30],
            }
        )
        _write_to_iceberg(initial_data)

        overwrite_data = _create_typed_dataframe(
            {"col_a": [4, 5], "col_b": ["row_4", "row_5"], "col_c": overwrite_col_c}
        )
        _write_to_iceberg(
            overwrite_data, mode=SaveMode.OVERWRITE, overwrite_filter=filter_expr
        )

        result_df = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(expected_data)
        assert rows_same(result_df, expected)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
class TestEdgeCases:
    """Test edge cases and special scenarios."""

    @pytest.mark.parametrize(
        "mode", [SaveMode.APPEND, SaveMode.UPSERT, SaveMode.OVERWRITE]
    )
    def test_write_empty_dataset(self, clean_table, mode):
        """Test writing empty dataset doesn't fail or modify table."""
        initial_data = _create_typed_dataframe(
            {"col_a": [1, 2], "col_b": ["row_1", "row_2"], "col_c": [1, 2]}
        )
        _write_to_iceberg(initial_data)

        empty_data = _create_typed_dataframe({"col_a": [], "col_b": [], "col_c": []})

        write_kwargs = {}
        if mode == SaveMode.UPSERT:
            write_kwargs["upsert_kwargs"] = {"join_cols": ["col_a"]}
        elif mode == SaveMode.OVERWRITE:
            write_kwargs["overwrite_filter"] = col("col_c") == 999

        _write_to_iceberg(empty_data, mode=mode, **write_kwargs)

        result_df = _read_from_iceberg(sort_by="col_a")
        assert rows_same(result_df, initial_data)

    def test_overwrite_empty_table(self, clean_table):
        """Test overwriting an empty table."""
        data = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3],
                "col_b": ["row_1", "row_2", "row_3"],
                "col_c": [10, 20, 30],
            }
        )
        _write_to_iceberg(data, mode=SaveMode.OVERWRITE)

        result_df = _read_from_iceberg(sort_by="col_a")
        assert rows_same(result_df, data)

    @pytest.mark.parametrize("mode", [SaveMode.APPEND, SaveMode.OVERWRITE])
    def test_snapshot_properties(self, clean_table, mode):
        """Test snapshot_properties are passed through for APPEND and OVERWRITE."""
        # Note: UPSERT doesn't support snapshot_properties in PyIceberg
        data = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3],
                "col_b": ["row_1", "row_2", "row_3"],
                "col_c": [10, 20, 30],
            }
        )

        snapshot_props = {"test_property": "test_value", "author": "ray_data_test"}

        ds = ray.data.from_pandas(data)
        ds.write_iceberg(
            table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
            catalog_kwargs=_CATALOG_KWARGS.copy(),
            mode=mode,
            snapshot_properties=snapshot_props,
        )

        sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
        table = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
        latest_snapshot = table.current_snapshot()

        assert latest_snapshot is not None
        assert latest_snapshot.summary.get("test_property") == "test_value"
        assert latest_snapshot.summary.get("author") == "ray_data_test"

    def test_mixed_mode_operations(self, clean_table):
        """Test mixing different write modes in sequence."""
        # Start with APPEND
        data1 = _create_typed_dataframe(
            {"col_a": [1, 2], "col_b": ["a", "b"], "col_c": [10, 20]}
        )
        _write_to_iceberg(data1, mode=SaveMode.APPEND)

        # Then UPSERT (update row 2, add row 3)
        data2 = _create_typed_dataframe(
            {"col_a": [2, 3], "col_b": ["updated_b", "c"], "col_c": [200, 30]}
        )
        _write_to_iceberg(
            data2, mode=SaveMode.UPSERT, upsert_kwargs={"join_cols": ["col_a"]}
        )

        # Then OVERWRITE with filter (deletes rows 2 and 3, adds rows 4 and 5)
        data3 = _create_typed_dataframe(
            {"col_a": [4, 5], "col_b": ["d", "e"], "col_c": [40, 50]}
        )
        _write_to_iceberg(
            data3, mode=SaveMode.OVERWRITE, overwrite_filter=col("col_c") >= 30
        )

        # Verify final state: rows 2 and 3 deleted, rows 4 and 5 added
        result_df = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {"col_a": [1, 4, 5], "col_b": ["a", "d", "e"], "col_c": [10, 40, 50]}
        )
        assert rows_same(result_df, expected)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
