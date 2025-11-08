import os
import random

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
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import IdentityTransform

import ray
from ray._private.arrow_utils import get_pyarrow_version
from ray.data import read_iceberg
from ray.data._internal.datasource.iceberg_datasource import IcebergDatasource
from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data._internal.logical.operators.map_operator import Filter, Project
from ray.data._internal.logical.optimizers import LogicalOptimizer
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
