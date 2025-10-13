"""Tests for Ray Data SQL API.

Tests cover:
- Basic SQL operations (SELECT, WHERE, JOIN, GROUP BY, etc.)
- Multi-dialect support via SQLGlot
- DataFusion optimization integration  
- Configuration via DataContext
- Error handling and fallback behavior
- Auto-discovery of datasets from variables
- Edge cases and validation

All tests are parametrized to run with both SQLGlot and DataFusion engines.
"""

import warnings

import pytest

import ray
from ray.data.experimental.sql import (
    ColumnNotFoundError,
    SQLError,
    SQLExecutionError,
    SQLParseError,
    TableNotFoundError,
    UnsupportedOperationError,
)
from ray.data.experimental.sql_api import (
    clear_tables,
    config,
    list_tables,
    register,
    sql,
)
from ray.tests.conftest import *  # noqa

# Module-level dataset for testing global variable discovery
global_test_dataset = None


# Basic SQL operations


def test_sql_auto_discovery(ray_start_regular_shared, sql_engine):
    """Test automatic dataset discovery from variables."""
    users = ray.data.from_items(  # noqa: F841
        [{"id": 1, "name": "Alice", "age": 30}, {"id": 2, "name": "Bob", "age": 25}]
    )

    # Auto-discovery should work without registration
    result = sql("SELECT * FROM users WHERE age > 25")
    rows = result.take_all()

    assert len(rows) == 1
    assert rows[0]["name"] == "Alice"


def test_sql_projection(ray_start_regular_shared, sql_engine):
    """Test SELECT column projection."""
    data = ray.data.from_items([{"x": 1, "y": 10}, {"x": 2, "y": 20}])  # noqa: F841

    result = sql("SELECT x FROM data")
    rows = result.take_all()

    assert len(rows) == 2
    assert all("x" in row for row in rows)
    assert all("y" not in row for row in rows)


def test_sql_filtering_and_sorting(ray_start_regular_shared, sql_engine):
    """Test WHERE and ORDER BY clauses."""
    scores = ray.data.from_items(  # noqa: F841
        [
            {"name": "Alice", "score": 95},
            {"name": "Bob", "score": 87},
            {"name": "Charlie", "score": 92},
        ]
    )

    result = sql("SELECT name, score FROM scores WHERE score > 90 ORDER BY score DESC")
    rows = result.take_all()

    assert len(rows) == 2
    assert rows[0]["name"] == "Alice"  # Highest score first
    assert rows[1]["name"] == "Charlie"


def test_sql_limit(ray_start_regular_shared, sql_engine):
    """Test LIMIT clause."""
    large_data = ray.data.range(100)  # noqa: F841

    result = sql("SELECT * FROM large_data LIMIT 5")
    rows = result.take_all()

    assert len(rows) == 5


# Join operations


def test_sql_join(ray_start_regular_shared, sql_engine):
    """Test basic JOIN operations."""
    users = ray.data.from_items(  # noqa: F841
        [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    )

    orders = ray.data.from_items(  # noqa: F841
        [{"user_id": 1, "amount": 100}, {"user_id": 2, "amount": 200}]
    )

    result = sql(
        "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id"
    )
    rows = result.take_all()

    assert len(rows) == 2
    assert any(row["name"] == "Alice" and row["amount"] == 100 for row in rows)


@pytest.mark.parametrize("join_type", ["INNER", "LEFT", "RIGHT"])
def test_sql_join_types(ray_start_regular_shared, join_type, sql_engine):
    """Test different JOIN types."""
    left_data = ray.data.from_items(  # noqa: F841
        [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]
    )

    right_data = ray.data.from_items(  # noqa: F841
        [{"user_id": 1, "amount": 100}, {"user_id": 2, "amount": 200}]
    )

    query = f"SELECT l.name, r.amount FROM left_data l {join_type} JOIN right_data r ON l.id = r.user_id"
    result = sql(query)
    rows = result.take_all()

    if join_type == "INNER":
        assert len(rows) == 2
    elif join_type == "LEFT":
        assert len(rows) == 3  # Includes Charlie with null amount
    elif join_type == "RIGHT":
        assert len(rows) == 2


# Aggregation operations


def test_sql_aggregation(ray_start_regular_shared, sql_engine):
    """Test aggregation functions."""
    numbers = ray.data.from_items(  # noqa: F841
        [{"value": 10}, {"value": 20}, {"value": 30}]
    )

    result = sql("SELECT COUNT(*) as count, SUM(value) as total FROM numbers")
    row = result.take_all()[0]

    assert row["count"] == 3
    assert row["total"] == 60


@pytest.mark.parametrize(
    "agg_func,expected",
    [
        ("COUNT(*)", 3),
        ("SUM(value)", 60),
        ("AVG(value)", 20),
        ("MIN(value)", 10),
        ("MAX(value)", 30),
    ],
)
def test_sql_aggregation_functions(
    ray_start_regular_shared, agg_func, expected, sql_engine
):
    """Test various aggregation functions."""
    numbers = ray.data.from_items(  # noqa: F841
        [{"value": 10}, {"value": 20}, {"value": 30}]
    )

    result = sql(f"SELECT {agg_func} as result FROM numbers")
    row = result.take_all()[0]

    assert row["result"] == expected


def test_sql_group_by(ray_start_regular_shared, sql_engine):
    """Test GROUP BY with aggregations."""
    data = ray.data.from_items(  # noqa: F841
        [
            {"category": "A", "value": 10},
            {"category": "A", "value": 20},
            {"category": "B", "value": 30},
        ]
    )

    result = sql("SELECT category, SUM(value) as total FROM data GROUP BY category")
    rows = result.take_all()

    assert len(rows) == 2
    category_totals = {row["category"]: row["total"] for row in rows}
    assert category_totals["A"] == 30
    assert category_totals["B"] == 30


# Complex queries


def test_sql_complex_query(ray_start_regular_shared, sql_engine):
    """Test complex query with multiple operations."""
    sales = ray.data.from_items(  # noqa: F841
        [
            {"region": "North", "product": "A", "amount": 100},
            {"region": "North", "product": "B", "amount": 150},
            {"region": "South", "product": "A", "amount": 200},
            {"region": "South", "product": "B", "amount": 120},
            {"region": "North", "product": "A", "amount": 80},
        ]
    )

    result = sql(
        """
        SELECT region, SUM(amount) as total
        FROM sales
        WHERE amount > 90
        GROUP BY region
        ORDER BY total DESC
        LIMIT 2
    """
    )
    rows = result.take_all()

    assert len(rows) == 2
    assert rows[0]["region"] == "South"  # Higher total
    assert rows[0]["total"] == 320


def test_sql_native_expressions(ray_start_regular_shared, sql_engine):
    """Test SQL with complex expressions."""
    data = ray.data.from_items(  # noqa: F841
        [
            {"age": 25, "income": 50000},
            {"age": 35, "income": 75000},
            {"age": 45, "income": 100000},
        ]
    )

    result = sql("SELECT * FROM data WHERE age BETWEEN 30 AND 50 AND income > 60000")
    rows = result.take_all()

    assert len(rows) == 2  # 35 and 45 year olds with high income


# Dataset passing and discovery


def test_sql_explicit_dataset_passing(ray_start_regular_shared, sql_engine):
    """Test explicit dataset passing via kwargs."""
    data = ray.data.from_items([{"value": 42}])

    result = sql("SELECT * FROM my_table", my_table=data)
    rows = result.take_all()

    assert len(rows) == 1
    assert rows[0]["value"] == 42


def test_sql_multiple_explicit_datasets(ray_start_regular_shared, sql_engine):
    """Test passing multiple datasets via kwargs."""
    ds1 = ray.data.from_items([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
    ds2 = ray.data.from_items(
        [{"ref_id": 1, "amount": 100}, {"ref_id": 2, "amount": 200}]
    )

    result = sql(
        "SELECT t1.name, t2.amount FROM table1 t1 JOIN table2 t2 ON t1.id = t2.ref_id",
        table1=ds1,
        table2=ds2,
    )
    rows = result.take_all()

    assert len(rows) == 2
    assert any(row["name"] == "Alice" and row["amount"] == 100 for row in rows)


def test_sql_mixed_discovery(ray_start_regular_shared, sql_engine):
    """Test mix of explicit and auto-discovered tables."""
    # Auto-discovered local variable
    local_data = ray.data.from_items([{"id": 1}, {"id": 2}])  # noqa: F841

    # Explicit dataset
    explicit_data = ray.data.from_items([{"ref_id": 1, "value": "test"}])

    result = sql(
        "SELECT l.id, e.value FROM local_data l JOIN explicit_table e ON l.id = e.ref_id",
        explicit_table=explicit_data,
    )
    rows = result.take_all()

    assert len(rows) == 1
    assert rows[0]["value"] == "test"


def test_sql_global_variable_discovery(ray_start_regular_shared, sql_engine):
    """Test auto-discovery from module-level global variables."""
    global global_test_dataset
    global_test_dataset = ray.data.from_items(
        [{"id": 1, "value": "global"}, {"id": 2, "value": "data"}]
    )

    result = sql("SELECT * FROM global_test_dataset WHERE id = 1")
    rows = result.take_all()

    assert len(rows) == 1
    assert rows[0]["value"] == "global"

    # Cleanup
    global_test_dataset = None


# Table registration operations


def test_sql_register_operations(ray_start_regular_shared, sql_engine):
    """Test table registration operations."""
    clear_tables()

    data = ray.data.from_items([{"test": 1}])

    # Initially no tables
    assert list_tables() == []

    # Register table
    register("test_table", data)

    # Should appear in list
    tables = list_tables()
    assert "test_table" in tables

    # Should work in queries
    result = sql("SELECT * FROM test_table")
    assert len(result.take_all()) == 1

    clear_tables()


def test_sql_clear_tables(ray_start_regular_shared, sql_engine):
    """Test clearing all registered tables."""
    clear_tables()

    data1 = ray.data.from_items([{"a": 1}])
    data2 = ray.data.from_items([{"b": 2}])

    register("table1", data1)
    register("table2", data2)

    assert len(list_tables()) == 2

    clear_tables()

    assert len(list_tables()) == 0


def test_sql_cleanup_after_query(ray_start_regular_shared, sql_engine):
    """Test that auto-registered tables are cleaned up."""
    temp_data = ray.data.from_items([{"temp": 123}])  # noqa: F841

    # Execute query with auto-discovery
    sql("SELECT * FROM temp_data")

    # Table should not be persistently registered
    tables = list_tables()
    assert "temp_data" not in tables


# Configuration tests


def test_sql_config(ray_start_regular_shared):
    """Test SQL configuration via DataContext."""
    ctx = ray.data.DataContext.get_current()

    # Default should be duckdb
    assert ctx.sql_dialect == "duckdb"
    assert config.dialect == "duckdb"

    # Should be able to change via config proxy
    config.dialect = "postgres"
    assert ctx.sql_dialect == "postgres"
    assert config.dialect == "postgres"

    # Should be able to change via DataContext directly
    ctx.sql_dialect = "mysql"
    assert config.dialect == "mysql"

    # Test SQLGlot optimizer config
    assert not ctx.sql_enable_sqlglot_optimizer  # Default
    config.enable_sqlglot_optimizer = True
    assert ctx.sql_enable_sqlglot_optimizer

    # Reset for other tests
    ctx.sql_dialect = "duckdb"
    ctx.sql_enable_sqlglot_optimizer = False


def test_config_invalid_dialect(ray_start_regular_shared):
    """Test that invalid dialect raises ValueError."""
    original_dialect = config.dialect

    with pytest.raises(ValueError, match="Invalid dialect"):
        config.dialect = "invalid_dialect_name"

    # Should not have changed
    assert config.dialect == original_dialect


def test_config_dialect_case_insensitive(ray_start_regular_shared):
    """Test dialect setting is case-insensitive."""
    original_dialect = config.dialect

    config.dialect = "POSTGRES"
    assert config.dialect == "postgres"

    config.dialect = "MySQL"
    assert config.dialect == "mysql"

    # Reset
    config.dialect = original_dialect


def test_config_optimizer_type_validation(ray_start_regular_shared):
    """Test optimizer flag accepts boolean values."""
    original_value = config.enable_sqlglot_optimizer

    # Should accept boolean
    config.enable_sqlglot_optimizer = True
    assert config.enable_sqlglot_optimizer is True

    config.enable_sqlglot_optimizer = False
    assert config.enable_sqlglot_optimizer is False

    # Should convert truthy values to boolean
    config.enable_sqlglot_optimizer = 1
    assert config.enable_sqlglot_optimizer is True

    config.enable_sqlglot_optimizer = 0
    assert config.enable_sqlglot_optimizer is False

    # Reset
    config.enable_sqlglot_optimizer = original_value


def test_config_all_properties(ray_start_regular_shared):
    """Test all SQL config properties are accessible via config proxy."""
    ctx = ray.data.DataContext.get_current()

    # Test log_level
    original_log_level = config.log_level
    config.log_level = "DEBUG"
    assert config.log_level == "DEBUG"
    assert ctx.sql_log_level == "DEBUG"
    config.log_level = original_log_level

    # Test case_sensitive
    original_case = config.case_sensitive
    config.case_sensitive = False
    assert config.case_sensitive is False
    assert ctx.sql_case_sensitive is False
    config.case_sensitive = original_case

    # Test strict_mode
    original_strict = config.strict_mode
    config.strict_mode = True
    assert config.strict_mode is True
    assert ctx.sql_strict_mode is True
    config.strict_mode = original_strict

    # Test enable_optimization
    original_opt = config.enable_optimization
    config.enable_optimization = False
    assert config.enable_optimization is False
    assert ctx.sql_enable_optimization is False
    config.enable_optimization = original_opt

    # Test max_join_partitions
    original_max = config.max_join_partitions
    config.max_join_partitions = 50
    assert config.max_join_partitions == 50
    assert ctx.sql_max_join_partitions == 50
    config.max_join_partitions = original_max

    # Test enable_predicate_pushdown
    original_pred = config.enable_predicate_pushdown
    config.enable_predicate_pushdown = False
    assert config.enable_predicate_pushdown is False
    assert ctx.sql_enable_predicate_pushdown is False
    config.enable_predicate_pushdown = original_pred

    # Test enable_projection_pushdown
    original_proj = config.enable_projection_pushdown
    config.enable_projection_pushdown = False
    assert config.enable_projection_pushdown is False
    assert ctx.sql_enable_projection_pushdown is False
    config.enable_projection_pushdown = original_proj

    # Test query_timeout_seconds
    original_timeout = config.query_timeout_seconds
    config.query_timeout_seconds = 60
    assert config.query_timeout_seconds == 60
    assert ctx.sql_query_timeout_seconds == 60
    config.query_timeout_seconds = original_timeout

    # Test use_datafusion
    original_df = config.use_datafusion
    config.use_datafusion = False
    assert config.use_datafusion is False
    assert ctx.sql_use_datafusion is False
    config.use_datafusion = original_df


def test_datafusion_config(ray_start_regular_shared):
    """Test DataFusion engine configuration."""
    ctx = ray.data.DataContext.get_current()
    original_use_datafusion = ctx.sql_use_datafusion

    # Default should be True
    assert ctx.sql_use_datafusion

    # Should be able to disable
    ctx.sql_use_datafusion = False
    assert not ctx.sql_use_datafusion

    # Re-enable
    ctx.sql_use_datafusion = True
    assert ctx.sql_use_datafusion

    # Reset
    ctx.sql_use_datafusion = original_use_datafusion


def test_datafusion_fallback_to_sqlglot(ray_start_regular_shared):
    """Test graceful fallback to SQLGlot when DataFusion disabled."""
    ctx = ray.data.DataContext.get_current()
    original_use_datafusion = ctx.sql_use_datafusion

    # Disable DataFusion
    ctx.sql_use_datafusion = False

    data = ray.data.from_items(  # noqa: F841
        [{"id": 1, "value": 100}, {"id": 2, "value": 200}]
    )

    # Should work with SQLGlot
    result = sql("SELECT * FROM data WHERE value > 100")
    rows = result.take_all()

    assert len(rows) == 1
    assert rows[0]["value"] == 200

    # Reset
    ctx.sql_use_datafusion = original_use_datafusion


# Error handling tests


def test_sql_error_handling(ray_start_regular_shared, sql_engine):
    """Test proper error handling."""
    # Test invalid query
    with pytest.raises(SQLParseError):
        sql("SELECT * FROM")  # Incomplete query

    # Test missing table
    with pytest.raises(SQLError):
        sql("SELECT * FROM nonexistent_table")


def test_sql_table_not_found_error(ray_start_regular_shared, sql_engine):
    """Test TableNotFoundError for missing tables."""
    with pytest.raises((TableNotFoundError, SQLError)):
        sql("SELECT * FROM nonexistent_table_xyz")


def test_sql_column_not_found_error(ray_start_regular_shared, sql_engine):
    """Test ColumnNotFoundError for missing columns."""
    data = ray.data.from_items([{"existing_col": 1}])  # noqa: F841

    # Try to select non-existent column
    with pytest.raises((ColumnNotFoundError, SQLError, SQLExecutionError)):
        result = sql("SELECT nonexistent_col FROM data")
        result.take_all()  # Force execution


def test_sql_unsupported_operation(ray_start_regular_shared, sql_engine):
    """Test UnsupportedOperationError for unsupported SQL operations."""
    data = ray.data.from_items([{"value": 1}])  # noqa: F841

    # Try unsupported operation (window functions may not be supported)
    with pytest.raises(
        (UnsupportedOperationError, SQLError, SQLExecutionError, SQLParseError)
    ):
        result = sql("SELECT value, ROW_NUMBER() OVER (ORDER BY value) as rn FROM data")
        result.take_all()  # Force execution


# Warning and experimental API tests


def test_sql_experimental_warning(ray_start_regular_shared):
    """Test that FutureWarning is emitted for experimental API."""
    data = ray.data.from_items([{"x": 1}])  # noqa: F841

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        sql("SELECT * FROM data")

        # Should have at least one warning
        assert len(w) >= 1

        # Check that a FutureWarning about experimental API was raised
        future_warnings = [
            warning for warning in w if issubclass(warning.category, FutureWarning)
        ]
        assert len(future_warnings) >= 1
        assert "experimental" in str(future_warnings[0].message).lower()


# Multi-dialect support tests


def test_sql_multi_dialect_support(ray_start_regular_shared, sql_engine):
    """Test multi-dialect support via SQLGlot."""
    ctx = ray.data.DataContext.get_current()
    original_dialect = ctx.sql_dialect

    # Set MySQL dialect
    ctx.sql_dialect = "mysql"

    data = ray.data.from_items([{"id": 1, "name": "Alice"}])  # noqa: F841

    # MySQL syntax - should work via SQLGlot translation
    result = sql("SELECT * FROM data WHERE id = 1")
    rows = result.take_all()

    assert len(rows) == 1
    assert rows[0]["name"] == "Alice"

    # Reset
    ctx.sql_dialect = original_dialect
