"""Tests for Ray Data SQL API.

Tests cover:
- Basic SQL operations (SELECT, WHERE, JOIN, GROUP BY, etc.)
- Multi-dialect support via SQLGlot
- DataFusion optimization integration
- Configuration via DataContext
- Error handling and fallback behavior
- Auto-discovery of datasets from variables
- Edge cases and validation
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


def test_sql_auto_discovery(ray_start_regular_shared, sql_engine):
    """Test automatic dataset discovery from variables with both SQL engines."""
    users = ray.data.from_items(  # noqa: F841
        [{"id": 1, "name": "Alice", "age": 30}, {"id": 2, "name": "Bob", "age": 25}]
    )

    # Auto-discovery should work without registration
    result = sql("SELECT * FROM users WHERE age > 25")
    rows = result.take_all()

    assert len(rows) == 1
    assert rows[0]["name"] == "Alice"


def test_sql_projection(ray_start_regular_shared, sql_engine):
    """Test SELECT column projection with both SQL engines."""
    data = ray.data.from_items([{"x": 1, "y": 10}, {"x": 2, "y": 20}])  # noqa: F841

    result = sql("SELECT x FROM data")
    rows = result.take_all()

    assert len(rows) == 2
    assert all("x" in row for row in rows)
    assert all("y" not in row for row in rows)


def test_sql_join(ray_start_regular_shared, sql_engine):
    """Test JOIN operations with both SQL engines."""
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


def test_sql_explicit_dataset_passing(ray_start_regular_shared, sql_engine):
    """Test explicit dataset passing via kwargs with both SQL engines."""
    data = ray.data.from_items([{"value": 42}])

    result = sql("SELECT * FROM my_table", my_table=data)
    rows = result.take_all()

    assert len(rows) == 1
    assert rows[0]["value"] == 42


def test_sql_cleanup_after_query(ray_start_regular_shared, sql_engine):
    """Test that auto-registered tables are cleaned up with both SQL engines."""
    temp_data = ray.data.from_items([{"temp": 123}])  # noqa: F841

    # Execute query with auto-discovery
    sql("SELECT * FROM temp_data")

    # Table should not be persistently registered
    tables = list_tables()
    assert "temp_data" not in tables


def test_sql_aggregation(ray_start_regular_shared, sql_engine):
    """Test aggregation functions with both SQL engines."""
    numbers = ray.data.from_items(  # noqa: F841
        [{"value": 10}, {"value": 20}, {"value": 30}]
    )

    result = sql("SELECT COUNT(*) as count, SUM(value) as total FROM numbers")
    row = result.take_all()[0]

    assert row["count"] == 3
    assert row["total"] == 60


def test_sql_filtering_and_sorting(ray_start_regular_shared, sql_engine):
    """Test WHERE and ORDER BY with both SQL engines."""
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
    """Test LIMIT using Ray Dataset native limit operation with both SQL engines."""
    large_data = ray.data.range(100)  # noqa: F841

    result = sql("SELECT * FROM large_data LIMIT 5")
    rows = result.take_all()

    assert len(rows) == 5


def test_sql_error_handling(ray_start_regular_shared, sql_engine):
    """Test proper error handling with both SQL engines."""
    # Test invalid query
    with pytest.raises(SQLParseError):
        sql("SELECT * FROM")  # Incomplete query

    # Test missing table
    with pytest.raises(SQLError):
        sql("SELECT * FROM nonexistent_table")


def test_sql_config(ray_start_regular_shared):
    """Test SQL configuration via DataContext."""
    import ray.data
    from ray.data.experimental.sql_api import config

    # Get DataContext
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


def test_sql_register_operations(ray_start_regular_shared, sql_engine):
    """Test table registration operations with both SQL engines."""
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
    """Test clearing all registered tables with both SQL engines."""
    clear_tables()

    data1 = ray.data.from_items([{"a": 1}])
    data2 = ray.data.from_items([{"b": 2}])

    register("table1", data1)
    register("table2", data2)

    assert len(list_tables()) == 2

    clear_tables()

    assert len(list_tables()) == 0


@pytest.mark.parametrize("join_type", ["INNER", "LEFT", "RIGHT"])
def test_sql_join_types(ray_start_regular_shared, join_type):
    """Test different JOIN types using Ray Dataset native joins."""
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
def test_sql_aggregation_functions(ray_start_regular_shared, agg_func, expected):
    """Test various aggregation functions using Ray Dataset native aggregates."""
    numbers = ray.data.from_items(  # noqa: F841
        [{"value": 10}, {"value": 20}, {"value": 30}]
    )

    result = sql(f"SELECT {agg_func} as result FROM numbers")
    row = result.take_all()[0]

    assert row["result"] == expected


def test_sql_complex_query(ray_start_regular_shared, sql_engine):
    """Test complex query with multiple Ray Dataset native operations and both SQL engines."""
    sales = ray.data.from_items(  # noqa: F841
        [
            {"region": "North", "product": "A", "amount": 100},
            {"region": "North", "product": "B", "amount": 150},
            {"region": "South", "product": "A", "amount": 200},
            {"region": "South", "product": "B", "amount": 120},
            {"region": "North", "product": "A", "amount": 80},
        ]
    )

    # Complex query using multiple Ray Dataset native operations
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
    """Test that SQL uses Ray Dataset native expression optimization with both SQL engines."""
    data = ray.data.from_items(  # noqa: F841
        [
            {"age": 25, "income": 50000},
            {"age": 35, "income": 75000},
            {"age": 45, "income": 100000},
        ]
    )

    # This should use dataset.filter(expr=...) internally
    result = sql("SELECT * FROM data WHERE age BETWEEN 30 AND 50 AND income > 60000")
    rows = result.take_all()

    assert len(rows) == 2  # 35 and 45 year olds with high income


# Edge case tests
def test_sql_global_variable_discovery(ray_start_regular_shared, sql_engine):
    """Test auto-discovery from module-level global variables with both SQL engines."""
    global global_test_dataset
    global_test_dataset = ray.data.from_items(
        [{"id": 1, "value": "global"}, {"id": 2, "value": "data"}]
    )

    # Should discover global variable
    result = sql("SELECT * FROM global_test_dataset WHERE id = 1")
    rows = result.take_all()

    assert len(rows) == 1
    assert rows[0]["value"] == "global"

    # Cleanup
    global_test_dataset = None


def test_sql_multiple_explicit_datasets(ray_start_regular_shared, sql_engine):
    """Test passing multiple datasets via kwargs with both SQL engines."""
    ds1 = ray.data.from_items([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
    ds2 = ray.data.from_items(
        [{"ref_id": 1, "amount": 100}, {"ref_id": 2, "amount": 200}]
    )

    # Pass both datasets explicitly
    result = sql(
        "SELECT t1.name, t2.amount FROM table1 t1 JOIN table2 t2 ON t1.id = t2.ref_id",
        table1=ds1,
        table2=ds2,
    )
    rows = result.take_all()

    assert len(rows) == 2
    assert any(row["name"] == "Alice" and row["amount"] == 100 for row in rows)


def test_sql_mixed_discovery(ray_start_regular_shared, sql_engine):
    """Test mix of explicit and auto-discovered tables with both SQL engines."""
    # Auto-discovered local variable
    local_data = ray.data.from_items([{"id": 1}, {"id": 2}])  # noqa: F841

    # Explicit dataset
    explicit_data = ray.data.from_items([{"ref_id": 1, "value": "test"}])

    # Should handle both
    result = sql(
        "SELECT l.id, e.value FROM local_data l JOIN explicit_table e ON l.id = e.ref_id",
        explicit_table=explicit_data,
    )
    rows = result.take_all()

    assert len(rows) == 1
    assert rows[0]["value"] == "test"


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


# Configuration validation tests
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


# Error class coverage tests
def test_sql_table_not_found_error(ray_start_regular_shared, sql_engine):
    """Test TableNotFoundError for missing tables with both SQL engines."""
    # Table does not exist
    with pytest.raises((TableNotFoundError, SQLError)):
        sql("SELECT * FROM nonexistent_table_xyz")


def test_sql_column_not_found_error(ray_start_regular_shared, sql_engine):
    """Test ColumnNotFoundError for missing columns with both SQL engines."""
    data = ray.data.from_items([{"existing_col": 1}])  # noqa: F841

    # Try to select non-existent column
    with pytest.raises((ColumnNotFoundError, SQLError, SQLExecutionError)):
        result = sql("SELECT nonexistent_col FROM data")
        result.take_all()  # Force execution


def test_sql_unsupported_operation(ray_start_regular_shared, sql_engine):
    """Test UnsupportedOperationError for unsupported SQL operations with both SQL engines."""
    data = ray.data.from_items([{"value": 1}])  # noqa: F841

    # Try unsupported operation (example: window functions may not be supported)
    with pytest.raises(
        (UnsupportedOperationError, SQLError, SQLExecutionError, SQLParseError)
    ):
        result = sql("SELECT value, ROW_NUMBER() OVER (ORDER BY value) as rn FROM data")
        result.take_all()  # Force execution
