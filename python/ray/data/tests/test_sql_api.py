"""Tests for Ray Data SQL API.

Tests cover core SQL operations supported by the implementation.
All tests are parametrized to run with both SQLGlot and DataFusion engines.
"""

import warnings

import pytest

import ray

from ray.data.sql import (
    ColumnNotFoundError,
    SQLError,
    SQLExecutionError,
    SQLParseError,
    TableNotFoundError,
    UnsupportedOperationError,
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
    assert rows[0]["name"] == "Alice"
    assert rows[1]["name"] == "Charlie"


def test_sql_limit(ray_start_regular_shared, sql_engine):
    """Test LIMIT clause."""
    large_data = ray.data.range(100)  # noqa: F841

    result = sql("SELECT * FROM large_data LIMIT 5")
    rows = result.take_all()

    assert len(rows) == 5


def test_sql_offset_unsupported(ray_start_regular_shared, sql_engine):
    """Test that OFFSET raises UnsupportedOperationError."""
    data = ray.data.range(20)  # noqa: F841

    with pytest.raises(UnsupportedOperationError, match="OFFSET"):
        sql("SELECT * FROM data LIMIT 5 OFFSET 10")


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
        assert len(rows) == 3
    elif join_type == "RIGHT":
        assert len(rows) == 2


# Aggregation operations


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
    """Test aggregation functions."""
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


def test_sql_having_clause(ray_start_regular_shared, sql_engine):
    """Test HAVING clause for post-aggregation filtering."""
    sales = ray.data.from_items(  # noqa: F841
        [
            {"region": "North", "amount": 100},
            {"region": "North", "amount": 200},
            {"region": "South", "amount": 50},
            {"region": "South", "amount": 75},
        ]
    )

    result = sql(
        """
        SELECT region, SUM(amount) as total
        FROM sales
        GROUP BY region
        HAVING SUM(amount) > 100
    """
    )
    rows = result.take_all()

    assert len(rows) == 1
    assert rows[0]["region"] == "North"
    assert rows[0]["total"] == 300


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
    assert rows[0]["region"] == "South"
    assert rows[0]["total"] == 320


def test_sql_between_operator(ray_start_regular_shared, sql_engine):
    """Test BETWEEN operator."""
    data = ray.data.from_items(  # noqa: F841
        [
            {"id": 1, "value": 10},
            {"id": 2, "value": 25},
            {"id": 3, "value": 40},
            {"id": 4, "value": 55},
        ]
    )

    result = sql("SELECT * FROM data WHERE value BETWEEN 20 AND 50")
    rows = result.take_all()

    assert len(rows) == 2
    assert all(20 <= row["value"] <= 50 for row in rows)


def test_sql_arithmetic_expressions(ray_start_regular_shared, sql_engine):
    """Test arithmetic expressions in SELECT clause."""
    data = ray.data.from_items(  # noqa: F841
        [
            {"price": 100, "quantity": 3},
            {"price": 50, "quantity": 2},
        ]
    )

    result = sql(
        """
        SELECT
            price,
            quantity,
            price * quantity as total,
            price + quantity as sum_val,
            price - quantity as diff_val
        FROM data
    """
    )
    rows = result.take_all()

    assert len(rows) == 2
    assert rows[0]["total"] == 300
    assert rows[0]["sum_val"] == 103
    assert rows[0]["diff_val"] == 97


def test_sql_null_handling(ray_start_regular_shared, sql_engine):
    """Test NULL handling in WHERE clause."""
    data = ray.data.from_items(  # noqa: F841
        [
            {"id": 1, "value": 100},
            {"id": 2, "value": None},
            {"id": 3, "value": 200},
        ]
    )

    result = sql("SELECT * FROM data WHERE value IS NOT NULL")
    rows = result.take_all()

    assert len(rows) == 2
    assert all(row["value"] is not None for row in rows)

    result = sql("SELECT * FROM data WHERE value IS NULL")
    rows = result.take_all()
    assert len(rows) == 1
    assert rows[0]["id"] == 2


def test_sql_cte(ray_start_regular_shared, sql_engine):
    """Test Common Table Expressions."""
    base_data = ray.data.from_items(  # noqa: F841
        [{"id": 1, "value": 10}, {"id": 2, "value": 20}, {"id": 3, "value": 30}]
    )

    result = sql(
        """
        WITH filtered AS (
            SELECT * FROM base_data WHERE value > 15
        )
        SELECT * FROM filtered ORDER BY value
    """
    )
    rows = result.take_all()

    assert len(rows) == 2
    assert rows[0]["value"] == 20
    assert rows[1]["value"] == 30


def test_sql_union(ray_start_regular_shared, sql_engine):
    """Test UNION operation."""
    data1 = ray.data.from_items([{"id": 1}, {"id": 2}])  # noqa: F841
    data2 = ray.data.from_items([{"id": 3}, {"id": 4}])  # noqa: F841

    result = sql("SELECT id FROM data1 UNION ALL SELECT id FROM data2")
    rows = result.take_all()

    assert len(rows) == 4


def test_sql_union_distinct_unsupported(ray_start_regular_shared, sql_engine):
    """Test that UNION DISTINCT raises UnsupportedOperationError."""
    data1 = ray.data.from_items([{"id": 1}])  # noqa: F841
    data2 = ray.data.from_items([{"id": 2}])  # noqa: F841

    with pytest.raises(UnsupportedOperationError, match="DISTINCT"):
        sql("SELECT id FROM data1 UNION SELECT id FROM data2")


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


def test_sql_explicit_registration_precedence(ray_start_regular_shared, sql_engine):
    """Test that explicit registration takes precedence over auto-discovery."""
    clear_tables()

    customers = ray.data.from_items([{"id": 1, "name": "Local Customer"}])  # noqa: F841

    explicit_customers = ray.data.from_items([{"id": 2, "name": "Registered Customer"}])
    register("customers", explicit_customers)

    result = sql("SELECT * FROM customers")
    rows = result.take_all()

    assert len(rows) == 1
    assert rows[0]["name"] == "Registered Customer"

    clear_tables()


# Table registration operations


def test_sql_register_operations(ray_start_regular_shared, sql_engine):
    """Test table registration operations."""
    clear_tables()

    data = ray.data.from_items([{"test": 1}])

    assert list_tables() == []

    register("test_table", data)

    tables = list_tables()
    assert "test_table" in tables

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

    sql("SELECT * FROM temp_data")

    tables = list_tables()
    assert "temp_data" not in tables


# Configuration tests


def test_sql_config(ray_start_regular_shared):
    """Test SQL configuration via DataContext."""
    ctx = ray.data.DataContext.get_current()

    assert ctx.sql_dialect == "duckdb"
    assert config.dialect == "duckdb"

    config.dialect = "postgres"
    assert ctx.sql_dialect == "postgres"
    assert config.dialect == "postgres"

    ctx.sql_dialect = "mysql"
    assert config.dialect == "mysql"

    ctx.sql_dialect = "duckdb"


def test_config_invalid_dialect(ray_start_regular_shared):
    """Test that invalid dialect raises ValueError."""
    original_dialect = config.dialect

    with pytest.raises(ValueError, match="Invalid dialect"):
        config.dialect = "invalid_dialect_name"

    assert config.dialect == original_dialect


def test_datafusion_fallback_to_sqlglot(ray_start_regular_shared):
    """Test graceful fallback to SQLGlot when DataFusion disabled."""
    ctx = ray.data.DataContext.get_current()
    original_use_datafusion = ctx.sql_use_datafusion

    ctx.sql_use_datafusion = False

    data = ray.data.from_items(  # noqa: F841
        [{"id": 1, "value": 100}, {"id": 2, "value": 200}]
    )

    result = sql("SELECT * FROM data WHERE value > 100")
    rows = result.take_all()

    assert len(rows) == 1
    assert rows[0]["value"] == 200

    ctx.sql_use_datafusion = original_use_datafusion


# Error handling tests


def test_sql_error_handling(ray_start_regular_shared, sql_engine):
    """Test proper error handling."""
    with pytest.raises(SQLParseError):
        sql("SELECT * FROM")  # Incomplete query

    with pytest.raises(SQLError):
        sql("SELECT * FROM nonexistent_table")


def test_sql_table_not_found_error(ray_start_regular_shared, sql_engine):
    """Test TableNotFoundError for missing tables."""
    with pytest.raises((TableNotFoundError, SQLError)):
        sql("SELECT * FROM nonexistent_table_xyz")


def test_sql_column_not_found_error(ray_start_regular_shared, sql_engine):
    """Test ColumnNotFoundError for missing columns."""
    data = ray.data.from_items([{"existing_col": 1}])  # noqa: F841

    with pytest.raises((ColumnNotFoundError, SQLError, SQLExecutionError)):
        result = sql("SELECT nonexistent_col FROM data")
        result.take_all()


def test_sql_unsupported_operation(ray_start_regular_shared, sql_engine):
    """Test UnsupportedOperationError for unsupported SQL operations."""
    data = ray.data.from_items([{"value": 1}])  # noqa: F841

    with pytest.raises(
        (UnsupportedOperationError, SQLError, SQLExecutionError, SQLParseError)
    ):
        result = sql("SELECT DISTINCT value FROM data")
        result.take_all()


def test_sql_unsupported_window_function(ray_start_regular_shared, sql_engine):
    """Test that window functions raise UnsupportedOperationError."""
    data = ray.data.from_items([{"value": 1}])  # noqa: F841

    with pytest.raises(
        (UnsupportedOperationError, SQLError, SQLExecutionError, SQLParseError)
    ):
        result = sql("SELECT value, ROW_NUMBER() OVER (ORDER BY value) as rn FROM data")
        result.take_all()


def test_sql_unsupported_case_expression(ray_start_regular_shared, sql_engine):
    """Test that CASE expressions raise UnsupportedOperationError."""
    data = ray.data.from_items([{"age": 25}])  # noqa: F841

    with pytest.raises(
        (UnsupportedOperationError, SQLError, SQLExecutionError, SQLParseError)
    ):
        result = sql("SELECT CASE WHEN age < 30 THEN 'young' ELSE 'old' END FROM data")
        result.take_all()


def test_sql_unsupported_like_operator(ray_start_regular_shared, sql_engine):
    """Test that LIKE operator raises UnsupportedOperationError."""
    data = ray.data.from_items([{"name": "Alice"}])  # noqa: F841

    with pytest.raises(
        (UnsupportedOperationError, SQLError, SQLExecutionError, SQLParseError)
    ):
    result = sql("SELECT * FROM data WHERE name LIKE 'Al%'")
        result.take_all()


# Edge cases


def test_sql_empty_result_set(ray_start_regular_shared, sql_engine):
    """Test query that returns empty result set."""
    data = ray.data.from_items([{"value": 1}])  # noqa: F841

    result = sql("SELECT * FROM data WHERE value > 100")
    rows = result.take_all()

    assert len(rows) == 0


def test_sql_limit_exceeds_data(ray_start_regular_shared, sql_engine):
    """Test LIMIT larger than dataset size."""
    data = ray.data.from_items([{"id": i} for i in range(5)])  # noqa: F841

    result = sql("SELECT * FROM data LIMIT 100")
    rows = result.take_all()

    assert len(rows) == 5


def test_sql_complex_where_clause(ray_start_regular_shared, sql_engine):
    """Test complex WHERE clause with AND/OR conditions."""
    data = ray.data.from_items(  # noqa: F841
        [
            {"id": 1, "category": "A", "value": 100},
            {"id": 2, "category": "B", "value": 150},
            {"id": 3, "category": "A", "value": 200},
            {"id": 4, "category": "C", "value": 50},
        ]
    )

    result = sql(
        """
        SELECT * FROM data
        WHERE (category = 'A' AND value > 150)
           OR (category = 'B' AND value < 200)
    """
    )
    rows = result.take_all()

    assert len(rows) == 2
    ids = {row["id"] for row in rows}
    assert ids == {2, 3}


def test_sql_self_join(ray_start_regular_shared, sql_engine):
    """Test self-join operation."""
    employees = ray.data.from_items(  # noqa: F841
        [
            {"id": 1, "name": "Alice", "manager_id": None},
            {"id": 2, "name": "Bob", "manager_id": 1},
            {"id": 3, "name": "Charlie", "manager_id": 1},
        ]
    )

    result = sql(
        """
        SELECT
            e.name as employee,
            m.name as manager
        FROM employees e
        LEFT JOIN employees m ON e.manager_id = m.id
        WHERE e.manager_id IS NOT NULL
    """
    )
    rows = result.take_all()

    assert len(rows) == 2
    assert all(row["manager"] == "Alice" for row in rows)
