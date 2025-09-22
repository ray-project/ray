"""Tests for Ray Data SQL API."""

import pytest

import ray
from ray.data.sql_api import clear_tables, list_tables, register, sql
from ray.data.sql import SQLError, SQLParseError
from ray.tests.conftest import *  # noqa


def test_sql_auto_discovery(ray_start_regular_shared):
    """Test automatic dataset discovery from variables."""
    users = ray.data.from_items([
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25}
    ])
    
    # Auto-discovery should work without registration
    result = sql("SELECT * FROM users WHERE age > 25")
    rows = result.take_all()
    
    assert len(rows) == 1
    assert rows[0]["name"] == "Alice"


def test_sql_projection(ray_start_regular_shared):
    """Test SELECT column projection using Ray Dataset native operations."""
    data = ray.data.from_items([{"x": 1, "y": 10}, {"x": 2, "y": 20}])
    
    result = sql("SELECT x FROM data")
    rows = result.take_all()
    
    assert len(rows) == 2
    assert all("x" in row for row in rows)
    assert all("y" not in row for row in rows)


def test_sql_join(ray_start_regular_shared):
    """Test JOIN operations using Ray Dataset native joins."""
    users = ray.data.from_items([
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"}
    ])
    
    orders = ray.data.from_items([
        {"user_id": 1, "amount": 100},
        {"user_id": 2, "amount": 200}
    ])
    
    result = sql("SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id")
    rows = result.take_all()
    
    assert len(rows) == 2
    assert any(row["name"] == "Alice" and row["amount"] == 100 for row in rows)


def test_sql_explicit_dataset_passing(ray_start_regular_shared):
    """Test explicit dataset passing via kwargs."""
    data = ray.data.from_items([{"value": 42}])
    
    result = sql("SELECT * FROM my_table", my_table=data)
    rows = result.take_all()
    
    assert len(rows) == 1
    assert rows[0]["value"] == 42


def test_sql_cleanup_after_query(ray_start_regular_shared):
    """Test that auto-registered tables are cleaned up."""
    temp_data = ray.data.from_items([{"temp": 123}])
    
    # Execute query with auto-discovery
    sql("SELECT * FROM temp_data")
    
    # Table should not be persistently registered
    tables = list_tables()
    assert "temp_data" not in tables


def test_sql_aggregation(ray_start_regular_shared):
    """Test aggregation functions using Ray Dataset native aggregates."""
    numbers = ray.data.from_items([
        {"value": 10}, {"value": 20}, {"value": 30}
    ])
    
    result = sql("SELECT COUNT(*) as count, SUM(value) as total FROM numbers")
    row = result.take_all()[0]
    
    assert row["count"] == 3
    assert row["total"] == 60


def test_sql_filtering_and_sorting(ray_start_regular_shared):
    """Test WHERE and ORDER BY using Ray Dataset native operations."""
    scores = ray.data.from_items([
        {"name": "Alice", "score": 95},
        {"name": "Bob", "score": 87},
        {"name": "Charlie", "score": 92}
    ])
    
    result = sql("SELECT name, score FROM scores WHERE score > 90 ORDER BY score DESC")
    rows = result.take_all()
    
    assert len(rows) == 2
    assert rows[0]["name"] == "Alice"  # Highest score first
    assert rows[1]["name"] == "Charlie"


def test_sql_limit(ray_start_regular_shared):
    """Test LIMIT using Ray Dataset native limit operation."""
    large_data = ray.data.range(100)
    
    result = sql("SELECT * FROM large_data LIMIT 5")
    rows = result.take_all()
    
    assert len(rows) == 5


def test_sql_error_handling(ray_start_regular_shared):
    """Test proper error handling."""
    # Test invalid query
    with pytest.raises(SQLParseError):
        sql("SELECT * FROM")  # Incomplete query
    
    # Test missing table
    with pytest.raises(SQLError):
        sql("SELECT * FROM nonexistent_table")


def test_sql_config():
    """Test SQL configuration."""
    from ray.data.sql_api import config
    
    # Default should be duckdb
    assert config.dialect == "duckdb"
    
    # Should be able to change
    config.dialect = "postgres"
    assert config.dialect == "postgres"
    
    # Reset for other tests
    config.dialect = "duckdb"


def test_sql_register_operations(ray_start_regular_shared):
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


def test_sql_clear_tables(ray_start_regular_shared):
    """Test clearing all registered tables."""
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
    left_data = ray.data.from_items([
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"}
    ])
    
    right_data = ray.data.from_items([
        {"user_id": 1, "amount": 100},
        {"user_id": 2, "amount": 200}
    ])
    
    query = f"SELECT l.name, r.amount FROM left_data l {join_type} JOIN right_data r ON l.id = r.user_id"
    result = sql(query)
    rows = result.take_all()
    
    if join_type == "INNER":
        assert len(rows) == 2
    elif join_type == "LEFT":
        assert len(rows) == 3  # Includes Charlie with null amount
    elif join_type == "RIGHT":
        assert len(rows) == 2


@pytest.mark.parametrize("agg_func,expected", [
    ("COUNT(*)", 3),
    ("SUM(value)", 60),
    ("AVG(value)", 20),
    ("MIN(value)", 10),
    ("MAX(value)", 30)
])
def test_sql_aggregation_functions(ray_start_regular_shared, agg_func, expected):
    """Test various aggregation functions using Ray Dataset native aggregates."""
    numbers = ray.data.from_items([
        {"value": 10}, {"value": 20}, {"value": 30}
    ])
    
    result = sql(f"SELECT {agg_func} as result FROM numbers")
    row = result.take_all()[0]
    
    assert row["result"] == expected


def test_sql_complex_query(ray_start_regular_shared):
    """Test complex query with multiple Ray Dataset native operations."""
    sales = ray.data.from_items([
        {"region": "North", "product": "A", "amount": 100},
        {"region": "North", "product": "B", "amount": 150},
        {"region": "South", "product": "A", "amount": 200},
        {"region": "South", "product": "B", "amount": 120},
        {"region": "North", "product": "A", "amount": 80}
    ])
    
    # Complex query using multiple Ray Dataset native operations
    result = sql("""
        SELECT region, SUM(amount) as total
        FROM sales 
        WHERE amount > 90
        GROUP BY region
        ORDER BY total DESC
        LIMIT 2
    """)
    rows = result.take_all()
    
    assert len(rows) == 2
    assert rows[0]["region"] == "South"  # Higher total
    assert rows[0]["total"] == 320


def test_sql_native_expressions(ray_start_regular_shared):
    """Test that SQL uses Ray Dataset native expression optimization."""
    data = ray.data.from_items([
        {"age": 25, "income": 50000},
        {"age": 35, "income": 75000},
        {"age": 45, "income": 100000}
    ])
    
    # This should use dataset.filter(expr=...) internally
    result = sql("SELECT * FROM data WHERE age BETWEEN 30 AND 50 AND income > 60000")
    rows = result.take_all()
    
    assert len(rows) == 2  # 35 and 45 year olds with high income