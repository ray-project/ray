"""
Comprehensive tests for Ray Data SQL API.

This module contains extensive tests for the new Ray Data SQL engine,
covering SQL operations, aggregations, joins, expressions, and error handling.
The tests follow Ray Data testing patterns and ensure API compliance.
"""

import pytest

import ray

# Import the SQL engine components
from ray.data.sql import (
    LogLevel,
    RaySQL,
    SQLConfig,
    clear_tables,
    get_schema,
    list_tables,
    register_table,
    sql,
)
from ray.tests.conftest import *  # noqa


def debug_registry_state(test_name=""):
    """Helper function to debug registry state during tests."""
    print(f"\nRegistry Debug ({test_name})")
    print("-" * 50)

    # Check list_tables()
    try:
        available_tables = list_tables()
        print(f"list_tables(): {available_tables}")
    except Exception as e:
        print(f"list_tables() error: {e}")

    # Check global registry directly
    try:
        from ray.data.sql.core import get_registry

        registry = get_registry()
        global_tables = list(registry._tables.keys())
        print(f"Global registry: {global_tables}")
    except Exception as e:
        print(f"Global registry error: {e}")

    # Check engine and executor registries
    try:
        from ray.data.sql.core import get_engine

        engine = get_engine()
        engine_tables = list(engine.registry._tables.keys())
        executor_tables = list(engine.executor.registry._tables.keys())
        print(f"Engine registry: {engine_tables}")
        print(f"Executor registry: {executor_tables}")

        # Check if they're the same objects
        engine_registry_id = id(engine.registry)
        executor_registry_id = id(engine.executor.registry)
        global_registry_id = id(registry)

        print("Registry object IDs:")
        print(f" Global: {global_registry_id}")
        print(f" Engine: {engine_registry_id}")
        print(f" Executor: {executor_registry_id}")

        if engine_registry_id == executor_registry_id == global_registry_id:
            print("All registries are the same object!")
        else:
            print("Registry objects are different!")

    except Exception as e:
        print(f"Engine/Executor registry error: {e}")

    print("-" * 50)


@pytest.fixture(scope="function")
def sql_test_data():
    """Fixture providing test datasets for SQL operations."""
    print("\n Setting up SQL test data fixture...")

    test_data = {
        "users": ray.data.from_items(
            [
                {"id": 1, "name": "Alice", "age": 30, "department": "Engineering"},
                {"id": 2, "name": "Bob", "age": 25, "department": "Marketing"},
                {"id": 3, "name": "Charlie", "age": 35, "department": "Engineering"},
                {"id": 4, "name": "Diana", "age": 28, "department": "Sales"},
            ]
        ),
        "orders": ray.data.from_items(
            [
                {"order_id": 1, "user_id": 1, "amount": 100.0, "status": "completed"},
                {"order_id": 2, "user_id": 2, "amount": 75.0, "status": "pending"},
                {"order_id": 3, "user_id": 1, "amount": 150.0, "status": "completed"},
                {"order_id": 4, "user_id": 3, "amount": 200.0, "status": "completed"},
                {"order_id": 5, "user_id": 4, "amount": 50.0, "status": "cancelled"},
            ]
        ),
        "products": ray.data.from_items(
            [
                {
                    "product_id": 1,
                    "name": "Widget A",
                    "price": 25.0,
                    "category": "Tools",
                },
                {
                    "product_id": 2,
                    "name": "Widget B",
                    "price": 35.0,
                    "category": "Tools",
                },
                {
                    "product_id": 3,
                    "name": "Gadget X",
                    "price": 45.0,
                    "category": "Electronics",
                },
                {
                    "product_id": 4,
                    "name": "Gadget Y",
                    "price": 55.0,
                    "category": "Electronics",
                },
            ]
        ),
        "null_table": ray.data.from_items(
            [
                {"id": 1, "value": 10},
                {"id": 2, "value": 20},
                {"id": 3, "value": None},
            ]
        ),
        "empty_table": ray.data.from_items([]),
    }

    # Clear any existing tables first
    print("Clearing existing tables...")
    clear_tables()

    # Check registry state before registration
    from ray.data.sql.core import get_registry

    registry = get_registry()
    print(f"Registry state before registration: {list(registry._tables.keys())}")

    # Register all test datasets
    print("Registering test datasets...")
    for name, dataset in test_data.items():
        print(f" Registering '{name}'...")
        register_table(name, dataset)

        # Verify registration immediately
        available_tables = list_tables()
        if name in available_tables:
            print(f"  '{name}' registered successfully")
        else:
            print(f"  '{name}' registration failed!")

    # Final registry state
    final_tables = list_tables()
    print(f"Final registered tables: {final_tables}")

    # Debug registry state after setup
    debug_registry_state("After fixture setup")

    return test_data


def test_basic_select_operations(ray_start_regular_shared, sql_test_data):
    """Test basic SELECT operations."""
    print("\n Testing basic SELECT operations...")
    debug_registry_state("test_basic_select_operations start")

    # SELECT * FROM table
    print("Executing: SELECT * FROM users")
    debug_registry_state("Before SELECT * query execution")
    result = sql("SELECT * FROM users")
    rows = result.take_all()
    print(f"Query executed, got {len(rows)} rows")
    assert len(rows) == 4
    assert all("id" in row for row in rows)
    assert all("name" in row for row in rows)

    # SELECT specific columns
    print("Executing: SELECT name, age FROM users")
    result = sql("SELECT name, age FROM users")
    rows = result.take_all()
    print(f"Query executed, got {len(rows)} rows")
    assert len(rows) == 4
    assert all(set(row.keys()) == {"name", "age"} for row in rows)

    # SELECT with WHERE condition
    print("Executing: SELECT * FROM users WHERE age > 25")
    result = sql("SELECT * FROM users WHERE age > 25")
    rows = result.take_all()
    print(f"Query executed, got {len(rows)} rows")
    assert len(rows) == 3  # Alice (30), Charlie (35), Diana (28)
    assert all(row["age"] > 25 for row in rows)

    # SELECT with multiple WHERE conditions
    print("Executing: SELECT name FROM users WHERE age > 25 AND age < 35")
    result = sql("SELECT name FROM users WHERE age > 25 AND age < 35")
    rows = result.take_all()
    print(f"Query executed, got {len(rows)} rows")
    assert len(rows) == 2  # Alice (30), Diana (28)
    expected_names = {"Alice", "Diana"}
    actual_names = {row["name"] for row in rows}
    assert actual_names == expected_names

    print("Basic SELECT operations completed successfully!")


def test_where_clauses(ray_start_regular_shared, sql_test_data):
    """Test WHERE clause filtering."""
    # Simple WHERE condition
    result = sql("SELECT name FROM users WHERE age > 30")
    rows = result.take_all()
    assert len(rows) == 1
    assert rows[0]["name"] == "Charlie"

    # WHERE with AND
    result = sql("SELECT name FROM users WHERE age > 25 AND department = 'Engineering'")
    rows = result.take_all()
    assert len(rows) == 1
    assert rows[0]["name"] == "Charlie"

    # WHERE with OR
    result = sql("SELECT name FROM users WHERE age < 27 OR department = 'Sales'")
    rows = result.take_all()
    names = {row["name"] for row in rows}
    assert names == {"Bob", "Diana"}


def test_order_by_operations(ray_start_regular_shared, sql_test_data):
    """Test ORDER BY functionality."""
    # ORDER BY ASC
    result = sql("SELECT name FROM users ORDER BY age ASC")
    rows = result.take_all()
    names = [row["name"] for row in rows]
    assert names == ["Bob", "Diana", "Alice", "Charlie"]

    # ORDER BY DESC
    result = sql("SELECT name FROM users ORDER BY age DESC")
    rows = result.take_all()
    names = [row["name"] for row in rows]
    assert names == ["Charlie", "Alice", "Diana", "Bob"]

    # ORDER BY multiple columns
    result = sql("SELECT name FROM users ORDER BY department DESC, age ASC")
    rows = result.take_all()
    names = [row["name"] for row in rows]
    assert names == ["Diana", "Bob", "Alice", "Charlie"]


def test_limit_operations(ray_start_regular_shared, sql_test_data):
    """Test LIMIT functionality."""
    print("\n Testing LIMIT operations...")
    debug_registry_state("test_limit_operations start")

    # Simple LIMIT
    print("Executing: SELECT name FROM users LIMIT 2")
    debug_registry_state("Before LIMIT query execution")
    result = sql("SELECT name FROM users LIMIT 2")
    rows = result.take_all()
    print(f"Query executed, got {len(rows)} rows")
    assert len(rows) == 2

    # LIMIT with ORDER BY
    result = sql("SELECT name FROM users ORDER BY age DESC LIMIT 2")
    rows = result.take_all()
    names = [row["name"] for row in rows]
    assert names == ["Charlie", "Bob"]

    # LIMIT 0
    result = sql("SELECT name FROM users LIMIT 0")
    rows = result.take_all()
    assert len(rows) == 0


def test_aggregate_functions(ray_start_regular_shared, sql_test_data):
    """Test aggregate functions without GROUP BY."""
    # COUNT(*)
    result = sql("SELECT COUNT(*) as total FROM users")
    rows = result.take_all()
    assert len(rows) == 1
    assert rows[0]["total"] == 4

    # COUNT(column)
    result = sql("SELECT COUNT(name) as name_count FROM users")
    rows = result.take_all()
    assert len(rows) == 1
    assert rows[0]["name_count"] == 4

    # SUM
    result = sql("SELECT SUM(age) as total_age FROM users")
    rows = result.take_all()
    assert len(rows) == 1
    assert rows[0]["total_age"] == 118  # 25+30+35+28

    # AVG
    result = sql("SELECT AVG(age) as avg_age FROM users")
    rows = result.take_all()
    assert len(rows) == 1
    assert abs(rows[0]["avg_age"] - 29.5) < 0.01  # 118/4

    # MIN and MAX
    result = sql("SELECT MIN(age) as min_age, MAX(age) as max_age FROM users")
    rows = result.take_all()
    assert len(rows) == 1
    assert rows[0]["min_age"] == 25
    assert rows[0]["max_age"] == 35


def test_group_by_operations(ray_start_regular_shared, sql_test_data):
    """Test GROUP BY with aggregates."""
    # GROUP BY with COUNT
    result = sql("SELECT city, COUNT(*) as user_count FROM users GROUP BY city")
    rows = result.take_all()
    assert len(rows) == 2
    city_counts = {row["city"]: row["user_count"] for row in rows}
    assert city_counts["Seattle"] == 2
    assert city_counts["Portland"] == 2

    # GROUP BY with SUM
    result = sql("SELECT city, SUM(age) as total_age FROM users GROUP BY city")
    rows = result.take_all()
    city_ages = {row["city"]: row["total_age"] for row in rows}
    assert city_ages["Seattle"] == 60  # 25+35
    assert city_ages["Portland"] == 58  # 30+28

    # GROUP BY with multiple aggregates
    result = sql(
        "SELECT city, COUNT(*) as cnt, AVG(age) as avg_age FROM users GROUP BY city"
    )
    rows = result.take_all()
    assert len(rows) == 2


def test_join_operations(ray_start_regular_shared, sql_test_data):
    """Test JOIN operations following Ray Data Join API."""
    # INNER JOIN
    result = sql(
        "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id"
    )
    rows = result.take_all()
    assert len(rows) == 5  # All orders have matching users

    # LEFT JOIN
    result = sql(
        "SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id"
    )
    rows = result.take_all()
    assert len(rows) == 5  # All users, some with orders

    # RIGHT JOIN
    result = sql(
        "SELECT u.name, o.amount FROM users u RIGHT JOIN orders o ON u.id = o.user_id"
    )
    rows = result.take_all()
    assert len(rows) == 5  # All orders with their users

    # Join with WHERE
    result = sql(
        "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount > 100"
    )
    rows = result.take_all()
    assert len(rows) == 2  # Alice (150) and Charlie (200)
    names = {row["name"] for row in rows}
    assert "Alice" in names and "Charlie" in names


def test_arithmetic_expressions(ray_start_regular_shared, sql_test_data):
    """Test arithmetic expressions in SELECT."""
    # Basic arithmetic
    result = sql("SELECT age, age + 5 as age_plus_5 FROM users WHERE name = 'Alice'")
    rows = result.take_all()
    assert len(rows) == 1
    assert rows[0]["age"] == 30
    assert rows[0]["age_plus_5"] == 35

    # Complex arithmetic
    result = sql("SELECT age, (age * 2) - 10 as formula FROM users WHERE name = 'Bob'")
    rows = result.take_all()
    assert len(rows) == 1
    assert rows[0]["age"] == 25
    assert rows[0]["formula"] == 40  # (25*2)-10

    # Arithmetic with multiple columns
    result = sql(
        "SELECT order_id, amount, amount * 1.1 as amount_with_tax FROM orders WHERE amount > 100"
    )
    rows = result.take_all()
    assert len(rows) == 2  # orders with amount > 100
    for row in rows:
        expected_tax = row["amount"] * 1.1
        assert abs(row["amount_with_tax"] - expected_tax) < 0.01


def test_literal_values(ray_start_regular_shared, sql_test_data):
    """Test literal values in SELECT."""
    # String literals
    result = sql("SELECT 'Hello' as greeting, 42 as number FROM users LIMIT 1")
    rows = result.take_all()
    assert len(rows) == 1
    assert rows[0]["greeting"] == "Hello"
    assert rows[0]["number"] == 42

    # Mixed literals and columns
    result = sql("SELECT name, 'User' as type FROM users WHERE name = 'Alice'")
    rows = result.take_all()
    assert len(rows) == 1
    assert rows[0]["name"] == "Alice"
    assert rows[0]["type"] == "User"


def test_null_handling(ray_start_regular_shared, sql_test_data):
    """Test NULL value handling."""
    # COUNT with nulls
    result = sql(
        "SELECT COUNT(*) as total, COUNT(value) as non_null_values FROM null_table"
    )
    rows = result.take_all()
    assert len(rows) == 1
    assert rows[0]["total"] == 3
    assert rows[0]["non_null_values"] == 2  # Only two non-null values

    # IS NULL condition
    result = sql("SELECT id FROM null_table WHERE value IS NULL")
    rows = result.take_all()
    assert len(rows) == 1
    assert rows[0]["id"] == 3


def test_empty_dataset_handling(ray_start_regular_shared, sql_test_data):
    """Test operations on empty datasets."""
    # SELECT from empty table
    result = sql("SELECT * FROM empty_table")
    rows = result.take_all()
    assert len(rows) == 0

    # Aggregates on empty table
    result = sql("SELECT COUNT(*) as cnt FROM empty_table")
    rows = result.take_all()
    assert len(rows) == 1
    assert rows[0]["cnt"] == 0


def test_complex_queries(ray_start_regular_shared, sql_test_data):
    """Test complex multi-operation queries."""
    # Complex query with JOIN, WHERE, GROUP BY, ORDER BY, LIMIT
    complex_query = """
    SELECT u.city, COUNT(*) as user_count, SUM(o.amount) as total_sales
    FROM users u
    JOIN orders o ON u.id = o.user_id
    WHERE o.amount > 50
    GROUP BY u.city
    ORDER BY total_sales DESC
    LIMIT 5
    """
    result = sql(complex_query)
    rows = result.take_all()
    assert len(rows) <= 2  # Maximum 2 cities
    assert all(
        "city" in row and "user_count" in row and "total_sales" in row for row in rows
    )


def test_configuration_and_context(ray_start_regular_shared, sql_test_data):
    """Test SQL configuration and DataContext integration."""
    # Test basic configuration
    config = SQLConfig(log_level=LogLevel.DEBUG, case_sensitive=False, strict_mode=True)
    engine = RaySQL(config)

    # Register a table and test
    engine.register("test_users", sql_test_data["users"])
    result = engine.sql("SELECT COUNT(*) as cnt FROM test_users")
    rows = result.take_all()
    assert len(rows) == 1
    assert rows[0]["cnt"] == 4


def test_error_conditions(ray_start_regular_shared, sql_test_data):
    """Test error handling and validation."""
    # Test invalid table name
    with pytest.raises(ValueError, match="not found"):
        sql("SELECT * FROM nonexistent_table")

    # Test invalid column name
    with pytest.raises(ValueError, match="not found"):
        sql("SELECT nonexistent_column FROM users")


def test_table_management(ray_start_regular_shared, sql_test_data):
    """Test table registration and management."""
    # Test list_tables
    tables = list_tables()
    expected_tables = {"users", "orders", "empty_table", "null_table"}
    assert set(tables) == expected_tables

    # Test schema retrieval
    schema = get_schema("users")
    assert schema is not None

    # Test clear_tables
    clear_tables()
    tables_after_clear = list_tables()
    assert len(tables_after_clear) == 0


def test_dataset_chaining(ray_start_regular_shared, sql_test_data):
    """Test Ray Dataset API method chaining with SQL results."""
    # SQL result should be a proper Dataset that supports chaining
    result = sql("SELECT name, age FROM users WHERE age > 25")

    # Chain Ray Dataset operations
    transformed = result.map(
        lambda row: {"name_upper": row["name"].upper(), "age_group": "adult"}
    )
    filtered = transformed.filter(lambda row: len(row["name_upper"]) > 3)
    final_rows = filtered.take_all()

    # Verify chaining worked
    assert len(final_rows) >= 1
    assert all("name_upper" in row and "age_group" in row for row in final_rows)


def test_api_integration(ray_start_regular_shared):
    """Test Ray Data API integration (monkey patching)."""
    # Test that ray.data.sql exists and works
    test_ds = ray.data.from_items([{"x": 1}, {"x": 2}])
    register_table("test_integration", test_ds)

    # Use ray.data.sql directly
    result = ray.data.sql("SELECT COUNT(*) as cnt FROM test_integration")
    rows = result.take_all()
    assert len(rows) == 1
    assert rows[0]["cnt"] == 2

    # Test that ray.data.register_table exists and works
    new_ds = ray.data.from_items([{"y": 3}, {"y": 4}])
    ray.data.register_table("test_integration_2", new_ds)

    result2 = ray.data.sql("SELECT COUNT(*) as cnt FROM test_integration_2")
    rows2 = result2.take_all()
    assert len(rows2) == 1
    assert rows2[0]["cnt"] == 2

    # Clean up
    clear_tables()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
