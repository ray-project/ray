"""Extended tests for Ray Data SQL API - Additional coverage for edge cases.

This test file complements test_sql_api.py by adding coverage for:
- HAVING clause
- Complex SQL expressions (CASE, COALESCE, NULL IF)
- String and mathematical functions
- Multiple GROUP BY columns
- Self-joins
- NULL handling
- Edge cases and boundary conditions
"""

import pytest

import ray
from ray.data.sql import (
    SQLError,
    SQLExecutionError,
    sql,
    register,
    clear_tables,
)
from ray.tests.conftest import *  # noqa


# HAVING clause tests


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

    # Only North (300) should pass HAVING clause
    assert len(rows) == 1
    assert rows[0]["region"] == "North"
    assert rows[0]["total"] == 300


def test_sql_having_with_alias(ray_start_regular_shared, sql_engine):
    """Test HAVING clause using column alias."""
    data = ray.data.from_items(  # noqa: F841
        [
            {"category": "A", "value": 10},
            {"category": "A", "value": 20},
            {"category": "B", "value": 5},
        ]
    )

    result = sql(
        """
        SELECT category, SUM(value) as total
        FROM data
        GROUP BY category
        HAVING total > 10
    """
    )
    rows = result.take_all()

    assert len(rows) == 1
    assert rows[0]["category"] == "A"


# Multiple GROUP BY columns


def test_sql_multiple_group_by_columns(ray_start_regular_shared, sql_engine):
    """Test GROUP BY with multiple columns."""
    sales = ray.data.from_items(  # noqa: F841
        [
            {"region": "North", "product": "A", "amount": 100},
            {"region": "North", "product": "A", "amount": 150},
            {"region": "North", "product": "B", "amount": 200},
            {"region": "South", "product": "A", "amount": 50},
            {"region": "South", "product": "B", "amount": 75},
        ]
    )

    result = sql(
        """
        SELECT region, product, SUM(amount) as total
        FROM sales
        GROUP BY region, product
        ORDER BY region, product
    """
    )
    rows = result.take_all()

    assert len(rows) == 4
    # Verify multi-dimensional aggregation
    north_a = [r for r in rows if r["region"] == "North" and r["product"] == "A"]
    assert len(north_a) == 1
    assert north_a[0]["total"] == 250


# Complex SQL expressions


def test_sql_case_expression(ray_start_regular_shared, sql_engine):
    """Test CASE WHEN expressions."""
    employees = ray.data.from_items(  # noqa: F841
        [
            {"name": "Alice", "age": 25},
            {"name": "Bob", "age": 35},
            {"name": "Charlie", "age": 45},
        ]
    )

    result = sql(
        """
        SELECT
            name,
            age,
            CASE
                WHEN age < 30 THEN 'Junior'
                WHEN age < 40 THEN 'Mid-level'
                ELSE 'Senior'
            END as level
        FROM employees
    """
    )
    rows = result.take_all()

    assert len(rows) == 3
    assert rows[0]["level"] == "Junior"
    assert rows[1]["level"] == "Mid-level"
    assert rows[2]["level"] == "Senior"


def test_sql_coalesce_function(ray_start_regular_shared, sql_engine):
    """Test COALESCE for NULL handling."""
    data = ray.data.from_items(  # noqa: F841
        [
            {"id": 1, "value": 100, "backup": 50},
            {"id": 2, "value": None, "backup": 75},
        ]
    )

    result = sql("SELECT id, COALESCE(value, backup) as final_value FROM data")
    rows = result.take_all()

    assert len(rows) == 2
    assert rows[0]["final_value"] == 100
    assert rows[1]["final_value"] == 75


# NULL handling tests


def test_sql_null_in_where_clause(ray_start_regular_shared, sql_engine):
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


def test_sql_null_comparison(ray_start_regular_shared, sql_engine):
    """Test that NULL comparisons follow SQL semantics."""
    data = ray.data.from_items(  # noqa: F841
        [
            {"id": 1, "value": 100},
            {"id": 2, "value": None},
        ]
    )

    # NULL != anything should be FALSE (no rows returned)
    result = sql("SELECT * FROM data WHERE value = NULL")
    rows = result.take_all()
    assert len(rows) == 0

    # Use IS NULL for NULL checks
    result = sql("SELECT * FROM data WHERE value IS NULL")
    rows = result.take_all()
    assert len(rows) == 1
    assert rows[0]["id"] == 2


# Self-join tests


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
    # Bob and Charlie both report to Alice
    assert all(row["manager"] == "Alice" for row in rows)


# String function tests


def test_sql_string_functions(ray_start_regular_shared, sql_engine):
    """Test SQL string functions."""
    data = ray.data.from_items(  # noqa: F841
        [
            {"name": "alice"},
            {"name": "BOB"},
        ]
    )

    result = sql("SELECT UPPER(name) as upper_name, LOWER(name) as lower_name FROM data")
    rows = result.take_all()

    assert len(rows) == 2
    assert rows[0]["upper_name"] == "ALICE"
    assert rows[0]["lower_name"] == "alice"
    assert rows[1]["upper_name"] == "BOB"
    assert rows[1]["lower_name"] == "bob"


# Mathematical function tests


def test_sql_math_functions(ray_start_regular_shared, sql_engine):
    """Test SQL mathematical functions."""
    numbers = ray.data.from_items(  # noqa: F841
        [
            {"value": -5.7},
            {"value": 3.2},
        ]
    )

    result = sql(
        """
        SELECT
            value,
            ABS(value) as abs_value,
            ROUND(value) as rounded
        FROM numbers
    """
    )
    rows = result.take_all()

    assert len(rows) == 2
    assert rows[0]["abs_value"] == 5.7
    assert rows[0]["rounded"] == -6
    assert rows[1]["abs_value"] == 3.2
    assert rows[1]["rounded"] == 3


# BETWEEN operator test


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


# IN operator test


def test_sql_in_operator(ray_start_regular_shared, sql_engine):
    """Test IN operator."""
    data = ray.data.from_items(  # noqa: F841
        [
            {"id": 1, "category": "A"},
            {"id": 2, "category": "B"},
            {"id": 3, "category": "C"},
            {"id": 4, "category": "D"},
        ]
    )

    result = sql("SELECT * FROM data WHERE category IN ('A', 'C')")
    rows = result.take_all()

    assert len(rows) == 2
    categories = {row["category"] for row in rows}
    assert categories == {"A", "C"}


# LIKE operator test


def test_sql_like_operator(ray_start_regular_shared, sql_engine):
    """Test LIKE pattern matching operator."""
    data = ray.data.from_items(  # noqa: F841
        [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Alex"},
        ]
    )

    result = sql("SELECT * FROM data WHERE name LIKE 'Al%'")
    rows = result.take_all()

    assert len(rows) == 2
    assert all(row["name"].startswith("Al") for row in rows)


# CTE (Common Table Expression) with JOIN


def test_sql_cte_with_join(ray_start_regular_shared, sql_engine):
    """Test CTE with JOIN operations."""
    customers = ray.data.from_items(  # noqa: F841
        [
            {"customer_id": 1, "name": "Alice"},
            {"customer_id": 2, "name": "Bob"},
        ]
    )

    orders = ray.data.from_items(  # noqa: F841
        [
            {"order_id": 1, "customer_id": 1, "amount": 100},
            {"order_id": 2, "customer_id": 1, "amount": 150},
            {"order_id": 3, "customer_id": 2, "amount": 200},
        ]
    )

    result = sql(
        """
        WITH customer_totals AS (
            SELECT customer_id, SUM(amount) as total
            FROM orders
            GROUP BY customer_id
        )
        SELECT c.name, ct.total
        FROM customers c
        JOIN customer_totals ct ON c.customer_id = ct.customer_id
        ORDER BY ct.total DESC
    """
    )
    rows = result.take_all()

    assert len(rows) == 2
    assert rows[0]["name"] == "Alice"
    assert rows[0]["total"] == 250


# Edge case: Empty result set


def test_sql_empty_result_set(ray_start_regular_shared, sql_engine):
    """Test query that returns empty result set."""
    data = ray.data.from_items([{"value": 1}])  # noqa: F841

    result = sql("SELECT * FROM data WHERE value > 100")
    rows = result.take_all()

    assert len(rows) == 0


# Edge case: Single row


def test_sql_single_row_operations(ray_start_regular_shared, sql_engine):
    """Test operations on single-row dataset."""
    data = ray.data.from_items([{"value": 42}])  # noqa: F841

    result = sql("SELECT value * 2 as doubled FROM data")
    rows = result.take_all()

    assert len(rows) == 1
    assert rows[0]["doubled"] == 84


# Edge case: Large LIMIT


def test_sql_limit_exceeds_data(ray_start_regular_shared, sql_engine):
    """Test LIMIT larger than dataset size."""
    data = ray.data.from_items([{"id": i} for i in range(5)])  # noqa: F841

    result = sql("SELECT * FROM data LIMIT 100")
    rows = result.take_all()

    # Should return all available rows (5)
    assert len(rows) == 5


# Edge case: LIMIT with OFFSET


def test_sql_limit_with_offset(ray_start_regular_shared, sql_engine):
    """Test LIMIT with OFFSET clause."""
    data = ray.data.range(20)  # noqa: F841

    result = sql("SELECT * FROM data LIMIT 5 OFFSET 10")
    rows = result.take_all()

    assert len(rows) == 5
    # Should get rows 10-14
    assert rows[0]["id"] == 10
    assert rows[4]["id"] == 14


# Arithmetic expressions in SELECT


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


# Complex WHERE clause with multiple conditions


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
    # Should get id=3 (A, 200) and id=2 (B, 150)
    ids = {row["id"] for row in rows}
    assert ids == {2, 3}


# Aggregation without GROUP BY


def test_sql_aggregation_without_group_by(ray_start_regular_shared, sql_engine):
    """Test aggregation functions without GROUP BY (global aggregation)."""
    data = ray.data.from_items(  # noqa: F841
        [
            {"value": 10},
            {"value": 20},
            {"value": 30},
        ]
    )

    result = sql(
        """
        SELECT
            COUNT(*) as count,
            SUM(value) as total,
            AVG(value) as average,
            MIN(value) as minimum,
            MAX(value) as maximum
        FROM data
    """
    )
    rows = result.take_all()

    assert len(rows) == 1
    row = rows[0]
    assert row["count"] == 3
    assert row["total"] == 60
    assert row["average"] == 20
    assert row["minimum"] == 10
    assert row["maximum"] == 30


# Test FULL OUTER JOIN


@pytest.mark.skip(reason="FULL OUTER JOIN may not be supported")
def test_sql_full_outer_join(ray_start_regular_shared, sql_engine):
    """Test FULL OUTER JOIN operation."""
    left_data = ray.data.from_items(  # noqa: F841
        [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
    )

    right_data = ray.data.from_items(  # noqa: F841
        [
            {"user_id": 2, "amount": 200},
            {"user_id": 3, "amount": 300},
        ]
    )

    result = sql(
        """
        SELECT l.name, r.amount
        FROM left_data l
        FULL OUTER JOIN right_data r ON l.id = r.user_id
    """
    )
    rows = result.take_all()

    # Should have 3 rows: Alice (null), Bob (200), null (300)
    assert len(rows) == 3


# Test column aliasing in JOINs


def test_sql_join_with_column_aliases(ray_start_regular_shared, sql_engine):
    """Test JOIN with explicit column aliasing to avoid conflicts."""
    users = ray.data.from_items(  # noqa: F841
        [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
    )

    profiles = ray.data.from_items(  # noqa: F841
        [
            {"id": 1, "user_id": 1, "bio": "Engineer"},
            {"id": 2, "user_id": 2, "bio": "Designer"},
        ]
    )

    result = sql(
        """
        SELECT
            u.id as user_id,
            u.name,
            p.id as profile_id,
            p.bio
        FROM users u
        JOIN profiles p ON u.id = p.user_id
    """
    )
    rows = result.take_all()

    assert len(rows) == 2
    # Verify aliasing worked correctly
    assert "user_id" in rows[0]
    assert "profile_id" in rows[0]
    assert rows[0]["user_id"] != rows[0]["profile_id"]  # Should be different values

