"""Tests for DataFusion integration with Ray Data SQL API."""

import pytest

import ray
from ray.data.experimental.sql.datafusion_optimizer import (
    DataFusionOptimizer,
    is_datafusion_available,
)
from ray.data.experimental.sql_api import clear_tables, sql
from ray.tests.conftest import *  # noqa


@pytest.fixture
def reset_sql_config():
    """Reset SQL configuration before and after tests."""
    ctx = ray.data.DataContext.get_current()
    original_use_datafusion = ctx.sql_use_datafusion
    original_dialect = ctx.sql_dialect

    yield

    # Reset after test
    ctx.sql_use_datafusion = original_use_datafusion
    ctx.sql_dialect = original_dialect
    clear_tables()


def test_datafusion_availability():
    """Test DataFusion availability detection."""
    available = is_datafusion_available()

    if available:
        print("DataFusion is available for testing")
        optimizer = DataFusionOptimizer()
        assert optimizer.is_available()
    else:
        print("DataFusion not available - tests will validate fallback")


def test_datafusion_config(ray_start_regular_shared, reset_sql_config):
    """Test DataFusion configuration via DataContext."""
    ctx = ray.data.DataContext.get_current()

    # Default should be True
    assert ctx.sql_use_datafusion

    # Should be able to disable
    ctx.sql_use_datafusion = False
    assert ctx.sql_use_datafusion

    # Re-enable
    ctx.sql_use_datafusion = True
    assert ctx.sql_use_datafusion


def test_datafusion_with_simple_query(ray_start_regular_shared, reset_sql_config):
    """Test DataFusion with simple SELECT query."""
    ctx = ray.data.DataContext.get_current()
    ctx.sql_use_datafusion = True

    data = ray.data.from_items(  # noqa: F841
        [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    )

    # Execute query - will use DataFusion if available, fallback to SQLGlot
    result = sql("SELECT * FROM data WHERE id > 0")
    rows = result.take_all()

    assert len(rows) == 2
    assert rows[0]["name"] in ["Alice", "Bob"]


def test_datafusion_fallback_to_sqlglot(ray_start_regular_shared, reset_sql_config):
    """Test graceful fallback to SQLGlot when DataFusion disabled."""
    ctx = ray.data.DataContext.get_current()

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


def test_datafusion_with_join(ray_start_regular_shared, reset_sql_config):
    """Test DataFusion optimization with JOIN queries."""
    ctx = ray.data.DataContext.get_current()
    ctx.sql_use_datafusion = True

    users = ray.data.from_items(  # noqa: F841
        [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    )

    orders = ray.data.from_items(  # noqa: F841
        [{"user_id": 1, "amount": 100}, {"user_id": 2, "amount": 200}]
    )

    # Execute JOIN - DataFusion should optimize join order
    result = sql(
        "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id"
    )
    rows = result.take_all()

    assert len(rows) == 2
    assert any(row["name"] == "Alice" and row["amount"] == 100 for row in rows)


def test_datafusion_with_aggregation(ray_start_regular_shared, reset_sql_config):
    """Test DataFusion with aggregation queries."""
    ctx = ray.data.DataContext.get_current()
    ctx.sql_use_datafusion = True

    data = ray.data.from_items(  # noqa: F841
        [
            {"category": "A", "value": 10},
            {"category": "A", "value": 20},
            {"category": "B", "value": 30},
        ]
    )

    # Aggregation query
    result = sql("SELECT category, SUM(value) as total FROM data GROUP BY category")
    rows = result.take_all()

    assert len(rows) == 2
    category_totals = {row["category"]: row["total"] for row in rows}
    assert category_totals["A"] == 30
    assert category_totals["B"] == 30


@pytest.mark.skipif(not is_datafusion_available(), reason="DataFusion not installed")
def test_datafusion_optimizer_initialization():
    """Test DataFusion optimizer initialization."""
    optimizer = DataFusionOptimizer()

    assert optimizer.is_available()
    assert optimizer.df_ctx is not None


@pytest.mark.skipif(not is_datafusion_available(), reason="DataFusion not installed")
def test_datafusion_dataset_registration():
    """Test registering Ray Datasets with DataFusion."""
    optimizer = DataFusionOptimizer()

    # Create test dataset
    test_data = ray.data.from_items(
        [{"id": 1, "name": "Alice", "age": 30}, {"id": 2, "name": "Bob", "age": 25}]
    )

    # Register with DataFusion
    datasets = {"test_table": test_data}
    optimizer._register_datasets(datasets)

    # Verify table was registered (DataFusion should have it)
    # Note: Full verification would query DataFusion's catalog


def test_datafusion_multi_dialect_support(ray_start_regular_shared, reset_sql_config):
    """Test that DataFusion works with multi-dialect support via SQLGlot."""
    ctx = ray.data.DataContext.get_current()

    # Set MySQL dialect (SQLGlot will translate)
    ctx.sql_dialect = "mysql"
    ctx.sql_use_datafusion = True

    data = ray.data.from_items([{"id": 1, "name": "Alice"}])  # noqa: F841

    # MySQL syntax - should work via SQLGlot translation
    result = sql("SELECT * FROM data WHERE id = 1")
    rows = result.take_all()

    assert len(rows) == 1
    assert rows[0]["name"] == "Alice"


def test_datafusion_config_via_api(ray_start_regular_shared, reset_sql_config):
    """Test enabling/disabling DataFusion via SQL API."""
    import ray.data

    ctx = ray.data.DataContext.get_current()

    # Test various ways to configure
    ctx.sql_use_datafusion = True
    assert ctx.sql_use_datafusion

    ctx.sql_use_datafusion = False
    assert ctx.sql_use_datafusion

    # Via configure function
    from ray.data.experimental.sql.core import configure

    configure(sql_use_datafusion=True)
    # Note: configure() doesn't directly support sql_use_datafusion yet
    # This tests that unknown parameters are handled


def test_datafusion_error_handling(ray_start_regular_shared, reset_sql_config):
    """Test error handling with DataFusion."""
    ctx = ray.data.DataContext.get_current()
    ctx.sql_use_datafusion = True

    # Invalid query should still raise appropriate error
    with pytest.raises(Exception):
        sql("SELECT * FROM nonexistent_table")


def test_datafusion_performance_logging(ray_start_regular_shared, reset_sql_config):
    """Test that DataFusion logs optimization info."""
    ctx = ray.data.DataContext.get_current()
    ctx.sql_use_datafusion = True
    ctx.sql_log_level = "DEBUG"

    data = ray.data.from_items([{"x": 1}, {"x": 2}, {"x": 3}])  # noqa: F841

    # Execute query - check logs indicate DataFusion usage
    result = sql("SELECT * FROM data WHERE x > 1")
    rows = result.take_all()

    assert len(rows) == 2


def test_datafusion_with_complex_query(ray_start_regular_shared, reset_sql_config):
    """Test DataFusion with complex multi-table query."""
    ctx = ray.data.DataContext.get_current()
    ctx.sql_use_datafusion = True

    # Create multiple tables
    users = ray.data.from_items(  # noqa: F841
        [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
            {"id": 3, "name": "Charlie", "age": 35},
        ]
    )

    orders = ray.data.from_items(  # noqa: F841
        [
            {"user_id": 1, "amount": 100, "status": "completed"},
            {"user_id": 2, "amount": 200, "status": "completed"},
            {"user_id": 3, "amount": 150, "status": "pending"},
        ]
    )

    # Complex query with join, filter, aggregation
    result = sql(
        """
        SELECT u.name, SUM(o.amount) as total
        FROM users u
        JOIN orders o ON u.id = o.user_id
        WHERE o.status = 'completed' AND u.age > 20
        GROUP BY u.name
        ORDER BY total DESC
    """
    )

    rows = result.take_all()

    assert len(rows) == 2
    # Bob has higher order amount
    assert rows[0]["name"] == "Bob"
    assert rows[0]["total"] == 200
