"""
Ray Data SQL API - Production-ready SQL interface for Ray Datasets.

This module provides a comprehensive SQL interface for Ray Datasets, allowing you to
execute SQL queries against distributed data using Ray's parallel processing
capabilities. The engine supports a wide range of SQL operations including
SELECT, JOIN, WHERE, GROUP BY, ORDER BY, LIMIT, and aggregate functions.

The implementation follows Ray Dataset API patterns for:
- Lazy evaluation of transformations
- Proper return types (Dataset objects)
- Method chaining and composition
- Error handling and validation
- Performance optimization

Examples:
    Basic Usage:
        >>> import ray.data
        >>> from ray.data.sql import register_table, sql
        >>>
        >>> # Create datasets
        >>> users = ray.data.from_items([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
        >>> orders = ray.data.from_items([{"user_id": 1, "amount": 100}, {"user_id": 2, "amount": 200}])
        >>>
        >>> # Register datasets as tables
        >>> register_table("users", users)
        >>> register_table("orders", orders)
        >>>
        >>> # Execute SQL queries
        >>> result = sql("SELECT u.name, SUM(o.amount) as total FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name")
        >>> print(result.take_all())

    Advanced Usage with DataContext:
        >>> from ray.data import DataContext
        >>> ctx = DataContext.get_current()
        >>> ctx.sql_config = {
        ...     "log_level": "DEBUG",
        ...     "case_sensitive": False,
        ...     "max_join_partitions": 20,
        ...     "enable_optimization": True,
        ...     "strict_mode": True
        ... }
        >>> result = ray.data.sql("SELECT * FROM users WHERE id > 1")

    Method Chaining:
        >>> ds = ray.data.sql("SELECT * FROM users WHERE id > 1")
        >>> transformed = ds.map(lambda row: {"name_upper": row["name"].upper()})
        >>> filtered = transformed.filter(lambda row: len(row["name_upper"]) > 4)
        >>> print(filtered.take_all())
"""

import ray.data
from ray.data.sql.config import LogLevel, SQLConfig
from ray.data.sql.core import (
    RaySQL,
    _dataset_name,
    _ray_data_register_table,
    _ray_data_sql,
    clear_tables,
    get_engine,
    get_registry,
    get_schema,
    list_tables,
    register_table,
    sql,
)
from ray.data.sql.testing import ExampleRunner, TestRunner

# Apply monkey patches to integrate with Ray Data API
# Store original functions for potential restoration
_original_ray_data_sql = getattr(ray.data, "sql", None)
_original_register_table = getattr(ray.data, "register_table", None)

# Apply patches
ray.data.sql = _ray_data_sql
ray.data.register_table = _ray_data_register_table

# Add the name method to Dataset class following Ray API patterns
if not hasattr(ray.data.Dataset, "name"):
    ray.data.Dataset.name = _dataset_name

# Public API exports
__all__ = [
    # Core public API
    "sql",
    "register_table",
    "list_tables",
    "get_schema",
    "clear_tables",
    "get_engine",
    "get_registry",
    # Core classes for type hinting and advanced usage
    "RaySQL",
    "SQLConfig",
    "LogLevel",
    # Convenience functions for testing and examples
    "run_comprehensive_tests",
    "example_usage",
    "example_sqlglot_features",
]


# Convenience functions for testing and examples
def run_comprehensive_tests():
    """Run a comprehensive test suite to validate the SQL engine.

    Returns:
        bool: True if all tests pass, False otherwise.
    """
    test_runner = TestRunner()

    # Run the main comprehensive tests
    main_success = test_runner.run_comprehensive_tests()

    # Run Ray Data Join API compliance tests
    try:
        join_api_success = test_runner.test_ray_data_join_api_compliance()
        print(
            f"\nRay Data Join API compliance: {'✅ PASSED' if join_api_success else '❌ FAILED'}"
        )
    except Exception as e:
        print(f"\nRay Data Join API compliance: ❌ FAILED - {e}")
        join_api_success = False

    # Overall success
    overall_success = main_success and join_api_success
    print(
        f"\nOverall test results: {'✅ ALL TESTS PASSED' if overall_success else '❌ SOME TESTS FAILED'}"
    )

    return overall_success


def example_usage():
    """Demonstrate example usage of the RaySQL engine with realistic business scenarios."""
    example_runner = ExampleRunner()
    example_runner.run_examples()


def example_sqlglot_features():
    """Demonstrate the SQLGlot-inspired features of the enhanced RaySQL engine."""
    example_runner = ExampleRunner()
    example_runner._run_sqlglot_features_example()


# Module-level documentation
__version__ = "1.0.0"
__author__ = "Ray Data Team"
__doc__ = (
    __doc__ or "Ray Data SQL API - Production-ready SQL interface for Ray Datasets"
)
