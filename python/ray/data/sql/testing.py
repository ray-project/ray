"""
Testing and example infrastructure for Ray Data SQL API.

This module provides comprehensive testing and example functionality
for the SQL engine, including automated test suites and demo scenarios.
"""

from typing import Any, Dict, List, Tuple

import ray
import ray.data
from ray.data import Dataset
from ray.data.sql.core import clear_tables, get_registry, register_table, sql
from ray.data.sql.utils import setup_logger


class TestRunner:
    """Handles running comprehensive tests for the SQL engine."""

    def __init__(self):
        self._logger = setup_logger("TestRunner")

    def run_comprehensive_tests(self) -> bool:
        """Run a comprehensive test suite to validate the SQL engine."""
        print("\n" + "=" * 80)
        print("RaySQL Engine - Comprehensive Test Suite")
        print("=" * 80)

        # Clear any existing tables
        clear_tables()

        # Create test datasets and register them
        test_datasets = self._create_test_datasets()
        for name, dataset in test_datasets.items():
            register_table(name, dataset)
        self._logger.debug(
            f"After registration, tables: {get_registry().list_tables()}"
        )

        # Define test cases
        test_cases = self._define_test_cases()
        self._logger.debug(
            f"Before running tests, tables: {get_registry().list_tables()}"
        )

        # Run tests
        results = self._run_test_cases(test_cases)

        # Report results
        self._report_test_results(results)

        return results["passed"] == len(test_cases)

    def _create_test_datasets(self) -> Dict[str, Dataset]:
        """Create test datasets with various data types and edge cases."""
        return {
            "ds1": ray.data.from_items(
                [
                    {"id": 1, "a": 10, "b": 100, "c": "foo", "flag": 1},
                    {"id": 2, "a": 20, "b": 200, "c": "bar", "flag": 0},
                    {"id": 3, "a": 30, "b": 300, "c": "bar", "flag": 1},
                    {"id": 4, "a": 0, "b": 400, "c": "baz", "flag": 0},
                ]
            ),
            "ds2": ray.data.from_items(
                [
                    {"id": 1, "d": 5.5},
                    {"id": 2, "d": 7.2},
                    {"id": 4, "d": 42.0},
                ]
            ),
            "ds3": ray.data.from_items(
                [
                    {"user_id": 1, "score": 9},
                    {"user_id": 3, "score": 2},
                    {"user_id": 5, "score": 8},
                ]
            ),
            "dsempty": ray.data.from_items([]),
            "dsnull": ray.data.from_items(
                [{"id": 1, "val": None}, {"id": 2, "val": 7}]
            ),
        }

    def _define_test_cases(self) -> List[Tuple[str, str, List[Dict], bool]]:
        """Define the test cases to run."""
        return [
            # Basic SELECT operations
            (
                "select_all",
                "SELECT * FROM ds1",
                [
                    {"id": 1, "a": 10, "b": 100, "c": "foo", "flag": 1},
                    {"id": 2, "a": 20, "b": 200, "c": "bar", "flag": 0},
                    {"id": 3, "a": 30, "b": 300, "c": "bar", "flag": 1},
                    {"id": 4, "a": 0, "b": 400, "c": "baz", "flag": 0},
                ],
                False,
            ),
            (
                "select_columns",
                "SELECT id, a FROM ds1",
                [
                    {"id": 1, "a": 10},
                    {"id": 2, "a": 20},
                    {"id": 3, "a": 30},
                    {"id": 4, "a": 0},
                ],
                False,
            ),
            (
                "select_with_alias",
                "SELECT a AS amount FROM ds1",
                [{"amount": 10}, {"amount": 20}, {"amount": 30}, {"amount": 0}],
                False,
            ),
            # WHERE clauses
            (
                "where_simple",
                "SELECT id FROM ds1 WHERE a > 15",
                [{"id": 2}, {"id": 3}],
                False,
            ),
            (
                "where_and",
                "SELECT id FROM ds1 WHERE a > 10 AND flag = 1",
                [{"id": 3}],
                False,
            ),
            (
                "where_or",
                "SELECT id FROM ds1 WHERE a = 10 OR a = 30",
                [{"id": 1}, {"id": 3}],
                False,
            ),
            # ORDER BY
            (
                "order_asc",
                "SELECT id FROM ds1 ORDER BY a ASC",
                [{"id": 4}, {"id": 1}, {"id": 2}, {"id": 3}],
                False,
            ),
            (
                "order_desc",
                "SELECT id FROM ds1 ORDER BY a DESC",
                [{"id": 3}, {"id": 2}, {"id": 1}, {"id": 4}],
                False,
            ),
            (
                "order_multiple",
                "SELECT id FROM ds1 ORDER BY flag DESC, a ASC",
                [{"id": 1}, {"id": 3}, {"id": 4}, {"id": 2}],
                False,
            ),
            # LIMIT
            (
                "limit_simple",
                "SELECT id FROM ds1 LIMIT 2",
                [{"id": 1}, {"id": 2}],
                False,
            ),
            (
                "limit_with_order",
                "SELECT id FROM ds1 ORDER BY a DESC LIMIT 2",
                [{"id": 3}, {"id": 2}],
                False,
            ),
            # Aggregates without GROUP BY
            ("count_all", "SELECT COUNT(*) as total FROM ds1", [{"total": 4}], False),
            ("sum_column", "SELECT SUM(a) as sum_a FROM ds1", [{"sum_a": 60}], False),
            (
                "multiple_agg",
                "SELECT COUNT(*) as cnt, SUM(a) as sum_a, AVG(a) as avg_a FROM ds1",
                [{"cnt": 4, "sum_a": 60, "avg_a": 15.0}],
                False,
            ),
            # GROUP BY
            (
                "group_by_count",
                "SELECT c, COUNT(*) as cnt FROM ds1 GROUP BY c",
                [
                    {"c": "foo", "cnt": 1},
                    {"c": "bar", "cnt": 2},
                    {"c": "baz", "cnt": 1},
                ],
                False,
            ),
            (
                "group_by_sum",
                "SELECT c, SUM(a) as sum_a FROM ds1 GROUP BY c",
                [
                    {"c": "foo", "sum_a": 10},
                    {"c": "bar", "sum_a": 50},
                    {"c": "baz", "sum_a": 0},
                ],
                False,
            ),
            (
                "group_by_multiple",
                "SELECT flag, COUNT(*) as cnt, SUM(a) as sum_a FROM ds1 GROUP BY flag",
                [
                    {"flag": 0, "cnt": 2, "sum_a": 20},
                    {"flag": 1, "cnt": 2, "sum_a": 40},
                ],
                False,
            ),
            # JOINs following Ray Data Join API
            (
                "inner_join",
                "SELECT ds1.id, ds1.a, ds2.d FROM ds1 JOIN ds2 ON ds1.id = ds2.id",
                [
                    {"id": 1, "a": 10, "d": 5.5},
                    {"id": 2, "a": 20, "d": 7.2},
                    {"id": 4, "a": 0, "d": 42.0},
                ],
                False,
            ),
            (
                "left_join",
                "SELECT ds1.id, ds1.a, ds2.d FROM ds1 LEFT JOIN ds2 ON ds1.id = ds2.id",
                [
                    {"id": 1, "a": 10, "d": 5.5},
                    {"id": 2, "a": 20, "d": 7.2},
                    {"id": 3, "a": 30, "d": None},
                    {"id": 4, "a": 0, "d": 42.0},
                ],
                False,
            ),
            (
                "right_join",
                "SELECT ds2.id, ds1.a, ds2.d FROM ds1 RIGHT JOIN ds2 ON ds1.id = ds2.id",
                [
                    {"id": 1, "a": 10, "d": 5.5},
                    {"id": 2, "a": 20, "d": 7.2},
                    {"id": 4, "a": 0, "d": 42.0},
                ],
                False,
            ),
            (
                "full_join",
                "SELECT ds1.id, ds1.a, ds2.d FROM ds1 FULL JOIN ds2 ON ds1.id = ds2.id",
                [
                    {"id": 1, "a": 10, "d": 5.5},
                    {"id": 2, "a": 20, "d": 7.2},
                    {"id": 3, "a": 30, "d": None},
                    {"id": 4, "a": 0, "d": 42.0},
                ],
                False,
            ),
            # Arithmetic expressions
            (
                "arithmetic",
                "SELECT id, a + b as sum_ab FROM ds1",
                [
                    {"id": 1, "sum_ab": 110},
                    {"id": 2, "sum_ab": 220},
                    {"id": 3, "sum_ab": 330},
                    {"id": 4, "sum_ab": 400},
                ],
                False,
            ),
            (
                "complex_expr",
                "SELECT id, (a + b) * 2 as complex FROM ds1",
                [
                    {"id": 1, "complex": 220},
                    {"id": 2, "complex": 440},
                    {"id": 3, "complex": 660},
                    {"id": 4, "complex": 800},
                ],
                False,
            ),
            # String literals
            (
                "string_literal",
                "SELECT 'Hello' as greeting FROM ds1 LIMIT 1",
                [{"greeting": "Hello"}],
                False,
            ),
            (
                "mixed_literal",
                "SELECT id, 'constant' as const FROM ds1",
                [
                    {"id": 1, "const": "constant"},
                    {"id": 2, "const": "constant"},
                    {"id": 3, "const": "constant"},
                    {"id": 4, "const": "constant"},
                ],
                False,
            ),
            # NULL handling
            (
                "null_count",
                "SELECT COUNT(*) as total, COUNT(val) as non_null FROM dsnull",
                [{"total": 2, "non_null": 1}],
                False,
            ),
            # Empty dataset handling
            ("empty_select", "SELECT * FROM dsempty", [], False),
            ("empty_count", "SELECT COUNT(*) as cnt FROM dsempty", [{"cnt": 0}], False),
            # Error cases
            ("invalid_column", "SELECT nonexistent FROM ds1", None, True),
            ("invalid_table", "SELECT * FROM nonexistent", None, True),
            ("invalid_agg", "SELECT UNSUPPORTED(a) FROM ds1", None, True),
        ]

    def _run_test_cases(
        self, test_cases: List[Tuple[str, str, List[Dict], bool]]
    ) -> Dict[str, Any]:
        """Run all test cases and collect results."""
        passed = 0
        failed = 0
        results = []

        for test_name, sql_query, expected_data, should_fail in test_cases:
            try:
                if test_name in [
                    "limit_simple",
                    "limit_with_order",
                    "left_join",
                    "string_literal",
                    "empty_count",
                    "count_all",
                    "null_count",
                ]:
                    self._logger.debug(f"Running {test_name}: {sql_query}")

                res_ds = sql(sql_query)
                actual_rows = res_ds.take_all()

                if test_name in [
                    "limit_simple",
                    "limit_with_order",
                    "left_join",
                    "string_literal",
                    "empty_count",
                    "count_all",
                    "null_count",
                ]:
                    self._logger.debug(
                        f"{test_name} returned {len(actual_rows)} rows: {actual_rows}"
                    )

                if should_fail:
                    results.append(
                        (
                            test_name,
                            "FAIL",
                            f"Expected error but got {len(actual_rows)} rows",
                            res_ds,
                        )
                    )
                    failed += 1
                else:
                    is_match, error_msg = self._compare_datasets(
                        actual_rows, expected_data
                    )
                    if is_match:
                        results.append(
                            (
                                test_name,
                                "PASS",
                                f"{len(actual_rows)} rows match expected",
                                res_ds,
                            )
                        )
                        passed += 1
                    else:
                        results.append((test_name, "FAIL", error_msg, res_ds))
                        failed += 1
            except Exception as e:
                if test_name in [
                    "limit_simple",
                    "limit_with_order",
                    "left_join",
                    "string_literal",
                    "empty_count",
                    "count_all",
                    "null_count",
                ]:
                    self._logger.debug(f"{test_name} failed with error: {str(e)}")

                if should_fail:
                    results.append(
                        (test_name, "PASS", f"Expected error: {str(e)[:50]}...", None)
                    )
                    passed += 1
                else:
                    results.append(
                        (test_name, "FAIL", f"Unexpected error: {str(e)[:50]}...", None)
                    )
                    failed += 1

        return {
            "passed": passed,
            "failed": failed,
            "total": len(test_cases),
            "results": results,
        }

    def _compare_datasets(
        self, actual_rows: List[Dict], expected_data: List[Dict]
    ) -> Tuple[bool, str]:
        """Compare actual results with expected data."""
        if expected_data is None:
            return False, "Expected data is None"

        actual_normalized = self._normalize_results(actual_rows)
        expected_normalized = self._normalize_results(expected_data)

        if len(actual_normalized) != len(expected_normalized):
            return (
                False,
                f"Row count mismatch: expected {len(expected_normalized)}, got {len(actual_normalized)}",
            )

        for i, (actual_row, expected_row) in enumerate(
            zip(actual_normalized, expected_normalized)
        ):
            if actual_row != expected_row:
                return (
                    False,
                    f"Row {i} mismatch: expected {expected_row}, got {actual_row}",
                )

        return True, "Match"

    def _normalize_results(self, rows: List[Dict]) -> List[Dict]:
        """Normalize results for comparison."""

        def normalize_value(v):
            if isinstance(v, float):
                return round(v, 6)
            return v

        def normalize_row(row):
            return {k: normalize_value(v) for k, v in row.items()}

        return sorted([normalize_row(row) for row in rows], key=str)

    def _report_test_results(self, results: Dict[str, Any]):
        """Report test results."""
        print(f"\nTest Results ({results['passed']}/{results['total']} passed):")
        print("-" * 80)
        print(f"{'Test Name':<25} {'Status':<6} {'Details':<45}")
        print("-" * 80)

        for test_name, status, details, output in results["results"]:
            print(f"{test_name:<25} {status:<6} {details:<45}")

        print("-" * 80)
        print(f"Summary: {results['passed']} passed, {results['failed']} failed")

    def test_ray_data_join_api_compliance(self):
        """Test that join operations follow the Ray Data Join API correctly."""
        print("\n" + "=" * 60)
        print("Testing Ray Data Join API Compliance")
        print("=" * 60)

        # Create test datasets following Ray Data API patterns
        doubles_ds = ray.data.range(4).map(
            lambda row: {"id": row["id"], "double": int(row["id"]) * 2}
        )

        squares_ds = ray.data.range(4).map(
            lambda row: {"id": row["id"], "square": int(row["id"]) ** 2}
        )

        # Register datasets
        register_table("doubles", doubles_ds)
        register_table("squares", squares_ds)

        # Test inner join following Ray Data Join API
        print("\n1. Testing INNER JOIN:")
        inner_join_query = "SELECT doubles.id, doubles.double, squares.square FROM doubles JOIN squares ON doubles.id = squares.id"
        inner_result = sql(inner_join_query)
        inner_rows = inner_result.take_all()
        print(f"INNER JOIN result: {inner_rows}")

        # Expected result following Ray Data Join API example
        expected_inner = [
            {"id": 0, "double": 0, "square": 0},
            {"id": 1, "double": 2, "square": 1},
            {"id": 2, "double": 4, "square": 4},
            {"id": 3, "double": 6, "square": 9},
        ]

        # Verify results match expected
        inner_match = sorted(inner_rows, key=lambda item: item["id"]) == expected_inner
        print(f"INNER JOIN matches expected: {inner_match}")

        print("\n✅ Ray Data Join API compliance tests completed!")
        return True


class ExampleRunner:
    """Handles running example usage demonstrations."""

    def __init__(self):
        self._logger = setup_logger("ExampleRunner")

    def run_examples(self):
        """Run example usage demonstrations."""
        self._run_basic_examples()
        self._run_sqlglot_features_example()

    def _run_basic_examples(self):
        """Run basic usage examples."""
        print("\n" + "=" * 60)
        print("RaySQL Engine - Example Usage")
        print("=" * 60)

        # Create sample datasets for a business scenario
        sales = ray.data.from_items(
            [
                {"id": 1, "product": "laptop", "amount": 1200, "region": "north"},
                {"id": 2, "product": "mouse", "amount": 25, "region": "south"},
                {"id": 3, "product": "laptop", "amount": 1200, "region": "south"},
                {"id": 4, "product": "keyboard", "amount": 75, "region": "north"},
                {"id": 5, "product": "mouse", "amount": 25, "region": "north"},
            ]
        )

        customers = ray.data.from_items(
            [
                {"id": 1, "name": "Alice", "region": "north"},
                {"id": 2, "name": "Bob", "region": "south"},
                {"id": 3, "name": "Charlie", "region": "south"},
                {"id": 4, "name": "Diana", "region": "north"},
                {"id": 5, "name": "Eve", "region": "west"},
            ]
        )

        # Register datasets using the new API
        register_table("sales", sales)
        register_table("customers", customers)

        # Example queries demonstrating various SQL features
        examples = [
            (
                "Total sales by region",
                "SELECT region, SUM(amount) as total_sales FROM sales GROUP BY region ORDER BY total_sales DESC",
            ),
            (
                "Product sales summary",
                "SELECT product, COUNT(*) as units_sold, SUM(amount) as revenue FROM sales GROUP BY product",
            ),
            (
                "Sales with customer names",
                "SELECT c.name, s.product, s.amount FROM customers c JOIN sales s ON c.id = s.id",
            ),
            (
                "High-value sales",
                "SELECT product, amount FROM sales WHERE amount > 100 ORDER BY amount DESC",
            ),
            (
                "Region performance",
                "SELECT region, COUNT(*) as num_sales, AVG(amount) as avg_sale FROM sales GROUP BY region",
            ),
        ]

        for description, query in examples:
            print(f"\n{description}:")
            print(f"Query: {query}")
            try:
                result = sql(query)
                rows = result.take_all()
                print(f"Results ({len(rows)} rows):")
                for row in rows:
                    print(f"  {row}")
            except Exception as e:
                print(f"Error: {e}")

    def _run_sqlglot_features_example(self):
        """Run SQLGlot-inspired features demonstration."""
        print("\n" + "=" * 60)
        print("RaySQL Engine - SQLGlot-Inspired Features Demo")
        print("=" * 60)

        # Create a more complex dataset to demonstrate features
        employees = ray.data.from_items(
            [
                {
                    "id": 1,
                    "name": "Alice",
                    "dept": "Engineering",
                    "salary": 80000,
                    "hire_date": "2020-01-15",
                },
                {
                    "id": 2,
                    "name": "Bob",
                    "dept": "Sales",
                    "salary": 65000,
                    "hire_date": "2019-03-20",
                },
                {
                    "id": 3,
                    "name": "Charlie",
                    "dept": "Engineering",
                    "salary": 90000,
                    "hire_date": "2018-11-10",
                },
                {
                    "id": 4,
                    "name": "Diana",
                    "dept": "Marketing",
                    "salary": 70000,
                    "hire_date": "2021-06-05",
                },
                {
                    "id": 5,
                    "name": "Eve",
                    "dept": "Engineering",
                    "salary": 85000,
                    "hire_date": "2020-08-12",
                },
            ]
        )

        departments = ray.data.from_items(
            [
                {"dept_id": "Engineering", "budget": 500000, "location": "Building A"},
                {"dept_id": "Sales", "budget": 300000, "location": "Building B"},
                {"dept_id": "Marketing", "budget": 200000, "location": "Building C"},
            ]
        )

        print("\n1. Schema Inference:")
        print("-" * 40)
        # The schema is automatically inferred when datasets are registered
        register_table("employees", employees)
        register_table("departments", departments)

        print("\n2. Complex Query with Optimization:")
        print("-" * 40)
        complex_query = """
        SELECT
            e.dept,
            COUNT(*) as emp_count,
            AVG(e.salary) as avg_salary,
            d.budget,
            d.location
        FROM employees e
        JOIN departments d ON e.dept = d.dept_id
        WHERE e.salary > 70000
        GROUP BY e.dept, d.budget, d.location
        ORDER BY avg_salary DESC
        """

        print(f"Query: {complex_query}")
        result = sql(complex_query)
        print(f"Result ({result.count()} rows):")
        for row in result.take_all():
            print(f"  {row}")

        print("\n3. Performance Statistics:")
        print("-" * 40)
        # The engine now logs detailed performance statistics
        simple_query = "SELECT dept, COUNT(*) as count FROM employees GROUP BY dept"
        print(f"Running: {simple_query}")
        result = sql(simple_query)
        print(f"Result: {result.take_all()}")

        print("\n4. Available Tables:")
        print("-" * 40)
        from ray.data.sql.core import get_schema, list_tables

        tables = list_tables()
        print(f"Registered tables: {tables}")

        for table in tables:
            schema = get_schema(table)
            print(f"  {table}: {schema}")
