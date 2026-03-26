"""Integration tests for predicate expression operations.

These tests require Ray and test end-to-end predicate expression evaluation.
"""

import pandas as pd
import pytest
from packaging.version import parse as parse_version

import ray
from ray.data._internal.util import rows_same
from ray.data._internal.utils.arrow_utils import get_pyarrow_version
from ray.data.expressions import col
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

pytestmark = pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="Expression integration tests require PyArrow >= 20.0.0",
)


class TestPredicateIntegration:
    """Integration tests for predicate expressions with Ray Dataset."""

    def test_null_predicates_with_dataset(self, ray_start_regular_shared):
        """Test null predicate expressions with Ray Dataset."""
        ds = ray.data.from_items(
            [
                {"value": 10, "name": "Alice"},
                {"value": None, "name": "Bob"},
                {"value": 30, "name": None},
                {"value": None, "name": None},
            ]
        )

        result = (
            ds.with_column("value_is_null", col("value").is_null())
            .with_column("name_not_null", col("name").is_not_null())
            .with_column(
                "both_present", col("value").is_not_null() & col("name").is_not_null()
            )
            .to_pandas()
        )

        expected = pd.DataFrame(
            {
                "value": [10, None, 30, None],
                "name": ["Alice", "Bob", None, None],
                "value_is_null": [False, True, False, True],
                "name_not_null": [True, True, False, False],
                "both_present": [True, False, False, False],
            }
        )
        assert rows_same(result, expected)

    def test_membership_predicates_with_dataset(self, ray_start_regular_shared):
        """Test membership predicate expressions with Ray Dataset."""
        ds = ray.data.from_items(
            [
                {"status": "active", "category": "A"},
                {"status": "inactive", "category": "B"},
                {"status": "pending", "category": "A"},
                {"status": "deleted", "category": "C"},
            ]
        )

        result = (
            ds.with_column(
                "is_valid_status", col("status").is_in(["active", "pending"])
            )
            .with_column("not_deleted", col("status").not_in(["deleted"]))
            .with_column("category_a", col("category").is_in(["A"]))
            .to_pandas()
        )

        expected = pd.DataFrame(
            {
                "status": ["active", "inactive", "pending", "deleted"],
                "category": ["A", "B", "A", "C"],
                "is_valid_status": [True, False, True, False],
                "not_deleted": [True, True, True, False],
                "category_a": [True, False, True, False],
            }
        )
        pd.testing.assert_frame_equal(result, expected, check_dtype=False)

    @pytest.mark.parametrize(
        "test_data,expression,expected_results,test_id",
        [
            pytest.param(
                [{"value": 1}, {"value": None}, {"value": 3}],
                col("value").is_null(),
                [False, True, False],
                "is_null_with_actual_nulls",
            ),
            pytest.param(
                [{"value": 1}, {"value": None}, {"value": 3}],
                col("value").is_not_null(),
                [True, False, True],
                "is_not_null_with_actual_nulls",
            ),
            pytest.param(
                [{"value": 1}, {"value": 2}, {"value": 3}],
                col("value").is_in([1, 3]),
                [True, False, True],
                "isin_operation",
            ),
            pytest.param(
                [{"value": 1}, {"value": 2}, {"value": 3}],
                col("value").not_in([1, 3]),
                [False, True, False],
                "not_in_operation",
            ),
            pytest.param(
                [{"name": "Alice"}, {"name": "Bob"}, {"name": "Charlie"}],
                col("name") == "Bob",
                [False, True, False],
                "string_equality",
            ),
            pytest.param(
                [{"name": "Alice"}, {"name": "Bob"}, {"name": "Charlie"}],
                col("name") != "Bob",
                [True, False, True],
                "string_not_equal",
            ),
            pytest.param(
                [{"name": "included"}, {"name": "excluded"}, {"name": None}],
                col("name").is_not_null() & (col("name") != "excluded"),
                [True, False, False],
                "string_filter",
            ),
        ],
    )
    def test_null_and_membership_with_dataset(
        self, ray_start_regular_shared, test_data, expression, expected_results, test_id
    ):
        """Test null checking and membership operations with Ray Dataset."""
        ds = ray.data.from_items(test_data)
        result = ds.with_column("result", expression).to_pandas()

        expected_data = {}
        for key in test_data[0].keys():
            expected_data[key] = [row[key] for row in test_data]
        expected_data["result"] = expected_results
        expected = pd.DataFrame(expected_data)

        assert rows_same(result, expected)

    @pytest.mark.parametrize(
        "filter_expr,test_data,expected_flags,test_id",
        [
            pytest.param(
                col("age") >= 21,
                [
                    {"age": 20, "name": "Alice"},
                    {"age": 21, "name": "Bob"},
                    {"age": 25, "name": "Charlie"},
                ],
                [False, True, True],
                "age_filter",
            ),
            pytest.param(
                col("score") > 50,
                [
                    {"score": 30, "status": "fail"},
                    {"score": 50, "status": "pass"},
                    {"score": 70, "status": "pass"},
                ],
                [False, False, True],
                "score_filter",
            ),
            pytest.param(
                (col("age") >= 18) & col("active"),
                [
                    {"age": 17, "active": True},
                    {"age": 18, "active": False},
                    {"age": 25, "active": True},
                ],
                [False, False, True],
                "complex_and_filter",
            ),
            pytest.param(
                (col("status") == "approved") | (col("priority") == "high"),
                [
                    {"status": "pending", "priority": "low"},
                    {"status": "approved", "priority": "low"},
                    {"status": "pending", "priority": "high"},
                ],
                [False, True, True],
                "complex_or_filter",
            ),
            pytest.param(
                col("value").is_not_null() & (col("value") > 0),
                [{"value": None}, {"value": -5}, {"value": 10}],
                [False, False, True],
                "null_aware_filter",
            ),
            pytest.param(
                col("name").is_not_null() & (col("name") != "excluded"),
                [{"name": "included"}, {"name": "excluded"}, {"name": None}],
                [True, False, False],
                "string_filter",
            ),
            pytest.param(
                col("category").is_in(["A", "B"]),
                [
                    {"category": "A"},
                    {"category": "B"},
                    {"category": "C"},
                    {"category": "D"},
                ],
                [True, True, False, False],
                "membership_filter",
            ),
            pytest.param(
                (col("score") >= 50) & (col("grade") != "F"),
                [
                    {"score": 45, "grade": "F"},
                    {"score": 55, "grade": "D"},
                    {"score": 75, "grade": "B"},
                    {"score": 30, "grade": "F"},
                ],
                [False, True, True, False],
                "nested_filters",
            ),
        ],
    )
    def test_filter_expressions_with_dataset(
        self, ray_start_regular_shared, filter_expr, test_data, expected_flags, test_id
    ):
        """Test filter expressions with Ray Dataset."""
        ds = ray.data.from_items(test_data)
        result = ds.with_column("is_filtered", filter_expr).to_pandas()

        expected = pd.DataFrame(test_data)
        expected["is_filtered"] = expected_flags

        assert rows_same(result, expected)

    def test_filter_in_pipeline_with_dataset(self, ray_start_regular_shared):
        """Test filter expressions in a data processing pipeline."""
        test_data = [
            {"product": "A", "quantity": 10, "price": 100, "region": "North"},
            {"product": "B", "quantity": 5, "price": 200, "region": "South"},
            {"product": "C", "quantity": 20, "price": 50, "region": "North"},
            {"product": "D", "quantity": 15, "price": 75, "region": "East"},
            {"product": "E", "quantity": 3, "price": 300, "region": "West"},
        ]

        ds = ray.data.from_items(test_data)

        result = (
            ds.with_column("revenue", col("quantity") * col("price"))
            .with_column("is_high_value", col("revenue") >= 1000)
            .with_column("is_bulk_order", col("quantity") >= 10)
            .with_column("is_premium", col("price") >= 100)
            .with_column(
                "needs_special_handling",
                (col("is_high_value")) | (col("is_bulk_order") & col("is_premium")),
            )
            .with_column("is_north_region", col("region") == "North")
            .to_pandas()
        )

        expected = pd.DataFrame(
            {
                "product": ["A", "B", "C", "D", "E"],
                "quantity": [10, 5, 20, 15, 3],
                "price": [100, 200, 50, 75, 300],
                "region": ["North", "South", "North", "East", "West"],
                "revenue": [1000, 1000, 1000, 1125, 900],
                "is_high_value": [True, True, True, True, False],
                "is_bulk_order": [True, False, True, True, False],
                "is_premium": [True, True, False, False, True],
                "needs_special_handling": [True, True, True, True, False],
                "is_north_region": [True, False, True, False, False],
            }
        )

        assert rows_same(result, expected)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
