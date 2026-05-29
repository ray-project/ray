"""Integration tests for boolean/logical expression operations.

These tests require Ray and test end-to-end boolean expression evaluation.
"""

import pandas as pd
import pytest
from packaging.version import parse as parse_version

import ray
from ray.data._internal.utils.arrow_utils import get_pyarrow_version
from ray.data.expressions import col, lit
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

pytestmark = pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="Expression integration tests require PyArrow >= 20.0.0",
)


class TestBooleanIntegration:
    """Integration tests for boolean expressions with Ray Dataset."""

    def test_boolean_filter_with_dataset(self, ray_start_regular_shared):
        """Test boolean expressions used for filtering with Ray Dataset."""
        ds = ray.data.from_items(
            [
                {"age": 17, "is_student": True, "name": "Alice"},
                {"age": 21, "is_student": True, "name": "Bob"},
                {"age": 25, "is_student": False, "name": "Charlie"},
                {"age": 30, "is_student": False, "name": "Diana"},
            ]
        )

        # Add boolean columns using expressions
        result = (
            ds.with_column("is_adult", col("age") >= 18)
            .with_column("adult_student", (col("age") >= 18) & col("is_student"))
            .with_column("minor_or_student", (col("age") < 18) | col("is_student"))
            .to_pandas()
        )

        expected = pd.DataFrame(
            {
                "age": [17, 21, 25, 30],
                "is_student": [True, True, False, False],
                "name": ["Alice", "Bob", "Charlie", "Diana"],
                "is_adult": [False, True, True, True],
                "adult_student": [False, True, False, False],
                "minor_or_student": [True, True, False, False],
            }
        )
        pd.testing.assert_frame_equal(result, expected, check_dtype=False)

    def test_complex_boolean_with_dataset(self, ray_start_regular_shared):
        """Test complex boolean expressions with Ray Dataset."""
        ds = ray.data.from_items(
            [
                {"score": 85, "passed": True, "bonus": False},
                {"score": 70, "passed": True, "bonus": True},
                {"score": 45, "passed": False, "bonus": False},
            ]
        )

        # Complex: (score > 80) OR (passed AND bonus)
        result = ds.with_column(
            "eligible", (col("score") > 80) | (col("passed") & col("bonus"))
        ).to_pandas()

        expected = pd.DataFrame(
            {
                "score": [85, 70, 45],
                "passed": [True, True, False],
                "bonus": [False, True, False],
                "eligible": [True, True, False],
            }
        )
        pd.testing.assert_frame_equal(result, expected, check_dtype=False)

    def test_logical_not_with_dataset(self, ray_start_regular_shared):
        """Test logical NOT operation with Ray Dataset."""
        ds = ray.data.range(5)
        result = ds.with_column("result", ~(col("id") == 2)).to_pandas()
        expected = pd.DataFrame(
            {"id": [0, 1, 2, 3, 4], "result": [True, True, False, True, True]}
        )
        pd.testing.assert_frame_equal(result, expected, check_dtype=False)

    @pytest.mark.parametrize(
        "expression_factory,expected_results,test_id",
        [
            pytest.param(
                lambda: (col("age") > 18) & (col("country") == "USA"),
                [True, False, False],
                "complex_and",
            ),
            pytest.param(
                lambda: (col("age") < 18) | (col("country") == "USA"),
                [True, True, False],
                "complex_or",
            ),
            pytest.param(
                lambda: ~((col("age") < 25) & (col("country") != "USA")),
                [True, False, True],
                "complex_not",
            ),
            pytest.param(
                lambda: (col("age") >= 21)
                & (col("score") >= 10)
                & col("active").is_not_null()
                & (col("active") == lit(True)),
                [True, False, False],
                "eligibility_flag",
            ),
        ],
    )
    def test_complex_boolean_expressions_with_dataset(
        self, ray_start_regular_shared, expression_factory, expected_results, test_id
    ):
        """Test complex boolean expressions with Ray Dataset."""
        test_data = [
            {"age": 25, "country": "USA", "active": True, "score": 20},
            {"age": 17, "country": "Canada", "active": False, "score": 10},
            {"age": 30, "country": "UK", "active": None, "score": 20},
        ]

        ds = ray.data.from_items(test_data)
        expression = expression_factory()
        result = ds.with_column("result", expression).to_pandas()

        expected = pd.DataFrame(
            {
                "age": [25, 17, 30],
                "country": ["USA", "Canada", "UK"],
                "active": [True, False, None],
                "score": [20, 10, 20],
                "result": expected_results,
            }
        )
        pd.testing.assert_frame_equal(result, expected, check_dtype=False)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
