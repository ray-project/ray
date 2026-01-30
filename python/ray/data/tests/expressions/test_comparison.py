"""Integration tests for comparison expression operations.

These tests require Ray and test end-to-end comparison expression evaluation.
"""

import pandas as pd
import pytest
from packaging.version import parse as parse_version

import ray
from ray.data._internal.utils.arrow_utils import get_pyarrow_version
from ray.data.expressions import col
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

pytestmark = pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="Expression integration tests require PyArrow >= 20.0.0",
)


class TestComparisonIntegration:
    """Integration tests for comparison expressions with Ray Dataset."""

    def test_comparison_with_dataset(self, ray_start_regular_shared):
        """Test comparison expressions work correctly with Ray Dataset."""
        ds = ray.data.from_items(
            [
                {"age": 17, "name": "Alice"},
                {"age": 21, "name": "Bob"},
                {"age": 25, "name": "Charlie"},
                {"age": 18, "name": "Diana"},
            ]
        )

        result = ds.with_column("is_adult", col("age") >= 18).to_pandas()
        expected = pd.DataFrame(
            {
                "age": [17, 21, 25, 18],
                "name": ["Alice", "Bob", "Charlie", "Diana"],
                "is_adult": [False, True, True, True],
            }
        )
        pd.testing.assert_frame_equal(result, expected, check_dtype=False)

    def test_multiple_comparisons_with_dataset(self, ray_start_regular_shared):
        """Test multiple comparison expressions with Ray Dataset."""
        ds = ray.data.from_items(
            [
                {"score": 45, "passing": 50},
                {"score": 75, "passing": 50},
                {"score": 50, "passing": 50},
            ]
        )

        result = (
            ds.with_column("passed", col("score") >= col("passing"))
            .with_column("failed", col("score") < col("passing"))
            .with_column("borderline", col("score") == col("passing"))
            .to_pandas()
        )

        expected = pd.DataFrame(
            {
                "score": [45, 75, 50],
                "passing": [50, 50, 50],
                "passed": [False, True, True],
                "failed": [True, False, False],
                "borderline": [False, False, True],
            }
        )
        pd.testing.assert_frame_equal(result, expected, check_dtype=False)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
