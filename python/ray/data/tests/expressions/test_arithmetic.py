"""Integration tests for arithmetic expression operations.

These tests require Ray and test end-to-end arithmetic expression evaluation.
"""

import math

import pandas as pd
import pytest
from packaging.version import parse as parse_version

import ray
from ray.data._internal.util import rows_same
from ray.data._internal.utils.arrow_utils import get_pyarrow_version
from ray.data.expressions import col, lit
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

pytestmark = pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="Expression integration tests require PyArrow >= 20.0.0",
)


class TestArithmeticIntegration:
    """Integration tests for arithmetic expressions with Ray Dataset."""

    def test_arithmetic_with_dataset(self, ray_start_regular_shared):
        """Test arithmetic expressions work correctly with Ray Dataset."""
        ds = ray.data.from_items(
            [
                {"price": 10.0, "quantity": 2},
                {"price": 20.0, "quantity": 3},
                {"price": 15.0, "quantity": 4},
            ]
        )

        result = ds.with_column("total", col("price") * col("quantity")).to_pandas()
        expected = pd.DataFrame(
            {
                "price": [10.0, 20.0, 15.0],
                "quantity": [2, 3, 4],
                "total": [20.0, 60.0, 60.0],
            }
        )
        assert rows_same(result, expected)

    def test_chained_arithmetic_with_dataset(self, ray_start_regular_shared):
        """Test chained arithmetic expressions with Ray Dataset."""
        ds = ray.data.from_items(
            [
                {"a": 10, "b": 5},
                {"a": 20, "b": 3},
            ]
        )

        result = (
            ds.with_column("sum", col("a") + col("b"))
            .with_column("diff", col("a") - col("b"))
            .with_column("product", col("a") * col("b"))
            .to_pandas()
        )

        expected = pd.DataFrame(
            {
                "a": [10, 20],
                "b": [5, 3],
                "sum": [15, 23],
                "diff": [5, 17],
                "product": [50, 60],
            }
        )
        assert rows_same(result, expected)

    def test_floor_division_with_dataset(self, ray_start_regular_shared):
        """Test floor division operations with Ray Dataset."""
        ds = ray.data.range(5)
        result = ds.with_column("result", col("id") // 2).to_pandas()
        expected = pd.DataFrame({"id": [0, 1, 2, 3, 4], "result": [0, 0, 1, 1, 2]})
        assert rows_same(result, expected)

    def test_literal_floor_division_with_dataset(self, ray_start_regular_shared):
        """Test literal floor division by expression with Ray Dataset."""
        ds = ray.data.range(5)
        result = ds.with_column("result", lit(10) // (col("id") + 2)).to_pandas()
        expected = pd.DataFrame({"id": [0, 1, 2, 3, 4], "result": [5, 3, 2, 2, 1]})
        assert rows_same(result, expected)

    @pytest.mark.parametrize(
        "expr_factory,expected_values",
        [
            pytest.param(lambda: col("value").ceil(), [-1, 0, 0, 1, 2], id="ceil"),
            pytest.param(lambda: col("value").floor(), [-2, -1, 0, 0, 1], id="floor"),
            pytest.param(lambda: col("value").round(), [-2, 0, 0, 0, 2], id="round"),
            pytest.param(lambda: col("value").trunc(), [-1, 0, 0, 0, 1], id="trunc"),
        ],
    )
    def test_rounding_with_dataset(
        self, ray_start_regular_shared, expr_factory, expected_values
    ):
        """Test rounding operations with Ray Dataset."""
        values = [-1.75, -0.25, 0.0, 0.25, 1.75]
        ds = ray.data.from_items([{"value": v} for v in values])
        result = ds.with_column("result", expr_factory()).to_pandas()
        expected = pd.DataFrame({"value": values, "result": expected_values})
        assert rows_same(result, expected)

    @pytest.mark.parametrize(
        "expr_factory,expected_fn",
        [
            pytest.param(lambda: col("value").ln(), math.log, id="ln"),
            pytest.param(lambda: col("value").log10(), math.log10, id="log10"),
            pytest.param(lambda: col("value").log2(), math.log2, id="log2"),
            pytest.param(lambda: col("value").exp(), math.exp, id="exp"),
        ],
    )
    def test_logarithmic_with_dataset(
        self, ray_start_regular_shared, expr_factory, expected_fn
    ):
        """Test logarithmic operations with Ray Dataset."""
        values = [1.0, math.e, 10.0, 4.0]
        ds = ray.data.from_items([{"value": v} for v in values])
        expected_values = [expected_fn(v) for v in values]
        result = ds.with_column("result", expr_factory()).to_pandas()
        expected = pd.DataFrame({"value": values, "result": expected_values})
        assert rows_same(result, expected)

    @pytest.mark.parametrize(
        "expr_factory,expected_fn",
        [
            pytest.param(lambda: col("value").sin(), math.sin, id="sin"),
            pytest.param(lambda: col("value").cos(), math.cos, id="cos"),
            pytest.param(lambda: col("value").tan(), math.tan, id="tan"),
            pytest.param(lambda: col("value").atan(), math.atan, id="atan"),
        ],
    )
    def test_trigonometric_with_dataset(
        self, ray_start_regular_shared, expr_factory, expected_fn
    ):
        """Test trigonometric operations with Ray Dataset."""
        values = [0.0, math.pi / 6, math.pi / 4, math.pi / 3]
        ds = ray.data.from_items([{"value": v} for v in values])
        expected_values = [expected_fn(v) for v in values]
        result = ds.with_column("result", expr_factory()).to_pandas()
        expected = pd.DataFrame({"value": values, "result": expected_values})
        assert rows_same(result, expected)

    @pytest.mark.parametrize(
        "test_data,expr_factory,expected_results",
        [
            pytest.param(
                [{"x": 5}, {"x": -3}, {"x": 0}],
                lambda: col("x").negate(),
                [-5, 3, 0],
                id="negate",
            ),
            pytest.param(
                [{"x": 5}, {"x": -3}, {"x": 0}],
                lambda: col("x").sign(),
                [1, -1, 0],
                id="sign",
            ),
            pytest.param(
                [{"x": 5}, {"x": -3}, {"x": 0}],
                lambda: col("x").abs(),
                [5, 3, 0],
                id="abs",
            ),
            pytest.param(
                [{"x": 2}, {"x": 3}, {"x": 4}],
                lambda: col("x").power(2),
                [4, 9, 16],
                id="power_int",
            ),
            pytest.param(
                [{"x": 4}, {"x": 9}, {"x": 16}],
                lambda: col("x").power(0.5),
                [2.0, 3.0, 4.0],
                id="power_sqrt",
            ),
        ],
    )
    def test_arithmetic_helpers_with_dataset(
        self, ray_start_regular_shared, test_data, expr_factory, expected_results
    ):
        """Test arithmetic helper operations with Ray Dataset."""
        ds = ray.data.from_items(test_data)
        result = ds.with_column("result", expr_factory()).to_pandas()
        expected = pd.DataFrame(test_data)
        expected["result"] = expected_results
        assert rows_same(result, expected)

    def test_age_group_calculation_with_dataset(self, ray_start_regular_shared):
        """Test floor division for grouping values (e.g., age into decades)."""
        test_data = [
            {"age": 25},
            {"age": 17},
            {"age": 30},
        ]
        ds = ray.data.from_items(test_data)
        result = ds.with_column("age_group", col("age") // 10 * 10).to_pandas()
        expected = pd.DataFrame({"age": [25, 17, 30], "age_group": [20, 10, 30]})
        assert rows_same(result, expected)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
