import numpy as np
import pytest

import ray
from ray.data.aggregate import (
    ApproximateQuantile,
    ApproximateTopK,
    MissingValuePercentage,
    ZeroPercentage,
)
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


class TestMissingValuePercentage:
    """Test cases for MissingValuePercentage aggregation."""

    def test_missing_value_percentage_basic(self, ray_start_regular_shared_2_cpus):
        """Test basic missing value percentage calculation."""
        # Create test data with some null values
        data = [
            {"id": 1, "value": 10},
            {"id": 2, "value": None},
            {"id": 3, "value": 30},
            {"id": 4, "value": None},
            {"id": 5, "value": 50},
        ]
        ds = ray.data.from_items(data)

        result = ds.aggregate(MissingValuePercentage(on="value"))
        expected = 40.0  # 2 nulls out of 5 total = 40%

        assert result["missing_pct(value)"] == expected

    def test_missing_value_percentage_no_nulls(self, ray_start_regular_shared_2_cpus):
        """Test missing value percentage with no null values."""
        data = [
            {"id": 1, "value": 10},
            {"id": 2, "value": 20},
            {"id": 3, "value": 30},
        ]
        ds = ray.data.from_items(data)

        result = ds.aggregate(MissingValuePercentage(on="value"))
        expected = 0.0  # 0 nulls out of 3 total = 0%

        assert result["missing_pct(value)"] == expected

    def test_missing_value_percentage_all_nulls(self, ray_start_regular_shared_2_cpus):
        """Test missing value percentage with all null values."""
        data = [
            {"id": 1, "value": None},
            {"id": 2, "value": None},
            {"id": 3, "value": None},
        ]
        ds = ray.data.from_items(data)

        result = ds.aggregate(MissingValuePercentage(on="value"))
        expected = 100.0  # 3 nulls out of 3 total = 100%

        assert result["missing_pct(value)"] == expected

    def test_missing_value_percentage_with_nan(self, ray_start_regular_shared_2_cpus):
        """Test missing value percentage with NaN values."""
        data = [
            {"id": 1, "value": 10.0},
            {"id": 2, "value": np.nan},
            {"id": 3, "value": None},
            {"id": 4, "value": 40.0},
        ]
        ds = ray.data.from_items(data)

        result = ds.aggregate(MissingValuePercentage(on="value"))
        expected = 50.0  # 2 nulls (NaN + None) out of 4 total = 50%

        assert result["missing_pct(value)"] == expected

    def test_missing_value_percentage_with_string(
        self, ray_start_regular_shared_2_cpus
    ):
        """Test missing value percentage with string values."""
        data = [
            {"id": 1, "value": "a"},
            {"id": 2, "value": None},
            {"id": 3, "value": None},
            {"id": 4, "value": "b"},
        ]
        ds = ray.data.from_items(data)

        result = ds.aggregate(MissingValuePercentage(on="value"))
        expected = 50.0  # 2 None out of 4 total = 50%

        assert result["missing_pct(value)"] == expected

    def test_missing_value_percentage_custom_alias(
        self, ray_start_regular_shared_2_cpus
    ):
        """Test missing value percentage with custom alias name."""
        data = [
            {"id": 1, "value": 10},
            {"id": 2, "value": None},
        ]
        ds = ray.data.from_items(data)

        result = ds.aggregate(MissingValuePercentage(on="value", alias_name="null_pct"))
        expected = 50.0  # 1 null out of 2 total = 50%

        assert result["null_pct"] == expected

    def test_missing_value_percentage_large_dataset(
        self, ray_start_regular_shared_2_cpus
    ):
        """Test missing value percentage with larger dataset."""
        # Create a larger dataset with known null percentage
        data = []
        for i in range(1000):
            value = None if i % 10 == 0 else i  # 10% null values
            data.append({"id": i, "value": value})

        ds = ray.data.from_items(data)

        result = ds.aggregate(MissingValuePercentage(on="value"))
        expected = 10.0  # 100 nulls out of 1000 total = 10%

        assert abs(result["missing_pct(value)"] - expected) < 0.01


class TestZeroPercentage:
    """Test cases for ZeroPercentage aggregation."""

    def test_zero_percentage_basic(self, ray_start_regular_shared_2_cpus):
        """Test basic zero percentage calculation."""
        data = [
            {"id": 1, "value": 10},
            {"id": 2, "value": 0},
            {"id": 3, "value": 30},
            {"id": 4, "value": 0},
            {"id": 5, "value": 50},
        ]
        ds = ray.data.from_items(data)

        result = ds.aggregate(ZeroPercentage(on="value"))
        expected = 40.0  # 2 zeros out of 5 total = 40%

        assert result["zero_pct(value)"] == expected

    def test_zero_percentage_no_zeros(self, ray_start_regular_shared_2_cpus):
        """Test zero percentage with no zero values."""
        data = [
            {"id": 1, "value": 10},
            {"id": 2, "value": 20},
            {"id": 3, "value": 30},
        ]
        ds = ray.data.from_items(data)

        result = ds.aggregate(ZeroPercentage(on="value"))
        expected = 0.0  # 0 zeros out of 3 total = 0%

        assert result["zero_pct(value)"] == expected

    def test_zero_percentage_all_zeros(self, ray_start_regular_shared_2_cpus):
        """Test zero percentage with all zero values."""
        data = [
            {"id": 1, "value": 0},
            {"id": 2, "value": 0},
            {"id": 3, "value": 0},
        ]
        ds = ray.data.from_items(data)

        result = ds.aggregate(ZeroPercentage(on="value"))
        expected = 100.0  # 3 zeros out of 3 total = 100%

        assert result["zero_pct(value)"] == expected

    def test_zero_percentage_with_nulls_ignore_nulls_true(
        self, ray_start_regular_shared_2_cpus
    ):
        """Test zero percentage with null values when ignore_nulls=True."""
        data = [
            {"id": 1, "value": 10},
            {"id": 2, "value": 0},
            {"id": 3, "value": None},
            {"id": 4, "value": 0},
        ]
        ds = ray.data.from_items(data)

        result = ds.aggregate(ZeroPercentage(on="value", ignore_nulls=True))
        expected = 66.67  # 2 zeros out of 3 non-null values ≈ 66.67%

        assert abs(result["zero_pct(value)"] - expected) < 0.01

    def test_zero_percentage_with_nulls_ignore_nulls_false(
        self, ray_start_regular_shared_2_cpus
    ):
        """Test zero percentage with null values when ignore_nulls=False."""
        data = [
            {"id": 1, "value": 10},
            {"id": 2, "value": 0},
            {"id": 3, "value": None},
            {"id": 4, "value": 0},
        ]
        ds = ray.data.from_items(data)

        result = ds.aggregate(ZeroPercentage(on="value", ignore_nulls=False))
        expected = 50.0  # 2 zeros out of 4 total values = 50%

        assert result["zero_pct(value)"] == expected

    def test_zero_percentage_all_nulls(self, ray_start_regular_shared_2_cpus):
        """Test zero percentage with all null values."""
        data = [
            {"id": 1, "value": None},
            {"id": 2, "value": None},
            {"id": 3, "value": None},
        ]
        ds = ray.data.from_items(data)

        result = ds.aggregate(ZeroPercentage(on="value", ignore_nulls=True))
        expected = None  # No non-null values to calculate percentage

        assert result["zero_pct(value)"] == expected

    def test_zero_percentage_custom_alias(self, ray_start_regular_shared_2_cpus):
        """Test zero percentage with custom alias name."""
        data = [
            {"id": 1, "value": 10},
            {"id": 2, "value": 0},
        ]
        ds = ray.data.from_items(data)

        result = ds.aggregate(ZeroPercentage(on="value", alias_name="zero_ratio"))
        expected = 50.0  # 1 zero out of 2 total = 50%

        assert result["zero_ratio"] == expected

    def test_zero_percentage_large_dataset(self, ray_start_regular_shared_2_cpus):
        """Test zero percentage with larger dataset."""
        # Create a larger dataset with known zero percentage
        data = []
        for i in range(1000):
            value = 0 if i % 5 == 0 else i  # 20% zero values
            data.append({"id": i, "value": value})

        ds = ray.data.from_items(data)

        result = ds.aggregate(ZeroPercentage(on="value"))
        expected = 20.0  # 200 zeros out of 1000 total = 20%

        assert abs(result["zero_pct(value)"] - expected) < 0.01

    def test_zero_percentage_float_zeros(self, ray_start_regular_shared_2_cpus):
        """Test zero percentage with float zero values."""
        data = [
            {"id": 1, "value": 10.5},
            {"id": 2, "value": 0.0},
            {"id": 3, "value": 30.7},
            {"id": 4, "value": 0.0},
            {"id": 5, "value": 50.2},
        ]
        ds = ray.data.from_items(data)

        result = ds.aggregate(ZeroPercentage(on="value"))
        expected = 40.0  # 2 zeros out of 5 total = 40%

        assert result["zero_pct(value)"] == expected

    def test_zero_percentage_negative_values(self, ray_start_regular_shared_2_cpus):
        """Test zero percentage with negative values (zeros should still be counted)."""
        data = [
            {"id": 1, "value": -10},
            {"id": 2, "value": 0},
            {"id": 3, "value": 30},
            {"id": 4, "value": -5},
            {"id": 5, "value": 0},
        ]
        ds = ray.data.from_items(data)

        result = ds.aggregate(ZeroPercentage(on="value"))
        expected = 40.0  # 2 zeros out of 5 total = 40%

        assert result["zero_pct(value)"] == expected


class TestApproximateQuantile:
    """Test cases for ApproximateQuantile aggregation."""

    def test_approximate_quantile_basic(self, ray_start_regular_shared_2_cpus):
        """Test basic approximate quantile calculation."""
        data = [
            {
                "id": 1,
                "value": 10,
            },
            {"id": 2, "value": 0},
            {"id": 3, "value": 30},
            {"id": 4, "value": 0},
            {"id": 5, "value": 50},
        ]
        ds = ray.data.from_items(data)

        result = ds.aggregate(
            ApproximateQuantile(on="value", quantiles=[0.1, 0.5, 0.9])
        )
        expected = [0.0, 10.0, 50.0]
        assert result["approx_quantile(value)"] == expected

    def test_approximate_quantile_ignores_nulls(self, ray_start_regular_shared_2_cpus):
        data = [
            {"id": 1, "value": 5.0},
            {"id": 2, "value": None},
            {"id": 3, "value": 15.0},
            {"id": 4, "value": None},
            {"id": 5, "value": 25.0},
        ]
        ds = ray.data.from_items(data)

        result = ds.aggregate(ApproximateQuantile(on="value", quantiles=[0.5]))
        assert result["approx_quantile(value)"] == [15.0]

    def test_approximate_quantile_custom_alias(self, ray_start_regular_shared_2_cpus):
        data = [
            {"id": 1, "value": 1.0},
            {"id": 2, "value": 3.0},
            {"id": 3, "value": 5.0},
            {"id": 4, "value": 7.0},
            {"id": 5, "value": 9.0},
        ]
        ds = ray.data.from_items(data)

        quantiles = [0.0, 1.0]
        result = ds.aggregate(
            ApproximateQuantile(
                on="value", quantiles=quantiles, alias_name="value_range"
            )
        )

        assert result["value_range"] == [1.0, 9.0]
        assert len(result["value_range"]) == len(quantiles)

    def test_approximate_quantile_groupby(self, ray_start_regular_shared_2_cpus):
        data = [
            {"group": "A", "value": 1.0},
            {"group": "A", "value": 2.0},
            {"group": "A", "value": 3.0},
            {"group": "B", "value": 10.0},
            {"group": "B", "value": 20.0},
            {"group": "B", "value": 30.0},
        ]
        ds = ray.data.from_items(data)

        result = (
            ds.groupby("group")
            .aggregate(ApproximateQuantile(on="value", quantiles=[0.5]))
            .take_all()
        )

        result_by_group = {
            row["group"]: row["approx_quantile(value)"] for row in result
        }

        assert result_by_group["A"] == [2.0]
        assert result_by_group["B"] == [20.0]


class TestApproximateTopK:
    """Test cases for ApproximateTopK aggregation."""

    def test_approximate_topk_ignores_nulls(self, ray_start_regular_shared_2_cpus):
        """Test that null values are ignored."""
        data = [
            *[{"word": "apple"} for _ in range(5)],
            *[{"word": None} for _ in range(10)],
            *[{"word": "banana"} for _ in range(3)],
            *[{"word": "cherry"} for _ in range(2)],
        ]
        ds = ray.data.from_items(data)
        result = ds.aggregate(ApproximateTopK(on="word", k=2))
        assert result["approx_topk(word)"] == [
            {"word": "apple", "count": 5},
            {"word": "banana", "count": 3},
        ]

    def test_approximate_topk_custom_alias(self, ray_start_regular_shared_2_cpus):
        """Test approximate top k with custom alias."""
        data = [
            *[{"item": "x"} for _ in range(3)],
            *[{"item": "y"} for _ in range(2)],
            *[{"item": "z"} for _ in range(1)],
        ]
        ds = ray.data.from_items(data)
        result = ds.aggregate(ApproximateTopK(on="item", k=2, alias_name="top_items"))
        assert "top_items" in result
        assert result["top_items"] == [
            {"item": "x", "count": 3},
            {"item": "y", "count": 2},
        ]

    def test_approximate_topk_groupby(self, ray_start_regular_shared_2_cpus):
        """Test approximate top k with groupby."""
        data = [
            *[{"category": "A", "item": "apple"} for _ in range(5)],
            *[{"category": "A", "item": "banana"} for _ in range(3)],
            *[{"category": "B", "item": "cherry"} for _ in range(4)],
            *[{"category": "B", "item": "date"} for _ in range(2)],
        ]
        ds = ray.data.from_items(data)
        result = (
            ds.groupby("category").aggregate(ApproximateTopK(on="item", k=1)).take_all()
        )

        result_by_category = {
            row["category"]: row["approx_topk(item)"] for row in result
        }

        assert result_by_category["A"] == [{"item": "apple", "count": 5}]
        assert result_by_category["B"] == [{"item": "cherry", "count": 4}]

    def test_approximate_topk_all_unique(self, ray_start_regular_shared_2_cpus):
        """Test approximate top k when all items are unique."""
        data = [{"id": f"item_{i}"} for i in range(10)]
        ds = ray.data.from_items(data)
        result = ds.aggregate(ApproximateTopK(on="id", k=3))

        # All items have count 1, so we should get exactly 3 items
        assert len(result["approx_topk(id)"]) == 3
        for item in result["approx_topk(id)"]:
            assert item["count"] == 1

    def test_approximate_topk_fewer_items_than_k(self, ray_start_regular_shared_2_cpus):
        """Test approximate top k when dataset has fewer unique items than k."""
        data = [
            {"id": "a"},
            {"id": "b"},
        ]
        ds = ray.data.from_items(data)
        result = ds.aggregate(ApproximateTopK(on="id", k=5))

        # Should only return 2 items since that's all we have
        assert len(result["approx_topk(id)"]) == 2

    def test_approximate_topk_different_log_capacity(
        self, ray_start_regular_shared_2_cpus
    ):
        """Test that different log_capacity values still produce correct top k."""
        data = [
            *[{"id": "frequent"} for _ in range(100)],
            *[{"id": "common"} for _ in range(50)],
            *[{"id": f"rare_{i}"} for i in range(50)],  # 50 unique rare items
        ]
        ds = ray.data.from_items(data)

        # Test with smaller log_capacity
        result_small = ds.aggregate(ApproximateTopK(on="id", k=2, log_capacity=10))
        # Test with larger log_capacity
        result_large = ds.aggregate(ApproximateTopK(on="id", k=2, log_capacity=15))

        # Both should correctly identify the top 2
        for result in [result_small, result_large]:
            assert result["approx_topk(id)"][0] == {"id": "frequent", "count": 100}
            assert result["approx_topk(id)"][1] == {"id": "common", "count": 50}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
