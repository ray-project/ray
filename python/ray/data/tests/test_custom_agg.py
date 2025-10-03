import numpy as np
import pytest

import ray
from ray.data.aggregate import MissingValuePercentage, ZeroPercentage
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
