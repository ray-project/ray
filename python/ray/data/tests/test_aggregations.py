from collections import Counter

import numpy as np
import pytest

import ray
from ray.data.aggregate import (
    ApproximateQuantile,
    ApproximateTopK,
    MissingValuePercentage,
    TopKUnique,
    Unique,
    ValueCounter,
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

    @pytest.mark.parametrize(
        ("data", "expected1", "expected2"),
        [
            (
                [{"id": 1}, {"id": 1}, {"id": 2}],
                {"id": 1, "count": 2},
                {"id": 2, "count": 1},
            ),
            (
                [{"id": [1, 2, 3]}, {"id": [1, 2, 3]}, {"id": [1, 2]}],
                {"id": [1, 2, 3], "count": 2},
                {"id": [1, 2], "count": 1},
            ),
        ],
    )
    def test_approximate_topk_non_string_datatype(
        self, data, expected1, expected2, ray_start_regular_shared_2_cpus
    ):
        """Test that ApproximateTopK works with non-string type elements."""
        ds = ray.data.from_items(data)

        result = ds.aggregate(ApproximateTopK(on="id", k=2, log_capacity=3))
        assert result["approx_topk(id)"][0] == expected1
        assert result["approx_topk(id)"][1] == expected2

    def test_approximate_topk_encode_lists(self, ray_start_regular_shared_2_cpus):
        """Test ApproximateTopK list encode feature."""
        data = [{"id": [1, 1, 1]}, {"id": [2, 2]}, {"id": [3]}]
        ds = ray.data.from_items(data)
        result = ds.aggregate(
            ApproximateTopK(on="id", k=4, log_capacity=10, encode_lists=True)
        )
        assert result["approx_topk(id)"][0] == {"id": 1, "count": 3}
        assert result["approx_topk(id)"][1] == {"id": 2, "count": 2}
        assert result["approx_topk(id)"][2] == {"id": 3, "count": 1}


class TestUnique:
    """Test cases for Unique aggregation."""

    def test_unique_basic(self, ray_start_regular_shared_2_cpus):
        """Test basic Unique aggregation."""
        data = [{"id": "a"}, {"id": "b"}, {"id": "b"}, {"id": None}]
        ds = ray.data.from_items(data)
        result = ds.aggregate(Unique(on="id", ignore_nulls=False))

        assert Counter(result["unique(id)"]) == Counter(["a", "b", None])

    def test_unique_ignores_nulls(self, ray_start_regular_shared_2_cpus):
        """Test Unique properly ignores nulls."""
        data = [{"id": "a"}, {"id": None}, {"id": "b"}, {"id": "b"}, {"id": None}]
        ds = ray.data.from_items(data)
        result = ds.aggregate(Unique(on="id", ignore_nulls=True))

        assert Counter(result["unique(id)"]) == Counter(["a", "b"])

    def test_unique_custom_alias(self, ray_start_regular_shared_2_cpus):
        """Test Unique with custom alias."""
        data = [{"id": "a"}, {"id": "b"}, {"id": "b"}]
        ds = ray.data.from_items(data)
        result = ds.aggregate(Unique(on="id", alias_name="custom"))

        assert sorted(result["custom"]) == ["a", "b"]

    def test_unique_list_datatype(self, ray_start_regular_shared_2_cpus):
        """Test Unique works with non-hashable types like list."""
        data = [
            {"id": ["a", "b", "c"]},
            {"id": ["a", "b", "c"]},
            {"id": ["a", "b", "c"]},
        ]
        ds = ray.data.from_items(data)
        result = ds.aggregate(Unique(on="id"))

        assert result["unique(id)"][0] == ["a", "b", "c"]

    def test_unique_encode_lists(self, ray_start_regular_shared_2_cpus):
        """Test Unique works when encode_lists is True."""
        data = [{"id": ["a", "b", "c"]}, {"id": ["a", "a", "a", "b", None]}]
        ds = ray.data.from_items(data)
        result = ds.aggregate(Unique(on="id", encode_lists=True, ignore_nulls=False))

        answer = ["a", "b", "c", None]

        assert Counter(result["unique(id)"]) == Counter(answer)

    def test_unique_encode_lists_ignores_nulls(self, ray_start_regular_shared_2_cpus):
        """Test Unique will drop null values when encode_lists is True."""
        data = [{"id": ["a", "b", "c"]}, {"id": ["a", "a", "a", "b", None]}]
        ds = ray.data.from_items(data)
        result = ds.aggregate(Unique(on="id", encode_lists=True, ignore_nulls=True))

        answer = ["a", "b", "c"]

        assert Counter(result["unique(id)"]) == Counter(answer)


class TestValueCounter:
    """Test cases for ValueCounter aggregation."""

    def test_value_counter_basic(self, ray_start_regular_shared_2_cpus):
        """Test basic value counting."""
        data = [
            {"category": "A"},
            {"category": "B"},
            {"category": "A"},
            {"category": "C"},
            {"category": "A"},
            {"category": "B"},
        ]
        ds = ray.data.from_items(data)

        result = ds.aggregate(ValueCounter(on="category"))
        vc = result["value_counter(category)"]

        # Convert to dict for easier comparison
        counts = dict(zip(vc["values"], vc["counts"]))
        assert counts == {"A": 3, "B": 2, "C": 1}

    def test_value_counter_custom_alias(self, ray_start_regular_shared_2_cpus):
        """Test value counting with custom alias."""
        data = [{"x": "a"}, {"x": "b"}, {"x": "a"}]
        ds = ray.data.from_items(data)

        result = ds.aggregate(ValueCounter(on="x", alias_name="my_counts"))
        assert "my_counts" in result
        vc = result["my_counts"]
        counts = dict(zip(vc["values"], vc["counts"]))
        assert counts == {"a": 2, "b": 1}

    def test_value_counter_groupby(self, ray_start_regular_shared_2_cpus):
        """Test value counting with groupby."""
        data = [
            {"group": "X", "category": "A"},
            {"group": "X", "category": "B"},
            {"group": "X", "category": "A"},
            {"group": "Y", "category": "A"},
            {"group": "Y", "category": "A"},
        ]
        ds = ray.data.from_items(data)

        result = ds.groupby("group").aggregate(ValueCounter(on="category")).take_all()

        result_by_group = {}
        for row in result:
            vc = row["value_counter(category)"]
            result_by_group[row["group"]] = dict(zip(vc["values"], vc["counts"]))

        assert result_by_group["X"] == {"A": 2, "B": 1}
        assert result_by_group["Y"] == {"A": 2}

    def test_value_counter_numeric_values(self, ray_start_regular_shared_2_cpus):
        """Test value counting with numeric values."""
        data = [{"n": 1}, {"n": 2}, {"n": 1}, {"n": 1}, {"n": 3}]
        ds = ray.data.from_items(data)

        result = ds.aggregate(ValueCounter(on="n"))
        vc = result["value_counter(n)"]
        counts = dict(zip(vc["values"], vc["counts"]))

        assert counts == {1: 3, 2: 1, 3: 1}

    def test_value_counter_large_dataset(self, ray_start_regular_shared_2_cpus):
        """Test value counting with a larger dataset across multiple blocks."""
        # Create data where values are distributed across blocks
        data = []
        for i in range(1000):
            data.append({"category": f"cat_{i % 10}"})

        ds = ray.data.from_items(data).repartition(10)

        result = ds.aggregate(ValueCounter(on="category"))
        vc = result["value_counter(category)"]
        counts = dict(zip(vc["values"], vc["counts"]))

        # Each category should appear 100 times
        for i in range(10):
            assert counts[f"cat_{i}"] == 100


class TestTopKUnique:
    """Test cases for TopKUnique aggregation."""

    def test_topk_unique_basic(self, ray_start_regular_shared_2_cpus):
        """Test basic top-k unique values."""
        data = [
            *[{"category": "A"} for _ in range(5)],
            *[{"category": "B"} for _ in range(3)],
            *[{"category": "C"} for _ in range(2)],
            *[{"category": "D"} for _ in range(1)],
        ]
        ds = ray.data.from_items(data)

        result = ds.aggregate(TopKUnique(on="category", k=2))
        top2 = result["top_k_unique(category)"]

        assert top2 == ["A", "B"]

    def test_topk_unique_global_frequency(self, ray_start_regular_shared_2_cpus):
        """Test that TopKUnique computes global top-k, not per-block top-k.

        This verifies that an element appearing across multiple blocks with
        high cumulative count is correctly identified as a top element.
        """
        # Block 1: A=101, B=99, C=52
        # Block 2: D=100, E=99, C=50
        # Global counts: C=102, A=101, D=100, B=99, E=99
        # Top 2 should be C and A
        block1_data = ["A"] * 101 + ["B"] * 99 + ["C"] * 52
        block2_data = ["D"] * 100 + ["E"] * 99 + ["C"] * 50

        ds = ray.data.from_items(
            [{"val": v} for v in block1_data + block2_data]
        ).repartition(2)

        result = ds.aggregate(TopKUnique(on="val", k=2))
        top2 = result["top_k_unique(val)"]

        assert "C" in top2, f"C should be in top 2 (count=102), but got {top2}"
        assert "A" in top2, f"A should be in top 2 (count=101), but got {top2}"
        assert len(top2) == 2

    def test_topk_unique_custom_alias(self, ray_start_regular_shared_2_cpus):
        """Test top-k unique with custom alias."""
        data = [{"x": "a"}, {"x": "b"}, {"x": "a"}, {"x": "c"}]
        ds = ray.data.from_items(data)

        result = ds.aggregate(TopKUnique(on="x", k=2, alias_name="top_items"))
        assert "top_items" in result
        assert result["top_items"][0] == "a"

    def test_topk_unique_fewer_than_k(self, ray_start_regular_shared_2_cpus):
        """Test top-k unique when fewer unique values than k exist."""
        data = [{"x": "a"}, {"x": "b"}, {"x": "a"}]
        ds = ray.data.from_items(data)

        result = ds.aggregate(TopKUnique(on="x", k=5))
        top = result["top_k_unique(x)"]

        # Should return only 2 items since that's all we have
        assert len(top) == 2
        assert top[0] == "a"  # Most frequent

    def test_topk_unique_encode_lists_true(self, ray_start_regular_shared_2_cpus):
        """Test top-k unique with list columns when encode_lists=True."""
        data = [
            {"tags": ["python", "java", "python"]},
            {"tags": ["java", "go"]},
            {"tags": ["python", "rust"]},
        ]
        ds = ray.data.from_items(data)

        result = ds.aggregate(TopKUnique(on="tags", k=2, encode_lists=True))
        top2 = result["top_k_unique(tags)"]

        # python appears 3 times, java 2 times
        assert top2[0] == "python"
        assert top2[1] == "java"

    def test_topk_unique_encode_lists_false(self, ray_start_regular_shared_2_cpus):
        """Test top-k unique with list columns when encode_lists=False (hash lists)."""
        data = [
            {"tags": ["a", "b"]},
            {"tags": ["a", "b"]},
            {"tags": ["c", "d"]},
        ]
        ds = ray.data.from_items(data)

        result = ds.aggregate(TopKUnique(on="tags", k=2, encode_lists=False))
        top = result["top_k_unique(tags)"]

        # Lists are hashed, so we get hashes back, not the original lists
        # Just verify we get the expected number of results
        assert len(top) == 2

    def test_topk_unique_groupby(self, ray_start_regular_shared_2_cpus):
        """Test top-k unique with groupby."""
        data = [
            *[{"group": "X", "item": "apple"} for _ in range(5)],
            *[{"group": "X", "item": "banana"} for _ in range(3)],
            *[{"group": "X", "item": "cherry"} for _ in range(1)],
            *[{"group": "Y", "item": "date"} for _ in range(4)],
            *[{"group": "Y", "item": "elderberry"} for _ in range(2)],
        ]
        ds = ray.data.from_items(data)

        result = (
            ds.groupby("group")
            .aggregate(TopKUnique(on="item", k=2))
            .take_all()
        )

        result_by_group = {row["group"]: row["top_k_unique(item)"] for row in result}

        assert result_by_group["X"] == ["apple", "banana"]
        assert result_by_group["Y"] == ["date", "elderberry"]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
