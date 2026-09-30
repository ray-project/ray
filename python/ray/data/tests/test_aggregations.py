from collections import Counter

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data.aggregate import (
    ApproximateQuantile,
    ApproximateTopK,
    MissingValuePercentage,
    TopKUnique,
    Unique,
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


class TestTopKUnique:
    """Test cases for TopKUnique aggregation."""

    @pytest.fixture(autouse=True)
    def force_per_block_partial_aggregation(self):
        """Makes multi-block datasets produce one partial aggregate per block.

        By default Ray batches shuffle inputs into a single map task (up to
        ``shuffle_input_batch_bytes``), which reduces each group to one partial
        aggregate on small test data and leaves the cross-partial merge untested.
        """
        ctx = ray.data.DataContext.get_current()
        original = ctx.shuffle_input_batch_bytes
        ctx.shuffle_input_batch_bytes = 1
        yield
        ctx.shuffle_input_batch_bytes = original

    def test_topk_unique_basic(self, ray_start_regular_shared_2_cpus):
        """Test basic exact top-k by global frequency."""
        data = [
            *[{"word": "apple"} for _ in range(5)],
            *[{"word": "banana"} for _ in range(3)],
            *[{"word": "cherry"} for _ in range(2)],
        ]
        ds = ray.data.from_items(data)
        result = ds.aggregate(TopKUnique(on="word", k=2))
        assert result["topk_unique(word)"] == ["apple", "banana"]

    def test_topk_unique_global_ranking_across_blocks(
        self, ray_start_regular_shared_2_cpus
    ):
        """A value that never wins within any single block must still win globally.

        With per-block top-1 semantics, "c" (3+3+3=9 occurrences, never a block
        winner) would be dropped in favor of per-block winners "a" (4) and
        "b" (4+2=6). Exact global counting must return "c".
        """
        data = [
            # Block 1: a wins locally.
            *[{"v": "a"} for _ in range(4)],
            *[{"v": "c"} for _ in range(3)],
            # Block 2: b wins locally.
            *[{"v": "b"} for _ in range(4)],
            *[{"v": "c"} for _ in range(3)],
            # Block 3: b wins locally.
            *[{"v": "b"} for _ in range(2)],
            *[{"v": "c"} for _ in range(3)],
        ]
        ds = ray.data.from_items(data, override_num_blocks=3)
        result = ds.aggregate(TopKUnique(on="v", k=1))
        assert result["topk_unique(v)"] == ["c"]

    def test_topk_unique_deterministic_tiebreak(self, ray_start_regular_shared_2_cpus):
        """Equal counts are broken by value order, deterministically."""
        data = [
            *[{"v": "zebra"} for _ in range(2)],
            *[{"v": "apple"} for _ in range(2)],
            *[{"v": "mango"} for _ in range(2)],
        ]
        ds = ray.data.from_items(data, override_num_blocks=2)
        result = ds.aggregate(TopKUnique(on="v", k=2))
        assert result["topk_unique(v)"] == ["apple", "mango"]

    def test_topk_unique_custom_alias(self, ray_start_regular_shared_2_cpus):
        """Test custom alias name."""
        data = [{"item": "x"}, {"item": "x"}, {"item": "y"}]
        ds = ray.data.from_items(data)
        result = ds.aggregate(TopKUnique(on="item", k=1, alias_name="top_items"))
        assert result["top_items"] == ["x"]

    def test_topk_unique_groupby(self, ray_start_regular_shared_2_cpus):
        """Test top-k per group."""
        data = [
            *[{"category": "A", "item": "apple"} for _ in range(5)],
            *[{"category": "A", "item": "banana"} for _ in range(3)],
            *[{"category": "B", "item": "cherry"} for _ in range(4)],
            *[{"category": "B", "item": "date"} for _ in range(2)],
        ]
        ds = ray.data.from_items(data)
        result = ds.groupby("category").aggregate(TopKUnique(on="item", k=1)).take_all()

        result_by_category = {
            row["category"]: row["topk_unique(item)"] for row in result
        }
        assert result_by_category["A"] == ["apple"]
        assert result_by_category["B"] == ["cherry"]

    def test_topk_unique_fewer_items_than_k(self, ray_start_regular_shared_2_cpus):
        """Fewer distinct values than k returns all of them."""
        data = [{"id": "a"}, {"id": "b"}]
        ds = ray.data.from_items(data)
        result = ds.aggregate(TopKUnique(on="id", k=5))
        assert sorted(result["topk_unique(id)"]) == ["a", "b"]

    def test_topk_unique_counts_nulls_by_default(self, ray_start_regular_shared_2_cpus):
        """With ignore_nulls=False (default), nulls compete for top-k slots."""
        data = [
            *[{"v": None} for _ in range(5)],
            *[{"v": "a"} for _ in range(3)],
            *[{"v": "b"} for _ in range(1)],
        ]
        ds = ray.data.from_items(data, override_num_blocks=2)
        result = ds.aggregate(TopKUnique(on="v", k=2))
        assert result["topk_unique(v)"] == [None, "a"]

    def test_topk_unique_ignore_nulls(self, ray_start_regular_shared_2_cpus):
        """With ignore_nulls=True, nulls are excluded from counting."""
        data = [
            *[{"v": None} for _ in range(5)],
            *[{"v": "a"} for _ in range(3)],
            *[{"v": "b"} for _ in range(1)],
        ]
        ds = ray.data.from_items(data, override_num_blocks=2)
        result = ds.aggregate(TopKUnique(on="v", k=2, ignore_nulls=True))
        assert result["topk_unique(v)"] == ["a", "b"]

    def test_topk_unique_encode_lists(self, ray_start_regular_shared_2_cpus):
        """With encode_lists=True, list elements are counted individually."""
        data = [
            {"tokens": ["a", "b", "a"]},
            {"tokens": ["a", "c"]},
            {"tokens": ["b"]},
        ]
        ds = ray.data.from_items(data)
        result = ds.aggregate(
            TopKUnique(on="tokens", k=2, ignore_nulls=True, encode_lists=True)
        )
        assert result["topk_unique(tokens)"] == ["a", "b"]

    def test_topk_unique_whole_lists_as_values(self, ray_start_regular_shared_2_cpus):
        """With encode_lists=False, whole lists are counted as single values."""
        data = [
            {"tokens": ["a", "b"]},
            {"tokens": ["a", "b"]},
            {"tokens": ["c"]},
        ]
        ds = ray.data.from_items(data, override_num_blocks=2)
        result = ds.aggregate(TopKUnique(on="tokens", k=1, ignore_nulls=True))
        # Values are counted as tuples internally (for hashability), but the
        # result round-trips through Arrow, which represents them as lists.
        assert [list(v) for v in result["topk_unique(tokens)"]] == [["a", "b"]]

    def test_topk_unique_invalid_k(self, ray_start_regular_shared_2_cpus):
        """k must be positive."""
        with pytest.raises(ValueError, match="`k` must be a positive integer"):
            TopKUnique(on="v", k=0)

    # ----- Column-wise merge (VectorizedAggregateFnV2._combine_column) -----

    @staticmethod
    def _accumulator_column(accumulators):
        """The column of partial accumulators the engine hands `_combine_column`
        for one group (a null row stands for an empty group)."""
        return pa.chunked_array([pa.array(accumulators)])

    @staticmethod
    def _as_counts(accumulator):
        """Order-insensitive {value: count} view (lists compare as tuples)."""
        return {
            tuple(v) if isinstance(v, list) else v: c
            for v, c in zip(accumulator["values"], accumulator["counts"])
        }

    @classmethod
    def _pairwise_merge(cls, agg, accumulators):
        merged = {"values": [], "counts": []}
        for accumulator in accumulators:
            if accumulator is not None:
                merged = agg.combine(merged, accumulator)
        return merged

    @pytest.mark.parametrize(
        "blocks",
        [
            pytest.param(
                [
                    pa.table({"v": ["a", "b", None, "a"]}),
                    pa.table({"v": ["b", "c", None]}),
                    pa.table({"v": pa.array([], pa.string())}),
                    pa.table({"v": ["c", "c", "d"]}),
                ],
                id="strings_with_nulls_and_empty_block",
            ),
            pytest.param(
                [
                    pa.table({"v": [1, 2, 2, None]}),
                    pa.table({"v": [3, 2, None, None]}),
                ],
                id="ints_with_nulls",
            ),
        ],
    )
    def test_topk_unique_combine_column_matches_pairwise_combine(self, blocks):
        agg = TopKUnique(on="v", k=2)
        partials = [agg.aggregate_block(block) for block in blocks]
        # A null accumulator row (an empty group) must be tolerated.
        accumulators = [partials[0], None, *partials[1:]]

        vectorized = agg._combine_column(self._accumulator_column(accumulators))
        pairwise = self._pairwise_merge(agg, accumulators)

        assert self._as_counts(vectorized) == self._as_counts(pairwise)

    def test_topk_unique_combine_column_is_recombinable(self):
        """The reduce side re-combines partially combined blocks (compaction)
        before finalizing, so `_combine_column` must keep counts and return a
        valid accumulator, not a pruned top-k."""
        agg = TopKUnique(on="v", k=1)
        partials = [
            agg.aggregate_block(pa.table({"v": values}))
            for values in (
                ["a"] * 4 + ["c"] * 3,
                ["b"] * 4 + ["c"] * 3,
                ["b"] * 2 + ["c"] * 3,
                ["a", "d"],
            )
        ]

        one_shot = agg._combine_column(self._accumulator_column(partials))
        first = agg._combine_column(self._accumulator_column(partials[:2]))
        second = agg._combine_column(self._accumulator_column(partials[2:]))
        compacted = agg._combine_column(self._accumulator_column([first, second]))

        assert self._as_counts(compacted) == self._as_counts(one_shot)
        assert self._as_counts(one_shot) == {"a": 5, "b": 6, "c": 9, "d": 1}
        assert agg.finalize(compacted) == ["c"]

    def test_topk_unique_combine_column_whole_list_fallback(self):
        """Whole-list values can't be grouped by Arrow; the Python merge must
        produce the same counts."""
        agg = TopKUnique(on="t", k=1)
        accumulators = [
            agg.aggregate_block(pa.table({"t": [["a", "b"], ["a", "b"], None]})),
            agg.aggregate_block(pa.table({"t": [["c"], ["a", "b"], None]})),
        ]

        merged = agg._combine_column(self._accumulator_column(accumulators))

        assert self._as_counts(merged) == {("a", "b"): 3, ("c",): 1, None: 2}

    @pytest.mark.parametrize(
        "block",
        [
            pytest.param(
                pd.DataFrame({"t": [["a"], ["a"], np.nan, pd.NA, None, ["b"]]}),
                id="pandas_nan_and_pd_na",
            ),
            pytest.param(
                pa.table({"t": pa.array([["a"], ["a"], None, None, None, ["b"]])}),
                id="arrow_null_rows",
            ),
        ],
    )
    def test_topk_unique_whole_list_rows_with_missing_values(self, block):
        """Missing rows in a list column (None, NaN, pd.NA) aren't iterable and
        must be counted as nulls rather than crash."""
        agg = TopKUnique(on="t", k=3)

        accumulator = agg.aggregate_block(block)

        assert self._as_counts(accumulator) == {("a",): 2, ("b",): 1, None: 3}
        # Ranked by count (None: 3, ("a",): 2, ("b",): 1).
        assert agg.finalize(accumulator) == [None, ("a",), ("b",)]

    # ----- Deterministic ranking (count desc, value asc, nulls last) -----

    def test_topk_unique_null_tie_keeps_natural_value_order(
        self, ray_start_regular_shared_2_cpus
    ):
        """A null tying on count must not push the ranking of the other values
        onto string order ("10" < "2")."""
        data = [{"v": 9}, {"v": 10}, {"v": 2}, {"v": None}]
        ds = ray.data.from_items(data, override_num_blocks=2)

        result = ds.aggregate(TopKUnique(on="v", k=4))

        assert result["topk_unique(v)"] == [2, 9, 10, None]

    def test_topk_unique_finalize_unsortable_values(self):
        """Values Arrow can't sort (whole lists) rank with the same ordering."""
        agg = TopKUnique(on="t", k=3)
        accumulator = {"values": [("b",), None, ("a",)], "counts": [1, 1, 1]}

        assert agg.finalize(accumulator) == [("a",), ("b",), None]

    def test_topk_unique_finalize_mixed_types_is_deterministic(self):
        agg = TopKUnique(on="v", k=3)
        accumulator = {"values": ["a", 1, None], "counts": [1, 1, 1]}

        expected = agg.finalize(accumulator)
        assert expected == [1, "a", None]
        assert agg.finalize({"values": [1, None, "a"], "counts": [1, 1, 1]}) == expected

    def test_topk_unique_groupby_many_groups_and_blocks(
        self, ray_start_regular_shared_2_cpus
    ):
        """Each group's partial counts are spread across many blocks."""
        import random

        num_groups = 20
        rows = []
        for g in range(num_groups):
            rows += [{"g": g, "v": f"top{g}"} for _ in range(5)]
            rows += [{"g": g, "v": "common"} for _ in range(3)]
            rows += [{"g": g, "v": "rare"}]
        random.Random(0).shuffle(rows)
        ds = ray.data.from_items(rows, override_num_blocks=8)

        result = ds.groupby("g").aggregate(TopKUnique(on="v", k=2)).take_all()

        assert len(result) == num_groups
        for row in result:
            assert row["topk_unique(v)"] == [f"top{row['g']}", "common"]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
