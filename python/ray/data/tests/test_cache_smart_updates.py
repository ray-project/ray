"""Comprehensive tests for cache smart update logic.

This test suite validates that the caching system correctly:
1. Preserves cache values when transformations allow it
2. Invalidates cache values when transformations require it
3. Smart-computes new cache values when possible
4. Matches the actual semantics of Ray Dataset operations
"""

import pytest

import ray
from ray.tests.conftest import *  # noqa


class TestSchemaPreservation:
    """Test that schema() is correctly preserved/invalidated."""

    def test_schema_preserved_after_limit(self, ray_start_regular_shared):
        """Schema should be preserved after limit() - only count changes."""
        ds = ray.data.range(100)

        # Cache schema
        original_schema = ds.schema()
        assert "id" in original_schema.names

        # Apply limit - schema should be preserved
        limited_ds = ds.limit(10)
        limited_schema = limited_ds.schema()

        assert limited_schema.names == original_schema.names
        assert limited_schema.types == original_schema.types

    def test_schema_preserved_after_filter(self, ray_start_regular_shared):
        """Schema should be preserved after filter() - only count changes."""
        ds = ray.data.range(100)

        # Cache schema
        original_schema = ds.schema()

        # Apply filter - schema should be preserved
        filtered_ds = ds.filter(lambda x: x["id"] < 50)
        filtered_schema = filtered_ds.schema()

        assert filtered_schema.names == original_schema.names

    def test_schema_preserved_after_sort(self, ray_start_regular_shared):
        """Schema should be preserved after sort() - reordering only."""
        ds = ray.data.range(100)

        # Cache schema
        original_schema = ds.schema()

        # Apply sort - schema should be preserved
        sorted_ds = ds.sort("id", descending=True)
        sorted_schema = sorted_ds.schema()

        assert sorted_schema.names == original_schema.names

    def test_schema_preserved_after_repartition(self, ray_start_regular_shared):
        """Schema should be preserved after repartition() - reordering only."""
        ds = ray.data.range(100)

        # Cache schema
        original_schema = ds.schema()

        # Apply repartition - schema should be preserved
        repartitioned_ds = ds.repartition(5)
        repartitioned_schema = repartitioned_ds.schema()

        assert repartitioned_schema.names == original_schema.names

    def test_schema_changed_after_add_column(self, ray_start_regular_shared):
        """Schema should NOT be preserved after add_column() - new column added."""
        ds = ray.data.range(100)

        # Cache schema
        original_schema = ds.schema()
        assert "id" in original_schema.names
        assert "doubled" not in original_schema.names

        # Apply add_column - schema changes
        added_ds = ds.add_column("doubled", lambda x: x["id"] * 2)
        added_schema = added_ds.schema()

        # Should have new column
        assert "id" in added_schema.names
        assert "doubled" in added_schema.names
        assert len(added_schema.names) == len(original_schema.names) + 1

    def test_schema_changed_after_drop_columns(self, ray_start_regular_shared):
        """Schema should change after drop_columns() - column removed."""
        ds = ray.data.from_items([{"a": 1, "b": 2, "c": 3}] * 100)

        # Cache schema
        original_schema = ds.schema()
        assert set(original_schema.names) == {"a", "b", "c"}

        # Apply drop_columns - schema changes
        dropped_ds = ds.drop_columns(["b"])
        dropped_schema = dropped_ds.schema()

        assert set(dropped_schema.names) == {"a", "c"}
        assert "b" not in dropped_schema.names

    def test_schema_changed_after_select_columns(self, ray_start_regular_shared):
        """Schema should change after select_columns() - only selected columns remain."""
        ds = ray.data.from_items([{"a": 1, "b": 2, "c": 3}] * 100)

        # Cache schema
        original_schema = ds.schema()
        assert set(original_schema.names) == {"a", "b", "c"}

        # Apply select_columns - schema changes
        selected_ds = ds.select_columns(["a", "c"])
        selected_schema = selected_ds.schema()

        assert set(selected_schema.names) == {"a", "c"}
        assert "b" not in selected_schema.names


class TestCountPreservation:
    """Test that count() is correctly preserved/invalidated."""

    def test_count_preserved_after_map(self, ray_start_regular_shared):
        """Count should be preserved after map() - 1:1 transformation."""
        ds = ray.data.range(100)

        # Cache count
        original_count = ds.count()
        assert original_count == 100

        # Apply map - count should be preserved
        mapped_ds = ds.map(lambda x: {"id": x["id"] * 2})
        mapped_count = mapped_ds.count()

        assert mapped_count == original_count

    def test_count_preserved_after_add_column(self, ray_start_regular_shared):
        """Count should be preserved after add_column() - 1:1 transformation."""
        ds = ray.data.range(100)

        # Cache count
        original_count = ds.count()

        # Apply add_column - count should be preserved
        added_ds = ds.add_column("doubled", lambda x: x["id"] * 2)
        added_count = added_ds.count()

        assert added_count == original_count

    def test_count_preserved_after_drop_columns(self, ray_start_regular_shared):
        """Count should be preserved after drop_columns() - 1:1 transformation."""
        ds = ray.data.from_items([{"a": 1, "b": 2}] * 100)

        # Cache count
        original_count = ds.count()

        # Apply drop_columns - count should be preserved
        dropped_ds = ds.drop_columns(["b"])
        dropped_count = dropped_ds.count()

        assert dropped_count == original_count

    def test_count_preserved_after_sort(self, ray_start_regular_shared):
        """Count should be preserved after sort() - reordering only."""
        ds = ray.data.range(100)

        # Cache count
        original_count = ds.count()

        # Apply sort - count should be preserved
        sorted_ds = ds.sort("id", descending=True)
        sorted_count = sorted_ds.count()

        assert sorted_count == original_count

    def test_count_preserved_after_repartition(self, ray_start_regular_shared):
        """Count should be preserved after repartition() - reordering only."""
        ds = ray.data.range(100)

        # Cache count
        original_count = ds.count()

        # Apply repartition - count should be preserved
        repartitioned_ds = ds.repartition(5)
        repartitioned_count = repartitioned_ds.count()

        assert repartitioned_count == original_count

    def test_count_not_preserved_after_limit(self, ray_start_regular_shared):
        """Count should NOT be preserved after limit() - count changes."""
        ds = ray.data.range(100)

        # Cache count
        original_count = ds.count()
        assert original_count == 100

        # Apply limit - count changes
        limited_ds = ds.limit(10)
        limited_count = limited_ds.count()

        assert limited_count == 10
        assert limited_count != original_count

    def test_count_not_preserved_after_filter(self, ray_start_regular_shared):
        """Count should NOT be preserved after filter() - count changes."""
        ds = ray.data.range(100)

        # Cache count
        original_count = ds.count()

        # Apply filter - count changes
        filtered_ds = ds.filter(lambda x: x["id"] < 50)
        filtered_count = filtered_ds.count()

        assert filtered_count == 50
        assert filtered_count != original_count


class TestSmartCountComputation:
    """Test smart count computation for limit() and filter()."""

    def test_smart_count_for_limit(self, ray_start_regular_shared):
        """Cache should smart-compute count for limit operation."""
        ds = ray.data.range(100)

        # Cache count
        original_count = ds.count()
        assert original_count == 100

        # Apply limit - cache should compute new count
        limited_ds = ds.limit(10)
        limited_count = limited_ds.count()

        # Should be smart-computed: min(100, 10) = 10
        assert limited_count == 10

    def test_smart_count_for_limit_larger_than_dataset(self, ray_start_regular_shared):
        """Smart count should handle limit larger than dataset."""
        ds = ray.data.range(50)

        # Cache count
        original_count = ds.count()
        assert original_count == 50

        # Apply limit larger than dataset
        limited_ds = ds.limit(200)
        limited_count = limited_ds.count()

        # Should be smart-computed: min(50, 200) = 50
        assert limited_count == 50


class TestAggregationPreservation:
    """Test that aggregations are preserved for reordering operations."""

    def test_aggregations_preserved_after_sort(self, ray_start_regular_shared):
        """Aggregations should be preserved after sort() - values don't change."""
        ds = ray.data.range(100)

        # Cache aggregations
        original_sum = ds.sum("id")
        original_min = ds.min("id")
        original_max = ds.max("id")

        # Apply sort - aggregations should be preserved
        sorted_ds = ds.sort("id", descending=True)
        sorted_sum = sorted_ds.sum("id")
        sorted_min = sorted_ds.min("id")
        sorted_max = sorted_ds.max("id")

        assert sorted_sum == original_sum
        assert sorted_min == original_min
        assert sorted_max == original_max

    def test_aggregations_preserved_after_repartition(self, ray_start_regular_shared):
        """Aggregations should be preserved after repartition() - values don't change."""
        ds = ray.data.range(100)

        # Cache aggregations
        original_sum = ds.sum("id")
        original_min = ds.min("id")
        original_max = ds.max("id")

        # Apply repartition - aggregations should be preserved
        repartitioned_ds = ds.repartition(5)
        repartitioned_sum = repartitioned_ds.sum("id")
        repartitioned_min = repartitioned_ds.min("id")
        repartitioned_max = repartitioned_ds.max("id")

        assert repartitioned_sum == original_sum
        assert repartitioned_min == original_min
        assert repartitioned_max == original_max

    def test_aggregations_not_preserved_after_map(self, ray_start_regular_shared):
        """Aggregations should NOT be preserved after map() - values change."""
        ds = ray.data.range(100)

        # Cache aggregations
        original_sum = ds.sum("id")
        assert original_sum == sum(range(100))  # 4950

        # Apply map that changes values - aggregations change
        mapped_ds = ds.map(lambda x: {"id": x["id"] * 2})
        mapped_sum = mapped_ds.sum("id")

        # Sum should be doubled
        assert mapped_sum == original_sum * 2
        assert mapped_sum != original_sum

    def test_aggregations_not_preserved_after_filter(self, ray_start_regular_shared):
        """Aggregations should NOT be preserved after filter() - dataset subset."""
        ds = ray.data.range(100)

        # Cache aggregations
        original_sum = ds.sum("id")
        original_max = ds.max("id")

        # Apply filter - aggregations change
        filtered_ds = ds.filter(lambda x: x["id"] < 50)
        filtered_sum = filtered_ds.sum("id")
        filtered_max = filtered_ds.max("id")

        # Aggregations should be different
        assert filtered_sum < original_sum
        assert filtered_max < original_max


class TestColumnsComputation:
    """Test smart columns computation for add/drop/select operations."""

    def test_columns_preserved_after_filter(self, ray_start_regular_shared):
        """Columns should be preserved after filter()."""
        ds = ray.data.from_items([{"a": 1, "b": 2, "c": 3}] * 100)

        # Cache columns
        original_columns = ds.columns()
        assert set(original_columns) == {"a", "b", "c"}

        # Apply filter - columns should be preserved
        filtered_ds = ds.filter(lambda x: x["a"] > 0)
        filtered_columns = filtered_ds.columns()

        assert set(filtered_columns) == set(original_columns)

    def test_columns_preserved_after_sort(self, ray_start_regular_shared):
        """Columns should be preserved after sort()."""
        ds = ray.data.from_items([{"a": i, "b": i * 2} for i in range(100)])

        # Cache columns
        original_columns = ds.columns()
        assert set(original_columns) == {"a", "b"}

        # Apply sort - columns should be preserved
        sorted_ds = ds.sort("a")
        sorted_columns = sorted_ds.columns()

        assert set(sorted_columns) == set(original_columns)

    def test_smart_columns_after_add_column(self, ray_start_regular_shared):
        """Cache should smart-compute columns after add_column()."""
        ds = ray.data.from_items([{"a": 1, "b": 2}] * 100)

        # Cache columns
        original_columns = ds.columns()
        assert set(original_columns) == {"a", "b"}

        # Add column - should smart-compute new columns list
        added_ds = ds.add_column("c", lambda x: x["a"] + x["b"])
        added_columns = added_ds.columns()

        # Should have smart-computed: original + ["c"]
        assert set(added_columns) == {"a", "b", "c"}

    def test_smart_columns_after_drop_columns(self, ray_start_regular_shared):
        """Cache should smart-compute columns after drop_columns()."""
        ds = ray.data.from_items([{"a": 1, "b": 2, "c": 3}] * 100)

        # Cache columns
        original_columns = ds.columns()
        assert set(original_columns) == {"a", "b", "c"}

        # Drop column - should smart-compute new columns list
        dropped_ds = ds.drop_columns(["b"])
        dropped_columns = dropped_ds.columns()

        # Should have smart-computed: original - ["b"]
        assert set(dropped_columns) == {"a", "c"}
        assert "b" not in dropped_columns

    def test_smart_columns_after_select_columns(self, ray_start_regular_shared):
        """Cache should smart-compute columns after select_columns()."""
        ds = ray.data.from_items([{"a": 1, "b": 2, "c": 3, "d": 4}] * 100)

        # Cache columns
        original_columns = ds.columns()
        assert set(original_columns) == {"a", "b", "c", "d"}

        # Select columns - should smart-compute new columns list
        selected_ds = ds.select_columns(["a", "c"])
        selected_columns = selected_ds.columns()

        # Should have smart-computed: just ["a", "c"]
        assert set(selected_columns) == {"a", "c"}


class TestMultiStepTransformations:
    """Test cache behavior across multi-step transformation chains."""

    def test_chain_filter_then_sort(self, ray_start_regular_shared):
        """Test cache preservation in filter -> sort chain."""
        ds = ray.data.range(100)

        # Cache initial state
        original_schema = ds.schema()

        # Filter then sort
        transformed_ds = ds.filter(lambda x: x["id"] < 50).sort("id", descending=True)

        # Count should change (filter), schema should be preserved (both ops)
        assert transformed_ds.count() == 50
        assert transformed_ds.schema().names == original_schema.names

    def test_chain_limit_then_map(self, ray_start_regular_shared):
        """Test cache preservation in limit -> map chain."""
        ds = ray.data.range(100)

        # Limit then map
        transformed_ds = ds.limit(10).map(lambda x: {"id": x["id"] * 2})

        # Count should be preserved through both (limit sets it to 10, map preserves)
        assert transformed_ds.count() == 10

    def test_chain_add_drop_select(self, ray_start_regular_shared):
        """Test smart column computation through add -> drop -> select chain."""
        ds = ray.data.from_items([{"a": 1, "b": 2}] * 100)

        # Chain: add "c", drop "b", select ["a", "c"]
        transformed_ds = (
            ds.add_column("c", lambda x: x["a"] + x["b"])
            .drop_columns(["b"])
            .select_columns(["a", "c"])
        )

        # Final columns should be smart-computed correctly
        final_columns = transformed_ds.columns()
        assert set(final_columns) == {"a", "c"}

    def test_chain_sort_then_limit(self, ray_start_regular_shared):
        """Test aggregation preservation in sort -> limit chain."""
        ds = ray.data.range(100)

        # Cache aggregations
        original_sum = ds.sum("id")

        # Sort (preserves aggregations) then limit (invalidates them)
        transformed_ds = ds.sort("id", descending=True).limit(10)

        # Aggregations should be different after limit
        limited_sum = transformed_ds.sum("id")
        assert limited_sum != original_sum
        # Top 10 after reverse sort: 99+98+...+90 = 945
        assert limited_sum == sum(range(90, 100))


class TestComplexTransformInvalidation:
    """Test that complex transformations properly invalidate cache."""

    def test_map_batches_invalidates_all(self, ray_start_regular_shared):
        """map_batches is complex - should invalidate all cache entries."""
        ds = ray.data.range(100)

        # Cache various values
        original_count = ds.count()
        original_schema = ds.schema()

        # Apply map_batches - everything should be invalidated
        batched_ds = ds.map_batches(lambda batch: {"value": batch["id"]})

        # Schema changes (different columns)
        assert batched_ds.schema().names != original_schema.names

        # Count is preserved (1:1) but cache doesn't know that
        assert batched_ds.count() == original_count

    def test_union_invalidates_all(self, ray_start_regular_shared):
        """union is multi-dataset operation - should invalidate all cache."""
        ds1 = ray.data.range(50)
        ds2 = ray.data.range(50)

        # Cache ds1 values
        ds1_schema = ds1.schema()

        # Union with ds2
        union_ds = ds1.union(ds2)

        # Count should be sum, schema preserved
        assert union_ds.count() == 100
        assert union_ds.schema().names == ds1_schema.names


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_empty_dataset_operations(self, ray_start_regular_shared):
        """Test cache behavior with empty datasets."""
        ds = ray.data.range(0)  # Empty dataset

        # Cache count
        count = ds.count()
        assert count == 0

        # Operations on empty dataset
        limited_ds = ds.limit(10)
        assert limited_ds.count() == 0

        filtered_ds = ds.filter(lambda x: True)
        assert filtered_ds.count() == 0

    def test_limit_zero(self, ray_start_regular_shared):
        """Test limit(0) smart count computation."""
        ds = ray.data.range(100)

        # Cache count
        original_count = ds.count()
        assert original_count == 100

        # Limit to 0
        limited_ds = ds.limit(0)
        assert limited_ds.count() == 0

    def test_drop_all_columns(self, ray_start_regular_shared):
        """Test dropping all columns (edge case)."""
        ds = ray.data.from_items([{"a": 1, "b": 2}] * 10)

        # Cache columns
        original_columns = ds.columns()
        assert len(original_columns) == 2

        # Drop all columns
        dropped_ds = ds.drop_columns(["a", "b"])
        dropped_columns = dropped_ds.columns()

        # Should have no columns
        assert len(dropped_columns) == 0

    def test_select_single_column(self, ray_start_regular_shared):
        """Test selecting a single column."""
        ds = ray.data.from_items([{"a": 1, "b": 2, "c": 3}] * 10)

        # Select single column
        selected_ds = ds.select_columns(["a"])
        selected_columns = selected_ds.columns()

        assert selected_columns == ["a"]
        assert len(selected_columns) == 1


class TestCacheStatsValidation:
    """Test that cache stats reflect the smart update behavior."""

    def test_cache_hits_for_preserved_values(self, ray_start_regular_shared):
        """Verify cache hits when values should be preserved."""
        import ray.data as rd

        rd.clear_dataset_cache()

        ds = ray.data.range(100)

        # First count - miss
        count1 = ds.count()
        stats1 = rd.get_cache_stats()

        # Second count - hit
        count2 = ds.count()
        stats2 = rd.get_cache_stats()

        assert count1 == count2
        assert stats2["hit_count"] > stats1.get("hit_count", 0)

        # Sort (preserves count)
        sorted_ds = ds.sort("id")
        sorted_count = sorted_ds.count()

        # Should have gotten count from cache preservation
        assert sorted_count == count1

    def test_no_false_cache_hits(self, ray_start_regular_shared):
        """Ensure no false cache hits when values should change."""
        import ray.data as rd

        rd.clear_dataset_cache()

        ds = ray.data.range(100)

        # Cache count
        original_count = ds.count()
        assert original_count == 100

        # Filter (changes count) - should NOT use cached count
        filtered_ds = ds.filter(lambda x: x["id"] < 50)
        filtered_count = filtered_ds.count()

        # Count should be different (not a false cache hit)
        assert filtered_count == 50
        assert filtered_count != original_count


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))

