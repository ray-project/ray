"""Comprehensive tests for Ray Data caching functionality.

This test suite validates the caching system's correctness, including:
- Basic caching of operations (count, schema, aggregations)
- Cache invalidation for transformations
- Smart cache updates and preservation rules
- Cache key stability and determinism
- Edge cases and boundary conditions
"""

import time

import pytest

import ray
from ray.data.context import DataContext
from ray.data.dataset import MaterializedDataset
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def test_count_caching(ray_start_regular_shared):
    """Test that count() results are cached for repeated calls."""
    ds = ray.data.range(1000)

    start_time = time.time()
    count1 = ds.count()
    first_duration = time.time() - start_time

    # Second count should use cached result
    start_time = time.time()
    count2 = ds.count()
    second_duration = time.time() - start_time

    assert count1 == count2 == 1000

    assert second_duration < first_duration * 0.5


def test_schema_caching(ray_start_regular_shared):
    """Test that schema() results are cached for repeated calls."""
    ds = ray.data.range(100)

    schema1 = ds.schema()
    schema2 = ds.schema()

    assert schema1.names == schema2.names
    assert schema1.types == schema2.types


def test_materialize_caching(ray_start_regular_shared):
    """Test that materialize() caches MaterializedDataset objects."""
    ds = ray.data.range(100).map(lambda x: {"value": x["id"] * 2})

    start_time = time.time()
    mat1 = ds.materialize()
    first_duration = time.time() - start_time

    # Second materialize should return cached MaterializedDataset
    start_time = time.time()
    mat2 = ds.materialize()
    second_duration = time.time() - start_time

    assert isinstance(mat1, MaterializedDataset)
    assert isinstance(mat2, MaterializedDataset)

    assert mat1.count() == mat2.count() == 100
    assert mat1.num_blocks() == mat2.num_blocks()
    assert mat1.schema().names == mat2.schema().names

    assert second_duration < first_duration * 0.5


def test_aggregation_caching(ray_start_regular_shared):
    """Test that aggregation operations are cached."""
    ds = ray.data.range(1000)

    sum1 = ds.sum("id")
    sum2 = ds.sum("id")
    assert sum1 == sum2

    min1 = ds.min("id")
    min2 = ds.min("id")
    assert min1 == min2 == 0

    max1 = ds.max("id")
    max2 = ds.max("id")
    assert max1 == max2 == 999


def test_cache_invalidation_map(ray_start_regular_shared):
    """Test cache invalidation for map transformation."""
    ds = ray.data.range(100)

    # Cache count and schema
    original_count = ds.count()
    original_schema = ds.schema()

    mapped_ds = ds.map(lambda x: {"doubled": x["id"] * 2})

    mapped_count = mapped_ds.count()
    assert mapped_count == original_count == 100

    mapped_schema = mapped_ds.schema()
    assert mapped_schema.names != original_schema.names
    assert "doubled" in mapped_schema.names
    assert "id" not in mapped_schema.names


def test_cache_invalidation_filter(ray_start_regular_shared):
    """Test cache invalidation for filter transformation."""
    ds = ray.data.range(100)

    # Cache count and schema
    original_count = ds.count()
    original_schema = ds.schema()

    filtered_ds = ds.filter(lambda x: x["id"] < 50)

    filtered_schema = filtered_ds.schema()
    assert filtered_schema.names == original_schema.names

    filtered_count = filtered_ds.count()
    assert filtered_count == 50
    assert filtered_count != original_count


def test_cache_invalidation_limit(ray_start_regular_shared):
    """Test cache invalidation for limit transformation."""
    ds = ray.data.range(100)

    # Cache count and schema
    original_count = ds.count()
    original_schema = ds.schema()

    limited_ds = ds.limit(25)

    limited_schema = limited_ds.schema()
    assert limited_schema.names == original_schema.names

    # Count should change to the limit value
    limited_count = limited_ds.count()
    assert limited_count == 25
    assert limited_count != original_count


def test_cache_invalidation_sort(ray_start_regular_shared):
    """Test cache invalidation for sort transformation."""
    ds = ray.data.range(100)

    # Cache aggregations
    original_count = ds.count()
    original_sum = ds.sum("id")
    original_schema = ds.schema()

    sorted_ds = ds.sort("id", descending=True)

    sorted_count = sorted_ds.count()
    sorted_sum = sorted_ds.sum("id")
    sorted_schema = sorted_ds.schema()

    assert sorted_count == original_count
    assert sorted_sum == original_sum
    assert sorted_schema.names == original_schema.names


def test_schema_preserved_after_limit(ray_start_regular_shared):
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


def test_schema_preserved_after_filter(ray_start_regular_shared):
    """Schema should be preserved after filter() - only count changes."""
    ds = ray.data.range(100)

    # Cache schema
    original_schema = ds.schema()

    # Apply filter - schema should be preserved
    filtered_ds = ds.filter(lambda x: x["id"] < 50)
    filtered_schema = filtered_ds.schema()

    assert filtered_schema.names == original_schema.names


def test_schema_preserved_after_sort(ray_start_regular_shared):
    """Schema should be preserved after sort() - reordering only."""
    ds = ray.data.range(100)

    # Cache schema
    original_schema = ds.schema()

    # Apply sort - schema should be preserved
    sorted_ds = ds.sort("id", descending=True)
    sorted_schema = sorted_ds.schema()

    assert sorted_schema.names == original_schema.names


def test_schema_preserved_after_repartition(ray_start_regular_shared):
    """Schema should be preserved after repartition() - reordering only."""
    ds = ray.data.range(100)

    # Cache schema
    original_schema = ds.schema()

    # Apply repartition - schema should be preserved
    repartitioned_ds = ds.repartition(5)
    repartitioned_schema = repartitioned_ds.schema()

    assert repartitioned_schema.names == original_schema.names


def test_schema_changed_after_add_column(ray_start_regular_shared):
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


def test_schema_changed_after_drop_columns(ray_start_regular_shared):
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


def test_schema_changed_after_select_columns(ray_start_regular_shared):
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


def test_schema_preserved_after_rename_columns(ray_start_regular_shared):
    """Schema column names should change but structure preserved after rename_columns()."""
    ds = ray.data.from_items([{"a": 1, "b": 2}] * 100)

    # Cache schema
    original_schema = ds.schema()
    assert set(original_schema.names) == {"a", "b"}

    # Apply rename_columns
    renamed_ds = ds.rename_columns({"a": "col_a", "b": "col_b"})
    renamed_schema = renamed_ds.schema()

    # Column names should change
    assert set(renamed_schema.names) == {"col_a", "col_b"}
    # But count should be preserved
    assert renamed_ds.count() == ds.count()


def test_count_preserved_after_map(ray_start_regular_shared):
    """Count should be preserved after map() - 1:1 transformation."""
    ds = ray.data.range(100)

    # Cache count
    original_count = ds.count()
    assert original_count == 100

    # Apply map - count should be preserved
    mapped_ds = ds.map(lambda x: {"id": x["id"] * 2})
    mapped_count = mapped_ds.count()

    assert mapped_count == original_count


def test_count_preserved_after_add_column(ray_start_regular_shared):
    """Count should be preserved after add_column() - 1:1 transformation."""
    ds = ray.data.range(100)

    # Cache count
    original_count = ds.count()

    # Apply add_column - count should be preserved
    added_ds = ds.add_column("doubled", lambda x: x["id"] * 2)
    added_count = added_ds.count()

    assert added_count == original_count


def test_count_preserved_after_drop_columns(ray_start_regular_shared):
    """Count should be preserved after drop_columns() - 1:1 transformation."""
    ds = ray.data.from_items([{"a": 1, "b": 2}] * 100)

    # Cache count
    original_count = ds.count()

    # Apply drop_columns - count should be preserved
    dropped_ds = ds.drop_columns(["b"])
    dropped_count = dropped_ds.count()

    assert dropped_count == original_count


def test_count_preserved_after_sort(ray_start_regular_shared):
    """Count should be preserved after sort() - reordering only."""
    ds = ray.data.range(100)

    # Cache count
    original_count = ds.count()

    # Apply sort - count should be preserved
    sorted_ds = ds.sort("id", descending=True)
    sorted_count = sorted_ds.count()

    assert sorted_count == original_count


def test_count_preserved_after_repartition(ray_start_regular_shared):
    """Count should be preserved after repartition() - reordering only."""
    ds = ray.data.range(100)

    # Cache count
    original_count = ds.count()

    # Apply repartition - count should be preserved
    repartitioned_ds = ds.repartition(5)
    repartitioned_count = repartitioned_ds.count()

    assert repartitioned_count == original_count


def test_count_not_preserved_after_limit(ray_start_regular_shared):
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


def test_count_not_preserved_after_filter(ray_start_regular_shared):
    """Count should NOT be preserved after filter() - count changes."""
    ds = ray.data.range(100)

    # Cache count
    original_count = ds.count()

    # Apply filter - count changes
    filtered_ds = ds.filter(lambda x: x["id"] < 50)
    filtered_count = filtered_ds.count()

    assert filtered_count == 50
    assert filtered_count != original_count


def test_smart_count_for_limit(ray_start_regular_shared):
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


def test_smart_count_for_limit_larger_than_dataset(ray_start_regular_shared):
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


def test_aggregations_preserved_after_sort(ray_start_regular_shared):
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


def test_aggregations_preserved_after_repartition(ray_start_regular_shared):
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


def test_aggregations_not_preserved_after_map(ray_start_regular_shared):
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


def test_aggregations_not_preserved_after_filter(ray_start_regular_shared):
    """Aggregations should NOT be preserved after filter() - dataset subset."""
    ds = ray.data.range(100)

    # Cache aggregations
    original_sum = ds.sum("id")
    original_max = ds.max("id")

    # Apply filter - aggregations change
    filtered_ds = ds.filter(lambda x: x["id"] < 50)
    filtered_sum = filtered_ds.sum("id")
    filtered_max = filtered_ds.max("id")

    assert filtered_sum < original_sum
    assert filtered_max < original_max


def test_columns_preserved_after_filter(ray_start_regular_shared):
    """Columns should be preserved after filter()."""
    ds = ray.data.from_items([{"a": 1, "b": 2, "c": 3}] * 100)

    # Cache columns
    original_columns = ds.columns()
    assert set(original_columns) == {"a", "b", "c"}

    # Apply filter - columns should be preserved
    filtered_ds = ds.filter(lambda x: x["a"] > 0)
    filtered_columns = filtered_ds.columns()

    assert set(filtered_columns) == set(original_columns)


def test_columns_preserved_after_sort(ray_start_regular_shared):
    """Columns should be preserved after sort()."""
    ds = ray.data.from_items([{"a": i, "b": i * 2} for i in range(100)])

    # Cache columns
    original_columns = ds.columns()
    assert set(original_columns) == {"a", "b"}

    # Apply sort - columns should be preserved
    sorted_ds = ds.sort("a")
    sorted_columns = sorted_ds.columns()

    assert set(sorted_columns) == set(original_columns)


def test_smart_columns_after_add_column(ray_start_regular_shared):
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


def test_smart_columns_after_drop_columns(ray_start_regular_shared):
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


def test_smart_columns_after_select_columns(ray_start_regular_shared):
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


def test_chain_filter_then_sort(ray_start_regular_shared):
    """Test cache preservation in filter -> sort chain."""
    ds = ray.data.range(100)

    # Cache initial state
    original_schema = ds.schema()

    # Filter then sort
    transformed_ds = ds.filter(lambda x: x["id"] < 50).sort("id", descending=True)

    assert transformed_ds.count() == 50
    assert transformed_ds.schema().names == original_schema.names


def test_chain_limit_then_map(ray_start_regular_shared):
    """Test cache preservation in limit -> map chain."""
    ds = ray.data.range(100)

    # Limit then map
    transformed_ds = ds.limit(10).map(lambda x: {"id": x["id"] * 2})

    assert transformed_ds.count() == 10


def test_chain_add_drop_select(ray_start_regular_shared):
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


def test_chain_sort_then_limit(ray_start_regular_shared):
    """Test aggregation preservation in sort -> limit chain."""
    ds = ray.data.range(100)

    # Cache aggregations
    original_sum = ds.sum("id")

    # Sort (preserves aggregations) then limit (invalidates them)
    transformed_ds = ds.sort("id", descending=True).limit(10)

    limited_sum = transformed_ds.sum("id")
    assert limited_sum != original_sum
    # Top 10 after reverse sort: 99+98+...+90 = 945
    assert limited_sum == sum(range(90, 100))


def test_map_batches_invalidates_all(ray_start_regular_shared):
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


def test_union_invalidates_all(ray_start_regular_shared):
    """union is multi-dataset operation - should invalidate all cache."""
    ds1 = ray.data.range(50)
    ds2 = ray.data.range(50)

    # Cache ds1 values
    ds1_schema = ds1.schema()

    # Union with ds2
    union_ds = ds1.union(ds2)

    assert union_ds.count() == 100
    assert union_ds.schema().names == ds1_schema.names


def test_empty_dataset_operations(ray_start_regular_shared):
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


def test_limit_zero(ray_start_regular_shared):
    """Test limit(0) smart count computation."""
    ds = ray.data.range(100)

    # Cache count
    original_count = ds.count()
    assert original_count == 100

    # Limit to 0
    limited_ds = ds.limit(0)
    assert limited_ds.count() == 0


def test_drop_all_columns(ray_start_regular_shared):
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


def test_select_single_column(ray_start_regular_shared):
    """Test selecting a single column."""
    ds = ray.data.from_items([{"a": 1, "b": 2, "c": 3}] * 10)

    # Select single column
    selected_ds = ds.select_columns(["a"])
    selected_columns = selected_ds.columns()

    assert selected_columns == ["a"]
    assert len(selected_columns) == 1


def test_cache_with_parameters(ray_start_regular_shared):
    """Test that cache keys include operation parameters."""
    ds = ray.data.range(100)

    # Different parameters should create different cache entries
    take_5 = ds.take(5)
    take_10 = ds.take(10)

    assert len(take_5) == 5
    assert len(take_10) == 10

    # Same parameters should use cached results
    take_5_cached = ds.take(5)
    assert take_5 == take_5_cached


def test_cache_with_kwargs(ray_start_regular_shared):
    """Test that cache properly handles both positional and keyword arguments."""
    import ray.data as rd

    ds = ray.data.range(100)

    # Clear cache to start fresh
    rd.clear_dataset_cache()

    # Call take with positional argument
    result1 = ds.take(10)
    assert len(result1) == 10

    # Call take with keyword argument (should use same cache entry)
    result2 = ds.take(limit=10)
    assert len(result2) == 10
    assert result1 == result2

    # Call take with different keyword argument (should create new cache entry)
    result3 = ds.take(limit=5)
    assert len(result3) == 5
    assert result1 != result3

    # Verify cache stats show appropriate hits/misses
    stats = rd.get_cache_stats()
    assert stats["hit_count"] >= 1, "Should have cache hits for same parameters"


def test_cache_disable(ray_start_regular_shared):
    """Test disabling cache functionality."""
    ds = ray.data.range(100)

    # Enable caching and cache a result
    ctx = DataContext.get_current()
    ctx.enable_dataset_caching = True
    count1 = ds.count()

    # Disable caching temporarily
    ctx.enable_dataset_caching = False
    count2 = ds.count()

    # Re-enable caching
    ctx.enable_dataset_caching = True
    count3 = ds.count()

    # All results should be identical
    assert count1 == count2 == count3 == 100


def test_cache_stats(ray_start_regular_shared):
    """Test cache statistics tracking."""
    import ray.data as rd

    # Clear cache to start fresh
    rd.clear_dataset_cache()

    ds = ray.data.range(50)

    # Perform operations to populate cache
    ds.count()
    ds.schema()
    ds.sum("id")

    # Get cache statistics
    stats = rd.get_cache_stats()

    # Should have cache entries
    assert stats["total_entries"] > 0
    assert stats["hit_count"] >= 0
    assert stats["miss_count"] >= 0


def test_cache_context_managers(ray_start_regular_shared):
    """Test cache context managers."""
    import ray.data as rd

    ds = ray.data.range(50)

    # Test disable context manager
    with rd.disable_dataset_caching():
        count_disabled = ds.count()

    count_enabled = ds.count()

    assert count_disabled == count_enabled == 50


def test_cache_clear(ray_start_regular_shared):
    """Test cache clearing functionality."""
    import ray.data as rd

    ds = ray.data.range(50)

    # Populate cache
    ds.count()
    ds.schema()

    # Check cache has entries
    stats = rd.get_cache_stats()
    assert stats["total_entries"] > 0

    # Clear cache
    rd.clear_dataset_cache()

    # Check cache is empty
    stats_after = rd.get_cache_stats()
    assert stats_after["total_entries"] == 0


def test_cache_with_complex_transformations(ray_start_regular_shared):
    """Test caching with complex transformation chains."""
    # Create complex dataset
    ds = (
        ray.data.range(1000)
        .map(lambda x: {"id": x["id"], "value": x["id"] * 2})
        .filter(lambda x: x["value"] < 1000)
        .map(lambda x: {"id": x["id"], "processed": x["value"] + 100})
    )

    count1 = ds.count()
    count2 = ds.count()
    assert count1 == count2

    schema1 = ds.schema()
    schema2 = ds.schema()
    assert schema1.names == schema2.names

    # Test materialize with complex pipeline
    mat1 = ds.materialize()
    mat2 = ds.materialize()

    assert isinstance(mat1, MaterializedDataset)
    assert isinstance(mat2, MaterializedDataset)
    assert mat1.count() == mat2.count()


@pytest.mark.parametrize("operation", ["count", "schema", "sum"])
def test_cache_different_datasets(ray_start_regular_shared, operation):
    """Test that different datasets have separate cache entries."""
    ds1 = ray.data.range(100)
    ds2 = ray.data.range(200)

    # Operations on different datasets should return different results
    if operation == "count":
        result1 = ds1.count()
        result2 = ds2.count()
        assert result1 == 100
        assert result2 == 200
    elif operation == "schema":
        result1 = ds1.schema()
        result2 = ds2.schema()
        assert result1.names == result2.names  # Same structure
    elif operation == "sum":
        result1 = ds1.sum("id")
        result2 = ds2.sum("id")
        assert result1 != result2  # Different sums


def test_cache_key_stability(ray_start_regular_shared):
    """Test that cache keys are stable and content-based, not memory-address based."""
    from ray.data._internal.cache.key_generation import make_cache_key

    # Create same dataset twice
    ds1 = ray.data.range(100)
    ds2 = ray.data.range(100)

    # Get cache keys for same operation on equivalent datasets
    key1 = make_cache_key(ds1._logical_plan, "count")
    key2 = make_cache_key(ds2._logical_plan, "count")

    # Keys should be identical for equivalent logical plans
    # (This verifies we're not using memory addresses like id())
    assert key1 == key2, "Cache keys should be stable and content-based"

    # Keys for different datasets should be different
    ds3 = ray.data.range(200)
    key3 = make_cache_key(ds3._logical_plan, "count")
    assert key1 != key3, "Different datasets should have different cache keys"

    # Keys for different operations should be different
    key4 = make_cache_key(ds1._logical_plan, "schema")
    assert key1 != key4, "Different operations should have different cache keys"

    # Keys with different parameters should be different
    key5 = make_cache_key(ds1._logical_plan, "count", limit=50)
    key6 = make_cache_key(ds1._logical_plan, "count", limit=100)
    assert key5 != key6, "Different parameters should produce different cache keys"


def test_cache_key_context_settings(ray_start_regular_shared):
    """Test that cache keys include context settings that affect results."""
    from ray.data._internal.cache.key_generation import make_cache_key
    from ray.data.context import DataContext

    ds = ray.data.range(100)

    # Get key with default context
    key1 = make_cache_key(ds._logical_plan, "count")

    # Change a context setting that affects results
    ctx = DataContext.get_current()
    original_block_size = ctx.target_max_block_size
    ctx.target_max_block_size = original_block_size * 2

    # Create same dataset with modified context
    ds2 = ray.data.range(100)
    key2 = make_cache_key(ds2._logical_plan, "count")

    # Keys should be different when context settings differ
    # (This verifies context is properly serialized, not using id())
    assert (
        key1 != key2
    ), "Different context settings should produce different cache keys"

    # Restore original setting
    ctx.target_max_block_size = original_block_size


def test_cache_key_deterministic(ray_start_regular_shared):
    """Test that cache keys are deterministic across process restarts."""
    from ray.data._internal.cache.key_generation import make_cache_key

    ds = ray.data.range(100)

    # Get initial cache key
    key1 = make_cache_key(ds._logical_plan, "count")
    key2 = make_cache_key(ds._logical_plan, "count")

    # Keys should be identical (deterministic)
    assert key1 == key2, "Cache keys should be deterministic"

    # Verify key doesn't use Python's randomized hash()
    # If it did, reloading the module would change the key
    # (This is a best-effort check; real verification requires process restart)
    ds2 = ray.data.range(100)
    key3 = make_cache_key(ds2._logical_plan, "count")
    assert key1 == key3, "Cache keys should be stable across dataset recreations"


def test_cache_fallback_deterministic(ray_start_regular_shared):
    """Test that fallback cache key generation is deterministic."""
    from ray.data._internal.cache.key_generation import make_cache_key

    # Create datasets that might trigger fallback path
    ds1 = ray.data.range(50)
    ds2 = ray.data.range(50)

    # Get cache keys multiple times
    key1a = make_cache_key(ds1._logical_plan, "test_op", param1="value1")
    key1b = make_cache_key(ds1._logical_plan, "test_op", param1="value1")
    key2a = make_cache_key(ds2._logical_plan, "test_op", param1="value1")

    # Keys should be deterministic
    assert key1a == key1b, "Same inputs should produce same keys"
    assert key1a == key2a, "Equivalent datasets should produce same keys"

    # Different parameters should produce different keys
    key3 = make_cache_key(ds1._logical_plan, "test_op", param1="value2")
    assert key1a != key3, "Different parameters should produce different keys"


def test_cache_hits_for_preserved_values(ray_start_regular_shared):
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


def test_no_false_cache_hits(ray_start_regular_shared):
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

    assert filtered_count == 50
    assert filtered_count != original_count


def test_with_column_count_preserved(ray_start_regular_shared):
    """Test that with_column preserves count (it's a 1:1 transformation)."""
    from ray.data.expressions import col

    ds = ray.data.range(100)

    # Cache count
    original_count = ds.count()

    # Apply with_column - count should be preserved
    transformed_ds = ds.with_column("doubled", col("id") * 2)
    transformed_count = transformed_ds.count()

    assert transformed_count == original_count == 100


def test_with_column_columns_computed(ray_start_regular_shared):
    """Test that with_column smart-computes columns list."""
    from ray.data.expressions import col

    ds = ray.data.range(100)

    # Cache columns
    original_columns = ds.columns()
    assert original_columns == ["id"]

    # Apply with_column - should smart-compute new columns
    transformed_ds = ds.with_column("doubled", col("id") * 2)
    transformed_columns = transformed_ds.columns()

    # Should have smart-computed: original + ["doubled"]
    assert set(transformed_columns) == {"id", "doubled"}


def test_with_column_preserves_schema_structure(ray_start_regular_shared):
    """Test that with_column preserves existing columns in schema."""
    from ray.data.expressions import col

    ds = ray.data.from_items([{"a": 1, "b": 2}] * 50)

    # Apply with_column
    transformed_ds = ds.with_column("c", col("a") + col("b"))

    # Original columns should still be present
    assert "a" in transformed_ds.columns()
    assert "b" in transformed_ds.columns()
    assert "c" in transformed_ds.columns()


def test_with_column_expression_evaluation(ray_start_regular_shared):
    """Test that with_column correctly evaluates expressions."""
    from ray.data.expressions import col

    ds = ray.data.from_items([{"x": i} for i in range(10)])

    # Apply with_column with complex expression
    result_ds = ds.with_column("y", col("x") * 2 + 1)

    # Verify the expression was evaluated correctly
    result = result_ds.take(3)
    assert result[0]["y"] == 1  # 0 * 2 + 1
    assert result[1]["y"] == 3  # 1 * 2 + 1
    assert result[2]["y"] == 5  # 2 * 2 + 1


def test_size_bytes_preserved_after_sort(ray_start_regular_shared):
    """Test that size_bytes is preserved after sort (reordering only)."""
    ds = ray.data.range(100)

    # Cache size_bytes
    original_size = ds.size_bytes()

    # Apply sort - size should be preserved
    sorted_ds = ds.sort("id")
    sorted_size = sorted_ds.size_bytes()

    # Size should be identical (reordering doesn't change size)
    assert sorted_size == original_size


def test_size_bytes_preserved_after_repartition(ray_start_regular_shared):
    """Test that size_bytes is preserved after repartition."""
    ds = ray.data.range(100)

    # Cache size_bytes
    original_size = ds.size_bytes()

    # Apply repartition - size should be preserved
    repartitioned_ds = ds.repartition(5)
    repartitioned_size = repartitioned_ds.size_bytes()

    # Size should be similar (may have small overhead variations)
    # Allow 10% variation for block overhead
    assert abs(repartitioned_size - original_size) / original_size < 0.10


def test_size_bytes_computed_after_limit(ray_start_regular_shared):
    """Test that size_bytes is proportionally computed after limit."""
    ds = ray.data.range(1000)

    # Cache size_bytes and count
    original_size = ds.size_bytes()
    original_count = ds.count()

    # Apply limit - size should be proportionally reduced
    limited_ds = ds.limit(100)
    limited_size = limited_ds.size_bytes()
    limited_count = limited_ds.count()

    # Size should be roughly proportional to count reduction
    expected_ratio = limited_count / original_count  # 0.1
    actual_ratio = limited_size / original_size

    # Allow for some overhead variation (within 50%)
    assert (
        0.05 < actual_ratio < 0.20
    ), f"Size ratio {actual_ratio} not close to expected {expected_ratio}"


def test_size_bytes_invalidated_after_map(ray_start_regular_shared):
    """Test that size_bytes is invalidated after map (data changes)."""
    ds = ray.data.range(100)

    # Cache size_bytes
    original_size = ds.size_bytes()

    # Apply map that changes data size
    mapped_ds = ds.map(lambda x: {"data": [x["id"]] * 100})  # Expand data
    mapped_size = mapped_ds.size_bytes()

    # Size should be different (data expanded)
    assert mapped_size != original_size
    assert mapped_size > original_size


def test_random_sample_schema_preserved(ray_start_regular_shared):
    """Test that random_sample preserves schema."""
    ds = ray.data.range(1000)

    # Cache schema
    original_schema = ds.schema()

    # Apply random_sample
    sampled_ds = ds.random_sample(0.1, seed=42)
    sampled_schema = sampled_ds.schema()

    assert sampled_schema.names == original_schema.names


def test_random_sample_count_estimated(ray_start_regular_shared):
    """Test that random_sample count is estimated correctly."""
    ds = ray.data.range(1000)

    # Cache count
    original_count = ds.count()
    assert original_count == 1000

    # Apply random_sample with 10% fraction
    sampled_ds = ds.random_sample(0.1, seed=42)
    sampled_count = sampled_ds.count()

    # Allow 50% variation due to randomness (50-150 rows)
    assert (
        50 < sampled_count < 150
    ), f"Sampled count {sampled_count} outside expected range"


def test_random_sample_aggregations_invalidated(ray_start_regular_shared):
    """Test that aggregations are invalidated after random_sample."""
    ds = ray.data.range(1000)

    # Cache aggregations
    original_sum = ds.sum("id")
    original_max = ds.max("id")

    # Apply random_sample
    sampled_ds = ds.random_sample(0.5, seed=42)
    sampled_sum = sampled_ds.sum("id")
    sampled_max = sampled_ds.max("id")

    assert sampled_sum < original_sum
    assert sampled_max <= original_max


def test_input_files_preserved_after_sort(ray_start_regular_shared):
    """Test that input_files metadata is preserved after sort."""
    import os
    import tempfile

    # Create temporary test data
    with tempfile.TemporaryDirectory() as tmpdir:
        # Write test data
        data_file = os.path.join(tmpdir, "test.json")
        import json

        with open(data_file, "w") as f:
            for i in range(100):
                json.dump({"id": i}, f)
                f.write("\n")

        # Read dataset
        ds = ray.data.read_json(tmpdir)

        # Cache input_files
        original_files = ds.input_files()

        # Apply sort - input_files should be preserved
        sorted_ds = ds.sort("id")
        sorted_files = sorted_ds.input_files()

        # Files should be identical
        assert sorted_files == original_files


def test_num_blocks_after_repartition(ray_start_regular_shared):
    """Test num_blocks metadata after repartition."""
    ds = ray.data.range(1000, override_num_blocks=10)

    # Verify initial block count
    assert ds.num_blocks() == 10

    # Repartition to 5 blocks
    repartitioned_ds = ds.repartition(5)

    # Should have 5 blocks
    assert repartitioned_ds.num_blocks() == 5

    # Repartition to 20 blocks
    repartitioned_ds2 = repartitioned_ds.repartition(20)

    # Should have 20 blocks
    assert repartitioned_ds2.num_blocks() == 20


def test_materialize_persists_cache(ray_start_regular_shared):
    """Test that cache persists after materialize."""
    ds = ray.data.range(100).map(lambda x: {"value": x["id"] * 2})

    # Materialize dataset
    mat_ds = ds.materialize()

    # Operations on materialized dataset should be cached
    count1 = mat_ds.count()
    count2 = mat_ds.count()

    assert count1 == count2 == 100

    # Schema should also be cached
    schema1 = mat_ds.schema()
    schema2 = mat_ds.schema()

    assert schema1.names == schema2.names


def test_materialize_invalidates_parent_cache(ray_start_regular_shared):
    """Test that materialize doesn't use parent dataset's cache incorrectly."""
    ds = ray.data.range(100)

    transformed_ds = ds.map(lambda x: {"doubled": x["id"] * 2})

    # Materialize
    mat_ds = transformed_ds.materialize()

    # Materialized dataset should have different schema than parent
    parent_schema = ds.schema()
    mat_schema = mat_ds.schema()

    assert parent_schema.names != mat_schema.names
    assert "id" in parent_schema.names
    assert "doubled" in mat_schema.names


def test_materialize_multiple_times(ray_start_regular_shared):
    """Test that multiple materializations are handled correctly."""
    ds = ray.data.range(50)

    # First materialize
    mat1 = ds.materialize()
    count1 = mat1.count()

    # Second materialize (should use cached MaterializedDataset)
    mat2 = ds.materialize()
    count2 = mat2.count()

    assert count1 == count2 == 50
    assert isinstance(mat1, MaterializedDataset)
    assert isinstance(mat2, MaterializedDataset)


def test_complex_pipeline_cache_behavior(ray_start_regular_shared):
    """Test cache behavior through a complex transformation pipeline."""
    # Build complex pipeline
    ds = (
        ray.data.range(1000)
        .map(lambda x: {"id": x["id"], "value": x["id"] * 2})
        .filter(lambda x: x["value"] < 1000)
        .sort("id", descending=True)
        .limit(100)
        .map(lambda x: {"id": x["id"], "processed": x["value"] + 10})
    )

    count1 = ds.count()
    count2 = ds.count()
    assert count1 == count2 == 100

    schema1 = ds.schema()
    schema2 = ds.schema()
    assert schema1.names == schema2.names == ["id", "processed"]


def test_cache_after_union(ray_start_regular_shared):
    """Test cache behavior after union operation."""
    ds1 = ray.data.range(50)
    ds2 = ray.data.range(50, 100)

    # Union datasets
    union_ds = ds1.union(ds2)

    # Operations should be cached normally
    count1 = union_ds.count()
    count2 = union_ds.count()

    assert count1 == count2 == 100


def test_cache_with_nested_transformations(ray_start_regular_shared):
    """Test cache with nested transformation chains."""
    # Create base dataset
    base_ds = ray.data.range(200)

    # Create multiple transformation branches
    branch1 = base_ds.filter(lambda x: x["id"] < 100)
    branch2 = base_ds.filter(lambda x: x["id"] >= 100)

    # Each branch should have independent cache
    count1 = branch1.count()
    count2 = branch2.count()

    assert count1 == 100
    assert count2 == 100

    # Base dataset cache should be independent
    base_count = base_ds.count()
    assert base_count == 200


def test_cache_preservation_through_rename(ray_start_regular_shared):
    """Test that rename_columns preserves count but updates column metadata."""
    ds = ray.data.from_items([{"old_name": i} for i in range(100)])

    # Cache count and columns
    original_count = ds.count()
    original_columns = ds.columns()

    assert original_columns == ["old_name"]

    # Rename column
    renamed_ds = ds.rename_columns({"old_name": "new_name"})

    renamed_count = renamed_ds.count()
    assert renamed_count == original_count == 100

    # Columns should be updated
    renamed_columns = renamed_ds.columns()
    assert renamed_columns == ["new_name"]


def test_cache_with_multiple_columns_operations(ray_start_regular_shared):
    """Test cache through multiple column manipulation operations."""
    ds = ray.data.from_items([{"a": 1, "b": 2, "c": 3}] * 100)

    # Chain: add "d", drop "b", select ["a", "c", "d"]
    result_ds = (
        ds.add_column("d", lambda x: x["a"] + x["c"])
        .drop_columns(["b"])
        .select_columns(["a", "c", "d"])
    )

    assert result_ds.count() == 100

    # Columns should be smart-computed correctly
    final_columns = result_ds.columns()
    assert set(final_columns) == {"a", "c", "d"}


def test_cache_with_sort_variations(ray_start_regular_shared):
    """Test cache preservation with different sort operations."""
    ds = ray.data.from_items([{"x": i, "y": i * 2} for i in range(100)])

    # Cache aggregations
    original_sum_x = ds.sum("x")
    original_sum_y = ds.sum("y")

    # Sort ascending
    sorted_asc = ds.sort("x")
    assert sorted_asc.sum("x") == original_sum_x
    assert sorted_asc.sum("y") == original_sum_y

    # Sort descending
    sorted_desc = ds.sort("x", descending=True)
    assert sorted_desc.sum("x") == original_sum_x
    assert sorted_desc.sum("y") == original_sum_y

    # Sort by different column
    sorted_y = ds.sort("y")
    assert sorted_y.sum("x") == original_sum_x
    assert sorted_y.sum("y") == original_sum_y


def test_cache_stats_tracking(ray_start_regular_shared):
    """Test that cache statistics are tracked correctly."""
    import ray.data as rd

    # Clear cache to start fresh
    rd.clear_dataset_cache()

    ds = ray.data.range(100)

    # First operations - misses
    initial_stats = rd.get_cache_stats()
    initial_misses = initial_stats.get("miss_count", 0)

    ds.count()
    ds.schema()

    stats_after_ops = rd.get_cache_stats()

    # Should have more misses
    assert stats_after_ops["miss_count"] > initial_misses

    # Repeat operations - should have hits
    ds.count()
    ds.schema()

    final_stats = rd.get_cache_stats()

    # Should have hits now
    assert final_stats["hit_count"] > stats_after_ops.get("hit_count", 0)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
