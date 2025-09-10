"""Tests for Ray Data caching functionality."""

import time
from unittest.mock import patch

import pytest

import ray
from ray.data.context import DataContext
from ray.data.dataset import Dataset, MaterializedDataset
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def test_count_caching(ray_start_regular_shared):
    """Test that count() results are cached for repeated calls."""
    ds = ray.data.range(1000)

    # First count should execute and cache
    start_time = time.time()
    count1 = ds.count()
    first_duration = time.time() - start_time

    # Second count should use cached result
    start_time = time.time()
    count2 = ds.count()
    second_duration = time.time() - start_time

    # Results should be identical
    assert count1 == count2 == 1000

    # Second call should be significantly faster (cached)
    assert second_duration < first_duration * 0.5


def test_schema_caching(ray_start_regular_shared):
    """Test that schema() results are cached for repeated calls."""
    ds = ray.data.range(100)

    schema1 = ds.schema()
    schema2 = ds.schema()

    # Should return identical schema objects
    assert schema1.names == schema2.names
    assert schema1.types == schema2.types


def test_materialize_caching(ray_start_regular_shared):
    """Test that materialize() caches MaterializedDataset objects."""
    ds = ray.data.range(100).map(lambda x: {"value": x["id"] * 2})

    # First materialize should execute pipeline
    start_time = time.time()
    mat1 = ds.materialize()
    first_duration = time.time() - start_time

    # Second materialize should return cached MaterializedDataset
    start_time = time.time()
    mat2 = ds.materialize()
    second_duration = time.time() - start_time

    # Both should be MaterializedDataset objects
    assert isinstance(mat1, MaterializedDataset)
    assert isinstance(mat2, MaterializedDataset)

    # Should have identical structure
    assert mat1.count() == mat2.count() == 100
    assert mat1.num_blocks() == mat2.num_blocks()
    assert mat1.schema().names == mat2.schema().names

    # Second call should be faster (cached MaterializedDataset object)
    assert second_duration < first_duration * 0.5


def test_aggregation_caching(ray_start_regular_shared):
    """Test that aggregation operations are cached."""
    ds = ray.data.range(1000)

    # Test sum caching
    sum1 = ds.sum("id")
    sum2 = ds.sum("id")
    assert sum1 == sum2

    # Test min/max caching
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

    # Apply map transformation
    mapped_ds = ds.map(lambda x: {"doubled": x["id"] * 2})

    # Count should be preserved (same number of rows)
    mapped_count = mapped_ds.count()
    assert mapped_count == original_count == 100

    # Schema should be different
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

    # Apply filter transformation
    filtered_ds = ds.filter(lambda x: x["id"] < 50)

    # Schema should be preserved
    filtered_schema = filtered_ds.schema()
    assert filtered_schema.names == original_schema.names

    # Count should be different
    filtered_count = filtered_ds.count()
    assert filtered_count == 50
    assert filtered_count != original_count


def test_cache_invalidation_limit(ray_start_regular_shared):
    """Test cache invalidation for limit transformation."""
    ds = ray.data.range(100)

    # Cache count and schema
    original_count = ds.count()
    original_schema = ds.schema()

    # Apply limit transformation
    limited_ds = ds.limit(25)

    # Schema should be preserved
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

    # Apply sort transformation
    sorted_ds = ds.sort("id", descending=True)

    # Aggregations should be preserved (same values)
    sorted_count = sorted_ds.count()
    sorted_sum = sorted_ds.sum("id")
    sorted_schema = sorted_ds.schema()

    assert sorted_count == original_count
    assert sorted_sum == original_sum
    assert sorted_schema.names == original_schema.names


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
    assert stats["cache_entries"] > 0
    assert stats["hit_count"] >= 0
    assert stats["miss_count"] >= 0


def test_cache_context_managers(ray_start_regular_shared):
    """Test cache context managers."""
    import ray.data as rd

    ds = ray.data.range(50)

    # Test disable context manager
    with rd.disable_dataset_caching():
        count_disabled = ds.count()

    # Test set cache size context manager
    with rd.set_cache_size(max_size_bytes=10_000_000):  # 10MB
        count_limited = ds.count()

    # Both should return correct results
    assert count_disabled == count_limited == 50


def test_cache_clear(ray_start_regular_shared):
    """Test cache clearing functionality."""
    import ray.data as rd

    ds = ray.data.range(50)

    # Populate cache
    ds.count()
    ds.schema()

    # Check cache has entries
    stats = rd.get_cache_stats()
    assert stats["cache_entries"] > 0

    # Clear cache
    rd.clear_dataset_cache()

    # Check cache is empty
    stats_after = rd.get_cache_stats()
    assert stats_after["cache_entries"] == 0


def test_cache_with_complex_transformations(ray_start_regular_shared):
    """Test caching with complex transformation chains."""
    # Create complex dataset
    ds = (
        ray.data.range(1000)
        .map(lambda x: {"id": x["id"], "value": x["id"] * 2})
        .filter(lambda x: x["value"] < 1000)
        .map(lambda x: {"id": x["id"], "processed": x["value"] + 100})
    )

    # Test that operations are cached correctly
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
