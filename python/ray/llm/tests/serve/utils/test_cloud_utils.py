import sys
import pytest
import time
import asyncio
from ray.llm._internal.serve.deployments.utils.cloud_utils import CloudObjectCache

# Mock fetch functions for testing
def sync_fetch(key: str):
    if key == "missing":
        return None
    return f"value-{key}"


async def async_fetch(key: str):
    if key == "missing":
        return None
    return f"value-{key}"


def test_sync_cache_basic():
    """Test basic synchronous cache functionality."""
    cache = CloudObjectCache(max_size=2, fetch_fn=sync_fetch)

    # Test fetching a value
    assert cache.get("key1") == "value-key1"

    # Test cache hit
    assert cache.get("key1") == "value-key1"

    assert len(cache) == 1

    # Test cache size limit
    cache.get("key2")
    cache.get("key3")  # This should evict key1

    assert len(cache) == 2

    # Verify key1 was evicted by checking if it's fetched again
    assert cache.get("key1") == "value-key1"


def test_sync_cache_expiration():
    """Test cache expiration for both missing and existing objects."""
    cache = CloudObjectCache(
        max_size=2,
        fetch_fn=sync_fetch,
        missing_expire_seconds=1,
        exists_expire_seconds=2,
    )

    # Test missing object expiration
    assert cache.get("missing") is None
    time.sleep(1.1)  # Wait for missing object to expire
    assert cache.get("missing") is None  # Should fetch again

    # Test existing object expiration
    assert cache.get("key1") == "value-key1"
    time.sleep(1)  # Not expired yet
    assert cache.get("key1") == "value-key1"  # Should hit cache
    time.sleep(1.1)  # Now expired
    assert cache.get("key1") == "value-key1"  # Should fetch again


@pytest.mark.asyncio
async def test_async_cache_basic():
    """Test basic asynchronous cache functionality."""
    cache = CloudObjectCache(max_size=2, fetch_fn=async_fetch)

    # Test fetching a value
    assert await cache.aget("key1") == "value-key1"

    # Test cache hit
    assert await cache.aget("key1") == "value-key1"

    # Test cache size limit
    await cache.aget("key2")
    await cache.aget("key3")  # This should evict key1

    # Verify key1 was evicted by checking if it's fetched again
    assert await cache.aget("key1") == "value-key1"


@pytest.mark.asyncio
async def test_async_cache_expiration():
    """Test async cache expiration for both missing and existing objects."""
    cache = CloudObjectCache(
        max_size=2,
        fetch_fn=async_fetch,
        missing_expire_seconds=1,
        exists_expire_seconds=2,
    )

    # Test missing object expiration
    assert await cache.aget("missing") is None
    await asyncio.sleep(1.1)  # Wait for missing object to expire
    assert await cache.aget("missing") is None  # Should fetch again

    # Test existing object expiration
    assert await cache.aget("key1") == "value-key1"
    await asyncio.sleep(1)  # Not expired yet
    assert await cache.aget("key1") == "value-key1"  # Should hit cache
    await asyncio.sleep(1.1)  # Now expired
    assert await cache.aget("key1") == "value-key1"  # Should fetch again


def test_sync_async_mismatch():
    """Test that using sync get with async fetch function raises error."""
    cache = CloudObjectCache(max_size=2, fetch_fn=async_fetch)
    with pytest.raises(ValueError, match="Cannot use sync get.*"):
        cache.get("key1")


def test_cache_max_size():
    """Test that cache respects max size limit."""
    cache = CloudObjectCache(max_size=2, fetch_fn=sync_fetch)

    # Fill cache
    cache.get("key1")
    cache.get("key2")

    # Add one more item, should evict oldest
    cache.get("key3")

    # Verify size is still 2
    assert len(cache._cache) == 2

    # Verify oldest item was evicted
    assert "key1" not in cache._cache
    assert "key2" in cache._cache
    assert "key3" in cache._cache


def test_custom_missing_value():
    """Test cache with custom missing object value."""
    MISSING = object()
    cache = CloudObjectCache(
        max_size=2,
        fetch_fn=lambda k: MISSING if k == "missing" else k,
        missing_object_value=MISSING,
    )

    # Test with missing value
    result = cache.get("missing")
    assert result is MISSING

    # Test with existing value
    assert cache.get("exists") == "exists"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
