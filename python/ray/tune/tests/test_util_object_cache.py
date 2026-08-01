import pytest

from ray.tune.utils.object_cache import _ObjectCache


@pytest.mark.parametrize("eager", [False, True])
def test_no_may_keep_one(eager):
    """Test object caching.

    - After init, no objects are cached (as max cached is 0), except when eager caching
    - After increasing max to 2, up to 2 objects are cached
    - Decreasing max objects will evict them on flush
    """
    cache = _ObjectCache(may_keep_one=eager)

    # max(A) = 0, so we we only cache when eager caching
    assert cache.cache_object("A", 1) == eager
    assert cache.num_cached_objects == int(eager)

    # Set max(A) = 2
    cache.increase_max("A", 2)

    # max(A) = 2, so we cache up to two objects
    if not eager:
        assert cache.cache_object("A", 1)

    assert cache.cache_object("A", 2)
    assert not cache.cache_object("A", 3)

    assert cache.num_cached_objects == 2

    # Nothing has to be evicted
    assert not list(cache.flush_cached_objects())

    # Set max(A) = 1, so we have one object too much
    cache.decrease_max("A", 1)

    # First cached object is evicted
    assert list(cache.flush_cached_objects()) == [1]
    assert cache.num_cached_objects == 1

    # Set max(A) = 0
    cache.decrease_max("A", 1)

    # Second cached object is evicted if not eager caching
    assert list(cache.flush_cached_objects()) == ([2] if not eager else [])
    assert cache.num_cached_objects == (0 if not eager else 1)


@pytest.mark.parametrize("eager", [False, True])
def test_multi(eager):
    """Test caching with multiple objects"""
    cache = _ObjectCache(may_keep_one=eager)

    # max(A) = 0, so we we only cache when eager caching
    assert cache.cache_object("A", 1) == eager
    assert cache.num_cached_objects == int(eager)

    # max(B) = 0, so no caching
    assert not cache.cache_object("B", 5)
    assert cache.num_cached_objects == int(eager)

    # Increase maximums levels
    cache.increase_max("A", 1)
    cache.increase_max("B", 1)

    # Cache objects (A is already cached if eager)
    assert cache.cache_object("A", 1) != eager
    assert cache.cache_object("B", 5)

    # No further objects can be cached
    assert not cache.cache_object("A", 2)
    assert not cache.cache_object("B", 6)

    assert cache.num_cached_objects == 2

    # Decrease
    cache.decrease_max("A", 1)

    # Evict A object
    assert list(cache.flush_cached_objects()) == [1]

    cache.decrease_max("B", 1)

    # If eager, keep B object, otherwise, evict B
    assert list(cache.flush_cached_objects()) == ([5] if not eager else [])
    assert cache.num_cached_objects == (0 if not eager else 1)


def test_multi_eager_other():
    """On eager caching, only cache an object if no other object is expected.

    - Expect up to one cached A object
    - Try to cache object B --> doesn't get cached
    - Remove expectation for A object
    - Try to cache object B --> get's cached
    """
    cache = _ObjectCache(may_keep_one=True)

    cache.increase_max("A", 1)
    assert not cache.cache_object("B", 2)

    cache.decrease_max("A", 1)
    assert cache.cache_object("B", 3)


@pytest.mark.parametrize("eager", [False, True])
def test_force_all(eager):
    """Assert that force_all=True will always evict all object."""
    cache = _ObjectCache(may_keep_one=eager)

    cache.increase_max("A", 2)

    assert cache.cache_object("A", 1)
    assert cache.cache_object("A", 2)

    assert list(cache.flush_cached_objects(force_all=True)) == [1, 2]
    assert cache.num_cached_objects == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
