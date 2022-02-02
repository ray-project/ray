from ray.util.client.common import (
    _id_is_newer,
    ResponseCache,
    OrderedResponseCache,
    INT32_MAX,
)
import threading
import time

import pytest


def test_id_is_newer():
    """
    Sanity checks the logic for ID is newer. In general, we would expect
    that higher IDs are newer than lower IDs, for example 25 can be assumed
    to be newer than 24.
    Since IDs roll over at INT32_MAX (~2**31), we should check for weird
    behavior there. In particular, we would expect an ID like `11` to be
    newer than the ID `2**31` since it's likely that the counter rolled
    over.
    """
    # Common cases -- higher IDs normally considered newer
    assert _id_is_newer(30, 29)
    assert _id_is_newer(12345, 12344)
    assert not _id_is_newer(12344, 12345)
    assert not _id_is_newer(5678, 5678)

    # Check behavior near max int boundary
    assert _id_is_newer(INT32_MAX, INT32_MAX - 1)
    assert _id_is_newer(INT32_MAX - 1, INT32_MAX - 2)

    # Low IDs are assumed newer than higher ones if it looks like rollover has
    # occurred
    assert _id_is_newer(0, INT32_MAX - 4)
    assert _id_is_newer(1001, INT32_MAX - 123)
    assert not _id_is_newer(INT32_MAX, 123)


def test_response_cache_complete_response():
    """
    Test basic check/update logic of cache, and that nothing blocks
    """
    cache = ResponseCache()
    cache.check_cache(123, 15)  # shouldn't block
    cache.update_cache(123, 15, "abcdef")
    assert cache.check_cache(123, 15) == "abcdef"


def test_ordered_response_cache_complete_response():
    """
    Test basic check/update logic of ordered cache, and that nothing blocks
    """
    cache = OrderedResponseCache()
    cache.check_cache(15)  # shouldn't block
    cache.update_cache(15, "vwxyz")
    assert cache.check_cache(15) == "vwxyz"


def test_response_cache_incomplete_response():
    """
    Tests case where a cache entry is populated after a long time. Any new
    threads attempting to access that entry should sleep until the response
    is ready.
    """
    cache = ResponseCache()

    def populate_cache():
        time.sleep(2)
        cache.update_cache(123, 15, "abcdef")

    cache.check_cache(123, 15)  # shouldn't block
    t = threading.Thread(target=populate_cache, args=())
    t.start()
    # Should block until other thread populates cache
    assert cache.check_cache(123, 15) == "abcdef"
    t.join()


def test_ordered_response_cache_incomplete_response():
    """
    Tests case where an ordered cache entry is populated after a long time. Any
    new threads attempting to access that entry should sleep until the response
    is ready.
    """
    cache = OrderedResponseCache()

    def populate_cache():
        time.sleep(2)
        cache.update_cache(15, "vwxyz")

    cache.check_cache(15)  # shouldn't block
    t = threading.Thread(target=populate_cache, args=())
    t.start()
    # Should block until other thread populates cache
    assert cache.check_cache(15) == "vwxyz"
    t.join()


def test_ordered_response_cache_cleanup():
    """
    Tests that the cleanup method of ordered cache works as expected, in
    particular that all entries <= the passed ID are cleared from the cache.
    """
    cache = OrderedResponseCache()

    for i in range(1, 21):
        assert cache.check_cache(i) is None
        cache.update_cache(i, str(i))

    assert len(cache.cache) == 20
    for i in range(1, 21):
        assert cache.check_cache(i) == str(i)

    # Expected: clean up all entries up to and including entry 10
    cache.cleanup(10)
    assert len(cache.cache) == 10

    with pytest.raises(RuntimeError):
        # Attempting to access value that has already been cleaned up
        cache.check_cache(10)

    for i in range(21, 31):
        # Check that more entries can be inserted
        assert cache.check_cache(i) is None
        cache.update_cache(i, str(i))

    # Cleanup everything
    cache.cleanup(30)
    assert len(cache.cache) == 0

    with pytest.raises(RuntimeError):
        cache.check_cache(30)

    # Cleanup requests received out of order are tolerated
    cache.cleanup(27)
    cache.cleanup(23)


def test_response_cache_update_while_waiting():
    """
    Tests that an error is thrown when a cache entry is updated with the
    response for a different request than what was originally being
    checked for.
    """
    # Error when awaiting cache to update, but entry is cleaned up
    cache = ResponseCache()
    assert cache.check_cache(16, 123) is None

    def cleanup_cache():
        time.sleep(2)
        cache.check_cache(16, 124)
        cache.update_cache(16, 124, "asdf")

    t = threading.Thread(target=cleanup_cache, args=())
    t.start()

    with pytest.raises(RuntimeError):
        cache.check_cache(16, 123)
    t.join()


def test_ordered_response_cache_cleanup_while_waiting():
    """
    Tests that an error is thrown when an ordered cache entry is updated with
    the response for a different request than what was originally being
    checked for.
    """
    # Error when awaiting cache to update, but entry is cleaned up
    cache = OrderedResponseCache()
    assert cache.check_cache(123) is None

    def cleanup_cache():
        time.sleep(2)
        cache.cleanup(123)

    t = threading.Thread(target=cleanup_cache, args=())
    t.start()

    with pytest.raises(RuntimeError):
        cache.check_cache(123)
    t.join()


def test_response_cache_cleanup():
    """
    Checks that the response cache replaces old entries for a given thread
    with new entries as they come in, instead of creating new entries
    (possibly wasting memory on unneeded entries)
    """
    # Check that the response cache cleans up previous entries for a given
    # thread properly.
    cache = ResponseCache()
    cache.check_cache(16, 123)
    cache.update_cache(16, 123, "Some response")
    assert len(cache.cache) == 1

    cache.check_cache(16, 124)
    cache.update_cache(16, 124, "Second response")
    assert len(cache.cache) == 1  # Should reuse entry for thread 16
    assert cache.check_cache(16, 124) == "Second response"


def test_response_cache_invalidate():
    """
    Check that ordered response cache invalidate works as expected
    """
    cache = OrderedResponseCache()
    e = RuntimeError("SomeError")
    # No pending entries, cache should be valid
    assert not cache.invalidate(e)
    # No entry for 123 yet
    assert cache.check_cache(123) is None
    # this should invalidate the entry for 123
    assert cache.invalidate(e)
    assert cache.check_cache(123) == e
    assert cache.invalidate(e)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
