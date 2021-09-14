from ray.util.client.common import (_id_is_newer, ResponseCache,
                                    OrderedResponseCache, INT32_MAX)
import threading
import time

import pytest


def test_id_is_newer():
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
    cache = ResponseCache()
    cache.check_cache(123, 15)  # shouldn't block
    cache.update_cache(123, 15, "abcdef")
    assert cache.check_cache(123, 15) == "abcdef"


def test_ordered_response_cache_complete_response():
    cache = OrderedResponseCache()
    cache.check_cache(15)  # shouldn't block
    cache.update_cache(15, "vwxyz")
    assert cache.check_cache(15) == "vwxyz"


def test_response_cache_incomplete_response():
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


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
