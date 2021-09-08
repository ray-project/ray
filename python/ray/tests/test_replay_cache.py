from ray.util.client.common import (_id_is_newer, ReplayCache,
                                    OrderedReplayCache, INT32_MAX)
import threading
import time

import pytest


def test_id_is_newer():
    # Common cases -- higher IDs normally considered newer
    assert _id_is_newer(30, 29)
    assert _id_is_newer(12345, 12344)
    assert not _id_is_newer(12344, 12345)

    # Check behavior near max int boundary
    assert _id_is_newer(INT32_MAX, INT32_MAX - 1)
    assert _id_is_newer(INT32_MAX - 1, INT32_MAX - 2)

    # Low IDs are assumed newer than higher if it looks like rollover has
    # occurred
    assert _id_is_newer(0, INT32_MAX - 4)
    assert _id_is_newer(1001, INT32_MAX - 123)
    assert not _id_is_newer(INT32_MAX, 123)


def test_replay_cache_complete_response():
    cache = ReplayCache()
    cache.check_cache(123, 15)  # shouldn't block
    cache.update_cache(123, 15, "abcdef")
    assert cache.check_cache(123, 15) == "abcdef"


def test_ordered_replay_cache_complete_response():
    cache = OrderedReplayCache()
    cache.check_cache(15)  # shouldn't block
    cache.update_cache(15, "vwxyz")
    assert cache.check_cache(15) == "vwxyz"


def test_replay_cache_incomplete_response():
    cache = ReplayCache()

    def populate_cache():
        time.sleep(3)
        cache.update_cache(123, 15, "abcdef")

    cache.check_cache(123, 15)  # shouldn't block
    t = threading.Thread(target=populate_cache, args=())
    t.start()
    # Should block until other thread populates cache
    assert cache.check_cache(123, 15) == "abcdef"
    t.join()


def test_ordered_replay_cache_incomplete_response():
    cache = OrderedReplayCache()

    def populate_cache():
        time.sleep(3)
        cache.update_cache(15, "vwxyz")

    cache.check_cache(15)  # shouldn't block
    t = threading.Thread(target=populate_cache, args=())
    t.start()
    # Should block until other thread populates cache
    assert cache.check_cache(15) == "vwxyz"
    t.join()


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
