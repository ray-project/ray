import pytest

from ray.data._internal.utils.cache import (
    _disable_timed_cache_for_tests,
    timed_cache,
)


class _FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_cache_miss():
    TEST_VALUE = 1

    @timed_cache(ttl=10000)
    def get_value():
        return TEST_VALUE

    # cache miss, should return TEST_VALUE
    v = get_value()
    assert v == TEST_VALUE, v


def test_cache_hit():
    TEST_VALUE = 1

    clock = _FakeClock()

    @timed_cache(ttl=10000, get_time_fn=clock)
    def get_value():
        return TEST_VALUE

    # cache miss, should return TEST_VALUE
    v = get_value()
    assert v == TEST_VALUE, v

    # ttl not expired, should return prev_value
    clock.advance(9999)
    prev_value = TEST_VALUE
    TEST_VALUE += 1
    v = get_value()
    assert v == prev_value, (v, prev_value)


def test_cache_ttl_expire():
    TEST_VALUE = 1

    clock = _FakeClock()

    @timed_cache(ttl=10000, get_time_fn=clock)
    def get_value():
        return TEST_VALUE

    # cache miss, should return TEST_VALUE
    v = get_value()
    assert v == TEST_VALUE, v

    # ttl expired, should return new TEST_VALUE
    clock.advance(10001)
    TEST_VALUE += 1
    v = get_value()
    assert v == TEST_VALUE, (v, TEST_VALUE)


def test_cache_keys():
    clock = _FakeClock()

    @timed_cache(ttl=10000, get_time_fn=clock)
    def get_value(x):
        return x

    # cache miss
    v = get_value(0)
    assert v == 0, v

    # cache miss
    v = get_value(1)
    assert v == 1, v


def test_cache_keys_expire():
    clock = _FakeClock()
    calls = []

    @timed_cache(ttl=10000, get_time_fn=clock)
    def get_value(x):
        calls.append(x)
        return x

    # Cache both keys, 9999 apart.
    assert get_value(0) == 0
    clock.advance(9999)
    assert get_value(1) == 1
    assert calls == [0, 1]

    # Key 0 is now 10001 old and has expired; key 1 is only 2 old.
    clock.advance(2)
    assert get_value(0) == 0
    assert calls == [0, 1, 0], "key 0 expired, so it should be recomputed"
    assert get_value(1) == 1
    assert calls == [0, 1, 0], "key 1 is still live, so it should be cached"


def test_cache_unhashable_args():
    @timed_cache(ttl=10000)
    def get_value(x):
        return x

    with pytest.raises(ValueError, match="hashable"):
        get_value(["not", "hashable"])


def test_disable_timed_cache_for_tests():
    num_calls = 0

    @timed_cache(ttl=10000)
    def get_value():
        nonlocal num_calls
        num_calls += 1
        return num_calls

    with _disable_timed_cache_for_tests():
        assert get_value() == 1
        assert get_value() == 2

    # Caching resumes once the context manager exits.
    assert get_value() == 3
    assert get_value() == 3


def test_disable_timed_cache_restored_on_error():
    @timed_cache(ttl=10000)
    def get_value():
        return 1

    with pytest.raises(RuntimeError):
        with _disable_timed_cache_for_tests():
            raise RuntimeError("boom")

    from ray.data._internal.utils import cache

    assert cache._IS_TIMED_CACHE_ENABLED


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
