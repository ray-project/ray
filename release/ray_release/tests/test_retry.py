import sys

import pytest

from ray_release import retry


def test_retry_with_no_error():
    invocation_count = 0

    # Function doesn't raise exception; use a dummy value to check invocation.
    @retry.retry()
    def no_error_func() -> int:
        nonlocal invocation_count
        invocation_count += 1
        return 1

    assert no_error_func() == 1
    assert invocation_count == 1


# Test senario: exception count is less than retry count.
def test_retry_with_limited_error():
    invocation_count = 0

    # Function doesn't raise exception; use a dummy value to check invocation.
    @retry.retry(init_delay_sec=1, jitter_sec=1)
    def limited_error() -> int:
        nonlocal invocation_count

        invocation_count += 1

        if invocation_count == 1:
            raise Exception("Manual exception")
        return 1

    assert limited_error() == 1
    assert invocation_count == 2


# Test senario: exception count exceeds retry count.
def test_retry_with_unlimited_error():
    invocation_count = 0

    @retry.retry(init_delay_sec=1, jitter_sec=1, backoff=1, max_retry_count=3)
    def unlimited_error() -> int:
        nonlocal invocation_count

        invocation_count += 1
        raise Exception("Manual exception")

    with pytest.raises(Exception, match="Manual exception"):
        unlimited_error()
    assert invocation_count == 3


def test_retry_on_certain_errors():
    invocation_count = 0

    # Function doesn't raise exception; use a dummy value to check invocation.
    @retry.retry(init_delay_sec=1, jitter_sec=1, exceptions=(KeyError,))
    def limited_error() -> int:
        nonlocal invocation_count

        invocation_count += 1

        if invocation_count == 1:
            raise KeyError("Manual exception")
        return 1

    assert limited_error() == 1
    assert invocation_count == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
