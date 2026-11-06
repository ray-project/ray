import sys

import pytest

from ray._common.retry import (
    call_with_retry,
    retry,
)


def test_call_with_retry_immediate_success_with_args():
    def func(a, b):
        return [a, b]

    assert call_with_retry(func, "func", [], 1, 0, "a", "b") == ["a", "b"]


def test_retry_immediate_success_with_object_args():
    class MyClass:
        @retry("func", [], 1, 0)
        def func(self, a, b):
            return [a, b]

    assert MyClass().func("a", "b") == ["a", "b"]


@pytest.mark.parametrize("use_decorator", [True, False])
def test_retry_last_attempt_successful_with_appropriate_wait_time(
    monkeypatch, use_decorator
):
    sleep_total = 0

    def sleep(x):
        nonlocal sleep_total
        sleep_total += x

    monkeypatch.setattr("time.sleep", sleep)
    monkeypatch.setattr("random.uniform", lambda a, b: 1)

    pattern = "have not reached 4th attempt"
    call_count = 0

    def func():
        nonlocal call_count
        call_count += 1
        if call_count == 4:
            return "success"
        raise ValueError(pattern)

    args = ["func", [pattern], 4, 3]
    if use_decorator:
        assert retry(*args)(func)() == "success"
    else:
        assert call_with_retry(func, *args) == "success"
    assert sleep_total == 6  # 1 + 2 + 3


@pytest.mark.parametrize("use_decorator", [True, False])
def test_retry_unretryable_error(use_decorator):
    call_count = 0

    def func():
        nonlocal call_count
        call_count += 1
        raise ValueError("unretryable error")

    args = ["func", ["only retryable error"], 10, 0]
    with pytest.raises(ValueError, match="unretryable error"):
        if use_decorator:
            retry(*args)(func)()
        else:
            call_with_retry(func, *args)
    assert call_count == 1


@pytest.mark.parametrize("use_decorator", [True, False])
def test_retry_fail_all_attempts_retry_all_errors(use_decorator):
    call_count = 0

    def func():
        nonlocal call_count
        call_count += 1
        raise ValueError(str(call_count))

    args = ["func", None, 3, 0]
    with pytest.raises(ValueError):
        if use_decorator:
            retry(*args)(func)()
        else:
            call_with_retry(func, *args)
    assert call_count == 3


def test_call_with_retry_matches_class_name():
    """Patterns can match the exception class name (e.g., 'RateLimit')."""

    class RateLimitError(Exception):
        pass

    call_count = 0

    def func():
        nonlocal call_count
        call_count += 1
        raise RateLimitError("Error code: 429")

    with pytest.raises(RateLimitError):
        call_with_retry(func, "func", ["RateLimit"], 3, 0)
    assert call_count == 3


@pytest.mark.parametrize(
    "pattern,should_retry",
    [
        # Valid regex that is not a literal substring, that matches via regex search
        (r"\d{3}", True),
        # Invalid regex, re.error is handled by returning False and the error is not retried.
        (r"[unclosed", False),
    ],
)
def test_call_with_retry_regex_matching(pattern, should_retry):
    call_count = 0

    def func():
        nonlocal call_count
        call_count += 1
        raise ValueError("Error code: 429")

    with pytest.raises(ValueError):
        call_with_retry(func, "func", [pattern], 3, 0)

    assert call_count == (3 if should_retry else 1)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
