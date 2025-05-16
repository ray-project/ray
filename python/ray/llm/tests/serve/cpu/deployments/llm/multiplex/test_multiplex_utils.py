import sys
from unittest.mock import Mock, patch, call

import pytest

from ray.llm._internal.serve.deployments.llm.multiplex.utils import (
    retry_with_exponential_backoff,
)


def test_retry_success_first_try():
    """Test that the function works normally when no exceptions occur."""
    # Use a simple counter to track calls instead of a mock

    def success_function(*args, **kwargs):
        return "success"

    # Apply our retry decorator
    decorated_fn = retry_with_exponential_backoff(
        max_tries=3, exception_to_check=ValueError
    )(success_function)

    # Call the decorated function
    result = decorated_fn("arg1", "arg2", kwarg1="kwarg1")

    # Verify it returned the expected result
    assert result == "success"


def test_retry_success_after_retries():
    """Test that the function retries and eventually succeeds."""
    # Create a mock that raises ValueError twice then succeeds
    mock_fn = Mock(side_effect=[ValueError("error1"), ValueError("error2"), "success"])

    # Apply our retry decorator with a small delay for faster testing
    decorated_fn = retry_with_exponential_backoff(
        max_tries=3, exception_to_check=ValueError, base_delay=0.01
    )(mock_fn)

    # Call the decorated function
    result = decorated_fn()

    # Verify it returned the expected result
    assert result == "success"

    # Verify the mock was called three times
    assert mock_fn.call_count == 3


def test_retry_exhaustion():
    """Test that the function gives up after max_tries."""
    # Create a mock that always raises ValueError
    error = ValueError("persistent error")
    mock_fn = Mock(side_effect=error)

    # Apply our retry decorator with a small delay for faster testing
    decorated_fn = retry_with_exponential_backoff(
        max_tries=3, exception_to_check=ValueError, base_delay=0.01
    )(mock_fn)

    # Call the decorated function and expect it to raise ValueError
    with pytest.raises(ValueError) as excinfo:
        decorated_fn()

    # Verify the error is the same one our mock raised
    assert excinfo.value is error

    # Verify the mock was called max_tries times
    assert mock_fn.call_count == 3


def test_retry_wrong_exception():
    """Test that the function doesn't retry for non-matching exceptions."""
    # Create a mock that raises TypeError
    error = TypeError("wrong error type")
    mock_fn = Mock(side_effect=error)

    # Apply our retry decorator
    decorated_fn = retry_with_exponential_backoff(
        max_tries=3, exception_to_check=ValueError
    )(mock_fn)

    # Call the decorated function and expect it to raise TypeError immediately
    with pytest.raises(TypeError) as excinfo:
        decorated_fn()

    # Verify the error is the same one our mock raised
    assert excinfo.value is error

    # Verify the mock was called only once
    mock_fn.assert_called_once()


def test_retry_backoff_timing():
    """Test that the function backs off with the expected delays."""
    # Create a mock that always raises ValueError
    mock_fn = Mock(side_effect=ValueError("error"))

    # Patch time.sleep so we can verify the delay
    with patch("time.sleep") as mock_sleep:
        # Apply our retry decorator with specific backoff parameters
        decorated_fn = retry_with_exponential_backoff(
            max_tries=4,
            exception_to_check=ValueError,
            base_delay=1,
            max_delay=8,
            exponential_base=2,
        )(mock_fn)

        # Call the decorated function and expect it to raise ValueError
        with pytest.raises(ValueError):
            decorated_fn()

        # Verify sleep was called with the expected delays
        # First retry: 1s, Second retry: 2s, Third retry: 4s
        mock_sleep.assert_has_calls([call(1), call(2), call(4)])


def test_retry_max_delay():
    """Test that the delay is capped at max_delay."""
    # Create a mock that always raises ValueError
    mock_fn = Mock(side_effect=ValueError("error"))

    # Patch time.sleep so we can verify the delay
    with patch("time.sleep") as mock_sleep:
        # Apply our retry decorator with max_delay=3
        decorated_fn = retry_with_exponential_backoff(
            max_tries=4,
            exception_to_check=ValueError,
            base_delay=2,
            max_delay=3,
            exponential_base=2,
        )(mock_fn)

        # Call the decorated function and expect it to raise ValueError
        with pytest.raises(ValueError):
            decorated_fn()

        # Verify sleep was called with delays capped at max_delay
        # First retry: 2s, Second retry: 3s (not 4s), Third retry: 3s (not 8s)
        mock_sleep.assert_has_calls([call(2), call(3), call(3)])


def test_retry_preserves_function_metadata():
    """Test that the decorator preserves the function's metadata."""
    # Define a function with docstring and name
    def test_function(x, y):
        """Test function docstring."""
        return x + y

    # Apply our retry decorator
    decorated_fn = retry_with_exponential_backoff(
        max_tries=3, exception_to_check=ValueError
    )(test_function)

    # Verify the metadata is preserved
    assert decorated_fn.__name__ == "test_function"
    assert decorated_fn.__doc__ == "Test function docstring."

    # Verify the function still works correctly
    assert decorated_fn(1, 2) == 3


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
