"""Tests for @ray.remote decorator num_returns validation."""

import pytest

import ray


class TestRemoteNumReturns:
    """Test num_returns validation for @ray.remote decorator."""

    def test_num_returns_negative_raises_error(self):
        """Test that num_returns < 0 raises ValueError at decoration time."""
        # Option validation happens before validate_num_returns, so it raises
        # a different error message, but still validates that negative values fail fast.
        with pytest.raises(ValueError, match="non-negative integer"):

            @ray.remote(num_returns=-1)
            def f():
                return 1

    def test_num_returns_streaming_with_non_generator_raises_error(self):
        """Test that num_returns='streaming' with non-generator raises ValueError."""
        with pytest.raises(
            ValueError, match="num_returns='streaming' can only be used with generator"
        ):

            @ray.remote(num_returns="streaming")
            def f():
                return 1

    def test_num_returns_dynamic_with_non_generator_raises_error(self):
        """Test that num_returns='dynamic' with non-generator raises ValueError."""
        with pytest.raises(
            ValueError, match="num_returns='dynamic' can only be used with generator"
        ):

            @ray.remote(num_returns="dynamic")
            def f():
                return 1

    def test_num_returns_streaming_with_generator_succeeds(self):
        """Test that num_returns='streaming' with generator function succeeds."""
        # This should not raise an error at decoration time
        @ray.remote(num_returns="streaming")
        def generator_func():
            for i in range(3):
                yield i

        # Verify it's a remote function (no error was raised)
        assert generator_func is not None

    def test_num_returns_dynamic_with_generator_succeeds(self):
        """Test that num_returns='dynamic' with generator function succeeds."""
        # This should not raise an error at decoration time
        @ray.remote(num_returns="dynamic")
        def generator_func():
            for i in range(3):
                yield i

        # Verify it's a remote function (no error was raised)
        assert generator_func is not None

    def test_num_returns_streaming_with_async_generator_succeeds(self):
        """Test that num_returns='streaming' with async generator function succeeds."""
        # This should not raise an error at decoration time
        @ray.remote(num_returns="streaming")
        async def async_generator_func():
            for i in range(3):
                yield i

        # Verify it's a remote function (no error was raised)
        assert async_generator_func is not None

    def test_num_returns_positive_integer_succeeds(self):
        """Test that num_returns with positive integer succeeds."""

        @ray.remote(num_returns=2)
        def f():
            return 1, 2

        # Verify it's a remote function (no error was raised)
        assert f is not None

    def test_num_returns_zero_succeeds(self):
        """Test that num_returns=0 succeeds."""

        @ray.remote(num_returns=0)
        def f():
            return

        # Verify it's a remote function (no error was raised)
        assert f is not None

    def test_num_returns_none_succeeds(self):
        """Test that num_returns=None succeeds."""

        @ray.remote(num_returns=None)
        def f():
            return 1

        # Verify it's a remote function (no error was raised)
        assert f is not None

    def test_num_returns_default_succeeds(self):
        """Test that default num_returns (not specified) succeeds."""

        @ray.remote
        def f():
            return 1

        # Verify it's a remote function (no error was raised)
        assert f is not None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
