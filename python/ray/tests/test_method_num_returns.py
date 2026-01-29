"""Tests for @ray.method decorator num_returns validation."""

import pytest

import ray


class TestMethodNumReturns:
    """Test num_returns validation for @ray.method decorator."""

    def test_num_returns_negative_raises_error(self, shutdown_only):
        """Test that num_returns < 0 raises ValueError at decoration time."""
        # validate_num_returns checks for negative values and raises an error
        with pytest.raises(ValueError, match="num_returns must be >= 0"):

            @ray.remote
            class TestActor:
                @ray.method(num_returns=-1)
                def method(self):
                    return 1

    def test_num_returns_streaming_with_non_generator_raises_error(self, shutdown_only):
        """Test that num_returns='streaming' with non-generator raises ValueError."""
        with pytest.raises(
            ValueError, match="num_returns='streaming' can only be used with generator"
        ):

            @ray.remote
            class TestActor:
                @ray.method(num_returns="streaming")
                def method(self):
                    return 1

    def test_num_returns_dynamic_with_non_generator_raises_error(self, shutdown_only):
        """Test that num_returns='dynamic' with non-generator raises ValueError."""
        with pytest.raises(
            ValueError, match="num_returns='dynamic' can only be used with generator"
        ):

            @ray.remote
            class TestActor:
                @ray.method(num_returns="dynamic")
                def method(self):
                    return 1

    def test_num_returns_streaming_with_generator_succeeds(self, shutdown_only):
        """Test that num_returns='streaming' with generator method succeeds."""
        # This should not raise an error at decoration time
        @ray.remote
        class TestActor:
            @ray.method(num_returns="streaming")
            def generator_method(self):
                for i in range(3):
                    yield i

        # Verify the class was created (no error was raised)
        assert TestActor is not None

    def test_num_returns_dynamic_with_generator_succeeds(self, shutdown_only):
        """Test that num_returns='dynamic' with generator method succeeds."""
        # This should not raise an error at decoration time
        @ray.remote
        class TestActor:
            @ray.method(num_returns="dynamic")
            def generator_method(self):
                for i in range(3):
                    yield i

        # Verify the class was created (no error was raised)
        assert TestActor is not None

    def test_num_returns_streaming_with_async_generator_succeeds(self, shutdown_only):
        """Test that num_returns='streaming' with async generator method succeeds."""
        # This should not raise an error at decoration time
        @ray.remote
        class TestActor:
            @ray.method(num_returns="streaming")
            async def async_generator_method(self):
                for i in range(3):
                    yield i

        # Verify the class was created (no error was raised)
        assert TestActor is not None

    def test_num_returns_positive_integer_succeeds(self, shutdown_only):
        """Test that num_returns with positive integer succeeds."""

        @ray.remote
        class TestActor:
            @ray.method(num_returns=2)
            def method(self):
                return 1, 2

        # Verify the class was created (no error was raised)
        assert TestActor is not None

    def test_num_returns_zero_succeeds(self, shutdown_only):
        """Test that num_returns=0 succeeds."""

        @ray.remote
        class TestActor:
            @ray.method(num_returns=0)
            def method(self):
                return

        # Verify the class was created (no error was raised)
        assert TestActor is not None

    def test_num_returns_none_succeeds(self, shutdown_only):
        """Test that num_returns=None succeeds."""

        @ray.remote
        class TestActor:
            @ray.method(num_returns=None)
            def method(self):
                return 1

        # Verify the class was created (no error was raised)
        assert TestActor is not None

    def test_num_returns_default_succeeds(self, shutdown_only):
        """Test that default num_returns (not specified) succeeds."""

        @ray.remote
        class TestActor:
            def method(self):
                return 1

        # Verify the class was created (no error was raised)
        assert TestActor is not None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
