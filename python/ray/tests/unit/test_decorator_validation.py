"""Tests for @ray.remote and @ray.method decorator validation."""

import re

import pytest

import ray
from ray._common.ray_option_utils import DYNAMIC_NUM_RETURNS_ERROR

_DYNAMIC_ERROR = re.escape(DYNAMIC_NUM_RETURNS_ERROR)


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

    def test_num_returns_dynamic_rejected(self):
        """Test that the deprecated num_returns='dynamic' is rejected."""
        with pytest.raises(ValueError, match=_DYNAMIC_ERROR):

            @ray.remote(num_returns="dynamic")
            def generator_func():
                for i in range(3):
                    yield i

    def test_num_returns_dynamic_rejected_via_options(self):
        """Test that f.options(num_returns='dynamic') is rejected."""

        @ray.remote
        def generator_func():
            for i in range(3):
                yield i

        with pytest.raises(ValueError, match=_DYNAMIC_ERROR):
            generator_func.options(num_returns="dynamic")

    def test_num_returns_streaming_with_generator_succeeds(self):
        """Test that num_returns='streaming' with generator function succeeds."""

        @ray.remote(num_returns="streaming")
        def generator_func():
            for i in range(3):
                yield i

    def test_num_returns_streaming_with_async_generator_succeeds(self):
        """Test that num_returns='streaming' with async generator function succeeds."""

        @ray.remote(num_returns="streaming")
        async def async_generator_func():
            for i in range(3):
                yield i

    def test_num_returns_positive_integer_succeeds(self):
        """Test that num_returns with positive integer succeeds."""

        @ray.remote(num_returns=2)
        def f():
            return 1, 2

    def test_num_returns_zero_succeeds(self):
        """Test that num_returns=0 succeeds."""

        @ray.remote(num_returns=0)
        def f():
            return

    def test_num_returns_none_succeeds(self):
        """Test that num_returns=None succeeds."""

        @ray.remote(num_returns=None)
        def f():
            return 1

    def test_num_returns_default_succeeds(self):
        """Test that default num_returns (not specified) succeeds."""

        @ray.remote
        def f():
            return 1


class TestMethodNumReturns:
    """Test num_returns validation for @ray.method decorator."""

    def test_num_returns_negative_raises_error(self):
        """Test that num_returns < 0 raises ValueError at decoration time."""
        with pytest.raises(ValueError, match="num_returns must be >= 0"):

            @ray.remote
            class TestActor:
                @ray.method(num_returns=-1)
                def method(self):
                    return 1

    def test_num_returns_streaming_with_non_generator_raises_error(self):
        """Test that num_returns='streaming' with non-generator raises ValueError."""
        with pytest.raises(
            ValueError, match="num_returns='streaming' can only be used with generator"
        ):

            @ray.remote
            class TestActor:
                @ray.method(num_returns="streaming")
                def method(self):
                    return 1

    def test_num_returns_dynamic_rejected(self):
        """Test that the deprecated num_returns='dynamic' is rejected."""
        with pytest.raises(ValueError, match=_DYNAMIC_ERROR):

            @ray.remote
            class TestActor:
                @ray.method(num_returns="dynamic")
                def generator_method(self):
                    for i in range(3):
                        yield i

    def test_num_returns_dynamic_rejected_via_options(self):
        """Test that actor.method.options(num_returns='dynamic') is rejected."""
        from ray.actor import ActorMethod

        method = ActorMethod(
            actor=None,
            method_name="generator_method",
            num_returns="streaming",
            max_task_retries=0,
            retry_exceptions=False,
            is_generator=True,
            generator_backpressure_num_objects=-1,
            num_objects_per_yield=1,
            enable_task_events=True,
        )
        with pytest.raises(ValueError, match=_DYNAMIC_ERROR):
            method.options(num_returns="dynamic")

    def test_num_returns_dynamic_rejected_via_remote(self):
        """Test that actor.method._remote(num_returns='dynamic') is rejected."""
        from ray.actor import ActorMethod

        method = ActorMethod(
            actor=None,
            method_name="generator_method",
            num_returns="streaming",
            max_task_retries=0,
            retry_exceptions=False,
            is_generator=True,
            generator_backpressure_num_objects=-1,
            num_objects_per_yield=1,
            enable_task_events=True,
        )
        # Bypass wrap_auto_init so this unit test does not call ray.init().
        with pytest.raises(ValueError, match=_DYNAMIC_ERROR):
            ActorMethod._remote.__wrapped__(method, num_returns="dynamic")

    def test_num_returns_dynamic_rejected_via_bind(self):
        """Test that actor.method._bind(num_returns='dynamic') is rejected."""
        from ray.actor import ActorMethod

        method = ActorMethod(
            actor=None,
            method_name="generator_method",
            num_returns="streaming",
            max_task_retries=0,
            retry_exceptions=False,
            is_generator=True,
            generator_backpressure_num_objects=-1,
            num_objects_per_yield=1,
            enable_task_events=True,
        )
        with pytest.raises(ValueError, match=_DYNAMIC_ERROR):
            ActorMethod._bind.__wrapped__(method, num_returns="dynamic")

    def test_num_returns_streaming_with_generator_succeeds(self):
        """Test that num_returns='streaming' with generator method succeeds."""

        @ray.remote
        class TestActor:
            @ray.method(num_returns="streaming")
            def generator_method(self):
                for i in range(3):
                    yield i

    def test_num_returns_streaming_with_async_generator_succeeds(self):
        """Test that num_returns='streaming' with async generator method succeeds."""

        @ray.remote
        class TestActor:
            @ray.method(num_returns="streaming")
            async def async_generator_method(self):
                for i in range(3):
                    yield i

    def test_num_returns_positive_integer_succeeds(self):
        """Test that num_returns with positive integer succeeds."""

        @ray.remote
        class TestActor:
            @ray.method(num_returns=2)
            def method(self):
                return 1, 2

    def test_num_returns_zero_succeeds(self):
        """Test that num_returns=0 succeeds."""

        @ray.remote
        class TestActor:
            @ray.method(num_returns=0)
            def method(self):
                return

    def test_num_returns_none_succeeds(self):
        """Test that num_returns=None succeeds."""

        @ray.remote
        class TestActor:
            @ray.method(num_returns=None)
            def method(self):
                return 1

    def test_num_returns_default_succeeds(self):
        """Test that default num_returns (not specified) succeeds."""

        @ray.remote
        class TestActor:
            def method(self):
                return 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
