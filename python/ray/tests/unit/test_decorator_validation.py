"""Tests for @ray.remote and @ray.method decorator validation."""

import re

import pytest

import ray
from ray._common.ray_option_utils import DYNAMIC_NUM_RETURNS_WARNING
from ray.util.annotations import RayDeprecationWarning

_DYNAMIC_WARNING = re.escape(DYNAMIC_NUM_RETURNS_WARNING)


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
        with pytest.warns(RayDeprecationWarning, match=_DYNAMIC_WARNING):
            with pytest.raises(
                ValueError,
                match="num_returns='dynamic' can only be used with generator",
            ):

                @ray.remote(num_returns="dynamic")
                def f():
                    return 1

    def test_num_returns_dynamic_with_generator_succeeds(self):
        """Test that num_returns='dynamic' with generator function succeeds."""
        with pytest.warns(RayDeprecationWarning, match=_DYNAMIC_WARNING):

            @ray.remote(num_returns="dynamic")
            def generator_func():
                for i in range(3):
                    yield i

    def test_num_returns_dynamic_warns_via_options(self):
        """Test that f.options(num_returns='dynamic') warns but still works."""

        @ray.remote
        def generator_func():
            for i in range(3):
                yield i

        with pytest.warns(RayDeprecationWarning, match=_DYNAMIC_WARNING):
            generator_func.options(num_returns="dynamic")

    def test_num_returns_dynamic_does_not_rewarn_on_unrelated_options(self):
        """Unrelated .options() overrides should not re-warn for a dynamic default."""
        import warnings

        with pytest.warns(RayDeprecationWarning, match=_DYNAMIC_WARNING):

            @ray.remote(num_returns="dynamic")
            def generator_func():
                for i in range(3):
                    yield i

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", RayDeprecationWarning)
            generator_func.options(num_cpus=2)
        assert not any(
            issubclass(w.category, RayDeprecationWarning) for w in caught
        ), "unrelated .options() should not re-emit the dynamic deprecation warning"

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

    def test_num_returns_streaming_via_options_with_async_generator_succeeds(self):
        """Async generators must be recognized as generators in .options()."""

        @ray.remote
        async def async_generator_func():
            for i in range(3):
                yield i

        async_generator_func.options(num_returns="streaming")

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

    def test_num_returns_dynamic_with_non_generator_raises_error(self):
        """Test that num_returns='dynamic' with non-generator raises ValueError."""
        with pytest.warns(RayDeprecationWarning, match=_DYNAMIC_WARNING):
            with pytest.raises(
                ValueError,
                match="num_returns='dynamic' can only be used with generator",
            ):

                @ray.remote
                class TestActor:
                    @ray.method(num_returns="dynamic")
                    def method(self):
                        return 1

    def test_num_returns_dynamic_with_generator_succeeds(self):
        """Test that num_returns='dynamic' with generator method succeeds."""
        with pytest.warns(RayDeprecationWarning, match=_DYNAMIC_WARNING):

            @ray.remote
            class TestActor:
                @ray.method(num_returns="dynamic")
                def generator_method(self):
                    for i in range(3):
                        yield i

    def test_num_returns_dynamic_warns_via_options(self):
        """Test that actor.method.options(num_returns='dynamic') warns but still works."""
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
        with pytest.warns(RayDeprecationWarning, match=_DYNAMIC_WARNING):
            method.options(num_returns="dynamic")

    def test_num_returns_dynamic_does_not_rewarn_on_remote(self):
        """Defaults from @ray.method are not re-validated on every .remote()."""
        import warnings

        from ray.actor import ActorMethod

        method = ActorMethod(
            actor=None,
            method_name="generator_method",
            num_returns="dynamic",
            max_task_retries=0,
            retry_exceptions=False,
            is_generator=True,
            generator_backpressure_num_objects=-1,
            num_objects_per_yield=1,
            enable_task_events=True,
        )
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", RayDeprecationWarning)
            with pytest.raises(Exception):
                # Bypass wrap_auto_init so this unit test does not call ray.init().
                ActorMethod._remote.__wrapped__(method)
        assert not any(
            issubclass(w.category, RayDeprecationWarning) for w in caught
        ), "method defaults should not re-emit the dynamic deprecation warning"

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
