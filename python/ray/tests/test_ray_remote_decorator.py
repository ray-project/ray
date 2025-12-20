import sys

import pytest

import ray


def test_num_returns_lt_0_with_non_generator_task_raises_value_error():
    with pytest.raises(ValueError):

        @ray.remote(num_returns=-1)
        def f():
            pass


def test_num_returns_lt_0_with_generator_task_raises_value_error():
    with pytest.raises(ValueError):

        @ray.remote(num_returns=-1)
        def f():
            yield 1


def test_num_returns_streaming_with_non_generator_task_raises_value_error():
    with pytest.raises(ValueError):

        @ray.remote(num_returns="streaming")
        def f():
            pass


def test_num_returns_streaming_with_generator_task_returns_remote_function():
    @ray.remote(num_returns="streaming")
    def f():
        yield 1


def test_num_returns_dynamic_with_non_generator_task_raises_value_error():
    with pytest.raises(ValueError):

        @ray.remote(num_returns="dynamic")
        def f():
            pass


def test_num_returns_dynamic_with_generator_task_returns_remote_function():
    @ray.remote(num_returns="dynamic")
    def f():
        yield 1


def test_num_returns_gte_0_with_generator_task_returns_remote_function():
    # TODO(irabbani): Allowing num_returns=0 for backwards compatability. I don't
    # think it makes any sense so we should probably remove it.
    @ray.remote(num_returns=0)
    def f():
        yield 1

    @ray.remote(num_returns=1)
    def g():
        yield 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
