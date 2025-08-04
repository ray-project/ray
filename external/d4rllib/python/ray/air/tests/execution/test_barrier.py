from typing import Optional, Type

import pytest

from ray.air.execution._internal.barrier import Barrier


def _raise(exception_type: Type[Exception] = RuntimeError, msg: Optional[str] = None):
    def _raise_exception(*args, **kwargs):
        raise exception_type(msg)

    return _raise_exception


def test_barrier_max_results():
    """Test the `max_results` attribute.

    - Set max_results=10
    - Assert that the barrier completion callback is not invoked with num_results<10
    - Assert that callback is invoked with num_results=10
    - Assert that callback is not invoked again when more events arrive
    - Assert that more events can arrive without triggering the callback after resetting
    """
    barrier = Barrier(max_results=10, on_completion=_raise(AssertionError))

    for i in range(9):
        barrier.arrive(i)

    assert not barrier.completed

    # Will trigger the on_completion callback
    with pytest.raises(AssertionError):
        barrier.arrive(10)

    assert barrier.completed

    assert barrier.num_results == 10

    # Further events will not trigger callback again
    barrier.arrive(11)

    barrier.reset()

    assert not barrier.completed

    # After flushing more events can arrive
    barrier.arrive(12)
    assert barrier.num_results == 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
