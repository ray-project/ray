import pytest
from typing import Optional, Type

from ray.air.execution._internal.barrier import Barrier


def _raise(exception_type: Type[Exception] = RuntimeError, msg: Optional[str] = None):
    def _raise_exception(*args, **kwargs):
        raise exception_type(msg)

    return _raise_exception


def test_barrier_no_args():
    """Test that a barrier with no arguments works as expected:

    - Completion callback is never invoked
    - Num results is as expected
    - Num errors is as expected
    - flush resets the data
    """
    barrier = Barrier()
    barrier.on_completion(_raise(RuntimeError))

    assert barrier.num_results == 0
    assert barrier.num_errors == 0

    for i in range(10):
        barrier.arrive(i)

    assert barrier.num_results == 10
    assert barrier.num_errors == 0

    for i in range(5):
        barrier.error(i)

    assert barrier.num_results == 10
    assert barrier.num_errors == 5

    barrier.flush()

    assert barrier.num_results == 0
    assert barrier.num_errors == 0


def test_barrier_max_results():
    """Test the `max_results` attribute.

    - Set max_results=10
    - Assert that the barrier completion callback is not invoked with num_results<10
    - Assert that callback is invoked with num_results=10
    - Assert that callback is not invoked again when more events arrive
    - Assert that more events can arrive without triggering the callback after flushing
    """
    barrier = Barrier(max_results=10)
    barrier.on_completion(_raise(AssertionError))

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

    barrier.flush()

    assert not barrier.completed

    # After flushing more events can arrive
    barrier.arrive(12)
    assert barrier.num_results == 1


def test_barrier_on_first_error():
    """Test the `on_first_error` attribute.

    - Set on_first_error and max_results=3
    - Assert that Barrier.error() triggers the callback
    - Assert that barrier is set to completed
    - Assert that subsequent errors do not trigger the callback again
    - Assert that subsequent arrivals do not trigger the callback again
    """
    barrier = Barrier(max_results=3)
    barrier.on_first_error(_raise(AssertionError))

    barrier.arrive(1)

    assert not barrier.has_error
    with pytest.raises(AssertionError):
        barrier.error(2)
    assert barrier.has_error

    assert barrier.num_results == 1
    assert barrier.num_errors == 1

    # Subsequent errors do not trigger the callback again
    barrier.error(3)
    barrier.arrive(4)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
