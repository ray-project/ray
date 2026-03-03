import time
from typing import Any, Type

import pytest

import ray
from ray.air.execution._internal import Barrier
from ray.air.execution._internal.event_manager import RayEventManager
from ray.exceptions import RayTaskError


@pytest.fixture(scope="module")
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    ray.shutdown()


@ray.remote
def succeeding(ret: Any = None) -> Any:
    return ret


@ray.remote
def failing(exc: Type[Exception], *args) -> None:
    raise exc(*args)


@ray.remote
def sleeping(seconds: int, result: Any) -> Any:
    time.sleep(seconds)
    return result


def test_track_future_success(ray_start_4_cpus):
    """Schedule a future that return successfully.

    Check that the on_result callback was triggered.
    """
    event_manager = RayEventManager()

    seen = set()

    def on_result(result: Any):
        seen.add(result)

    event_manager.track_future(succeeding.remote("a"), on_result=on_result)

    event_manager.wait()
    assert "a" in seen

    assert not event_manager._tracked_futures


def test_track_future_success_no_callback(ray_start_4_cpus):
    """Schedule a future that return successfully.

    Check that passing no callback still succeeds.
    """
    event_manager = RayEventManager()

    event_manager.track_future(succeeding.remote("a"))

    event_manager.wait()

    assert not event_manager._tracked_futures


def test_track_future_error(ray_start_4_cpus):
    """Schedule a future that fails.

    Check that the on_error callback was triggered.
    """
    event_manager = RayEventManager()

    seen = set()

    class CustomError(RuntimeError):
        pass

    def on_error(exception: Exception):
        seen.add(exception)

    event_manager.track_future(failing.remote(CustomError), on_error=on_error)

    event_manager.wait()
    assert isinstance(seen.pop(), CustomError)

    assert not event_manager._tracked_futures


def test_track_future_error_no_callback(ray_start_4_cpus):
    """Schedule a future that fails.

    Check that passing no callback raises the original error.
    """
    event_manager = RayEventManager()

    event_manager.track_future(failing.remote(RuntimeError))

    with pytest.raises(RuntimeError):
        event_manager.wait()

    assert not event_manager._tracked_futures


@pytest.mark.parametrize("results_per_wait", [None, 1, 5, 10, 100])
def test_many_futures(ray_start_4_cpus, results_per_wait):
    """Schedule 500 succeeding and failing futures.

    Check that the callbacks get triggered correctly, independent of the number
    of results we await per call to RayEventManager.wait().
    """
    num_futures = 500

    event_manager = RayEventManager()

    seen_results = set()
    seen_errors = set()

    def on_result(result: Any):
        seen_results.add(result)

    def on_error(exception: RayTaskError):
        seen_errors.add(exception.cause.args[0])

    for i in range(num_futures):
        event_manager.track_futures(
            [
                succeeding.remote("a" + str(i)),
                failing.remote(RuntimeError, "b" + str(i)),
            ],
            on_result=on_result,
            on_error=on_error,
        )

    while event_manager.num_futures > 0:
        event_manager.wait(num_results=results_per_wait)

    for i in range(num_futures):
        assert "a" + str(i) in seen_results
        assert "b" + str(i) in seen_errors


def test_timeout(ray_start_4_cpus):
    """Test the timeout parameter.

    Start 4 tasks: Two succeed immediately, two after 1 second.

    After waiting for 0.5 seconds, the first two tasks should have returned.
    After waiting for up to 5 seconds, the other two tasks should have returned.
    But because the tasks take only 0.5 seconds to run, we should have waited
    way less than 5 seconds.
    """
    event_manager = RayEventManager()

    seen = set()

    def on_result(result: Any):
        seen.add(result)

    event_manager.track_futures(
        [
            succeeding.remote("a"),
            succeeding.remote("b"),
            sleeping.remote(1, "c"),
            sleeping.remote(1, "d"),
        ],
        on_result=on_result,
    )

    start = time.monotonic()
    event_manager.wait(num_results=None, timeout=0.5)
    assert "a" in seen
    assert "b" in seen
    assert "c" not in seen
    assert "d" not in seen

    event_manager.wait(num_results=None, timeout=5)
    taken = time.monotonic() - start

    assert "c" in seen
    assert "d" in seen

    # Should have returned much earlier than after 5 seconds
    assert taken < 3

    assert not event_manager._tracked_futures


def test_task_barrier(ray_start_4_cpus):
    event_manager = RayEventManager()

    seen = set()

    def on_completion(barrier: Barrier):
        seen.update(barrier.get_results())

    barrier = Barrier(max_results=4, on_completion=on_completion)

    event_manager.track_futures(
        [
            succeeding.remote("a"),
            succeeding.remote("b"),
            succeeding.remote("c"),
            succeeding.remote("d"),
            sleeping.remote(2, "e"),
        ],
        on_result=barrier.arrive,
    )

    event_manager.wait(num_results=4)

    assert "a" in seen
    assert "b" in seen
    assert "c" in seen
    assert "d" in seen
    assert "e" not in seen


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
