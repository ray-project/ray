import pytest

import ray
from ray.air.execution import FixedResourceManager
from ray.air.execution._internal.event_manager import RayEventManager


@pytest.fixture(scope="module")
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    ray.shutdown()


@pytest.fixture
def event_manager():
    manager = RayEventManager(resource_manager=FixedResourceManager())
    yield manager


def test_resolve(ray_start_4_cpus, event_manager):
    """Test that the `on_result` callback is invoked when a task completes.

    - Instantiate global data object
    - Schedule task that returns a value
    - The callback writes the returned value to the global data object
    """
    seen = {"data": 0}

    def result_callback(result):
        seen["data"] = result

    @ray.remote
    def foo():
        return 4

    event_manager.schedule_task(foo).on_result(result_callback)
    event_manager.wait()

    assert seen["data"] == 4


def test_error_noop(ray_start_4_cpus, event_manager):
    """When no `on_error` callback is specified, errors should be ignored."""

    @ray.remote
    def foo():
        raise RuntimeError

    event_manager.schedule_task(foo)
    event_manager.wait()


def test_error_custom(ray_start_4_cpus, event_manager):
    """When an `on_error` callback is specified, it is invoked."""

    def error_callback(exception):
        raise exception

    @ray.remote
    def foo():
        raise RuntimeError

    event_manager.schedule_task(foo).on_error(error_callback)

    with pytest.raises(RuntimeError):
        event_manager.wait()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
