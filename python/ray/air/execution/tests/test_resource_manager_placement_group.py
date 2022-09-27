import pytest

import ray
from ray.air.execution.resources.placement_group import PlacementGroupResourceManager
from ray.air.execution.resources.request import ResourceRequest

REQUEST_2_CPU = ResourceRequest([{"CPU": 2}])
REQUEST_4_CPU = ResourceRequest([{"CPU": 4}])


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_request_cancel_resources(ray_start_4_cpus):
    manager = PlacementGroupResourceManager(update_interval=0)
    assert not manager.has_resources_ready(REQUEST_2_CPU)

    manager.request_resources(REQUEST_2_CPU)

    assert manager.get_resource_futures()

    manager.cancel_resource_request(REQUEST_2_CPU)

    assert not manager.get_resource_futures()


def test_acquire_return_resources(ray_start_4_cpus):
    manager = PlacementGroupResourceManager(update_interval=0)
    assert not manager.has_resources_ready(REQUEST_2_CPU)

    # Request PG
    manager.request_resources(REQUEST_2_CPU)

    # Wait until ready
    ray.wait(manager.get_resource_futures(), num_returns=1)

    assert manager.has_resources_ready(REQUEST_2_CPU)

    # Acquire PG
    acquired = manager.acquire_resources(REQUEST_2_CPU)

    assert not manager.has_resources_ready(REQUEST_2_CPU)

    # Return
    manager.return_resources(acquired, cancel_request=False)

    assert manager.has_resources_ready(REQUEST_2_CPU)

    # Cancel request
    manager.cancel_resource_request(acquired.request)

    assert not manager.has_resources_ready(REQUEST_2_CPU)


def test_request_pending(ray_start_4_cpus):
    manager = PlacementGroupResourceManager(update_interval=0)
    assert not manager.has_resources_ready(REQUEST_2_CPU)

    manager.request_resources(REQUEST_2_CPU)
    manager.request_resources(REQUEST_2_CPU)
    manager.request_resources(REQUEST_2_CPU)

    # Wait until some are ready
    ray.wait(manager.get_resource_futures(), num_returns=2)

    assert manager.has_resources_ready(REQUEST_2_CPU)
    assert len(manager.get_resource_futures()) == 1

    acq1 = manager.acquire_resources(REQUEST_2_CPU)
    acq2 = manager.acquire_resources(REQUEST_2_CPU)

    assert not manager.has_resources_ready(REQUEST_2_CPU)

    manager.return_resources(acq1, cancel_request=True)
    manager.return_resources(acq2, cancel_request=True)

    # Implementation: The PG manager returns the pending request, not the
    # scheduled one.
    assert not manager.get_resource_futures()
    assert manager.has_resources_ready(REQUEST_2_CPU)

    manager.cancel_resource_request(REQUEST_2_CPU)
    assert not manager.has_resources_ready(REQUEST_2_CPU)


def test_acquire_unavailable(ray_start_4_cpus):
    manager = PlacementGroupResourceManager(update_interval=0)
    assert not manager.acquire_resources(REQUEST_2_CPU)

    manager.request_resources(REQUEST_2_CPU)
    ray.wait(manager.get_resource_futures(), num_returns=1)
    assert manager.acquire_resources(REQUEST_2_CPU)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
