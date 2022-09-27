import pytest

import ray
from ray.air.execution.resources.placement_group import PlacementGroupResourceManager
from ray.air.execution.resources.request import ResourceRequest

REQUEST_2_CPU = ResourceRequest([{"CPU": 2}])
REQUEST_1_2_CPU = ResourceRequest([{"CPU": 1}, {"CPU": 2}])
REQUEST_0_2_CPU = ResourceRequest([{"CPU": 0}, {"CPU": 2}])


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


def test_bind_two_bundles(ray_start_4_cpus):
    manager = PlacementGroupResourceManager(update_interval=0)
    manager.request_resources(REQUEST_1_2_CPU)
    ray.wait(manager.get_resource_futures(), num_returns=1)

    assert manager.has_resources_ready(REQUEST_1_2_CPU)

    @ray.remote
    def get_assigned_resources():
        return ray.get_runtime_context().get_assigned_resources()

    acq = manager.acquire_resources(REQUEST_1_2_CPU)
    [av1] = acq.annotate_remote_objects([get_assigned_resources])

    res1 = ray.get(av1.remote())

    assert sum(v for k, v in res1.items() if k.startswith("CPU_group_0")) == 1

    [av1, av2] = acq.annotate_remote_objects(
        [get_assigned_resources, get_assigned_resources]
    )

    res1, res2 = ray.get([av1.remote(), av2.remote()])
    assert sum(v for k, v in res1.items() if k.startswith("CPU_group_0")) == 1
    assert sum(v for k, v in res2.items() if k.startswith("CPU_group_1")) == 2


def test_bind_empty_head_bundle(ray_start_4_cpus):
    manager = PlacementGroupResourceManager(update_interval=0)
    assert REQUEST_0_2_CPU.head_bundle_is_empty
    manager.request_resources(REQUEST_0_2_CPU)
    ray.wait(manager.get_resource_futures(), num_returns=1)

    assert manager.has_resources_ready(REQUEST_0_2_CPU)

    @ray.remote
    def get_assigned_resources():
        return ray.get_runtime_context().get_assigned_resources()

    acq = manager.acquire_resources(REQUEST_0_2_CPU)
    [av1] = acq.annotate_remote_objects([get_assigned_resources])

    res1 = ray.get(av1.remote())

    assert sum(v for k, v in res1.items() if k.startswith("CPU_group_0")) == 0

    [av1, av2] = acq.annotate_remote_objects(
        [get_assigned_resources, get_assigned_resources]
    )

    res1, res2 = ray.get([av1.remote(), av2.remote()])
    assert sum(v for k, v in res1.items() if k.startswith("CPU_group_0")) == 0
    assert sum(v for k, v in res2.items() if k.startswith("CPU_group_0")) == 2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
