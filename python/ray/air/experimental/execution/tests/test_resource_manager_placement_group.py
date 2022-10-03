from collections import Counter

import pytest

import ray
from ray.air.experimental.execution.resources.placement_group import (
    PlacementGroupResourceManager,
)
from ray.air.experimental.execution.resources.request import ResourceRequest

REQUEST_2_CPU = ResourceRequest([{"CPU": 2}])
REQUEST_1_2_CPU = ResourceRequest([{"CPU": 1}, {"CPU": 2}])
REQUEST_0_2_CPU = ResourceRequest([{"CPU": 0}, {"CPU": 2}])


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def _count_pg_states():
    counter = Counter()
    for _, pg_info in ray.util.placement_group_table().items():
        counter[pg_info["state"]] += 1
    return counter


def test_request_cancel_resources(ray_start_4_cpus):
    """Test that canceling a resource request clears the PG futures.

    - Create request
    - Assert actual PG is created
    - Cancel request
    - Assert staging future is removed
    - Assert actual PG is removed
    """
    manager = PlacementGroupResourceManager(update_interval=0)
    assert not manager.has_resources_ready(REQUEST_2_CPU)

    manager.request_resources(REQUEST_2_CPU)

    # Could be pending or created
    pg_states = _count_pg_states()
    assert pg_states["PENDING"] + pg_states["CREATED"] == 1
    assert pg_states["REMOVED"] == 0

    assert manager.get_resource_futures()

    manager.cancel_resource_request(REQUEST_2_CPU)

    assert not manager.get_resource_futures()

    pg_states = _count_pg_states()
    assert pg_states["PENDING"] + pg_states["CREATED"] == 0
    assert pg_states["REMOVED"] == 1


def test_acquire_return_resources(ray_start_4_cpus):
    """Tests that acquiring and returning resources works.

    - At the start, no resources should be ready (no PG scheduled)
    - Request resources for 2 CPUs
    - (wait until they are ready)
    - Assert that these 2 CPUs are available to be acquired
    - Acquire
    - Assert that there are no 2 CPU resources available anymore
    - Return, but don't cancel the request (keep PG)
    - Assert that the 2 CPU resources are available again
    - Cancel request
    - Assert that the 2 CPU resources are not available anymore
        - This is also tested in includes test_request_cancel_resources
    """
    manager = PlacementGroupResourceManager(update_interval=0)
    assert not manager.has_resources_ready(REQUEST_2_CPU)

    # Request PG
    manager.request_resources(REQUEST_2_CPU)

    # Wait until ready
    ray.wait(manager.get_resource_futures(), num_returns=1)

    assert manager.has_resources_ready(REQUEST_2_CPU)

    # PG exists
    pg_states = _count_pg_states()
    assert pg_states["CREATED"] == 1
    assert pg_states["REMOVED"] == 0

    # Acquire PG
    acquired = manager.acquire_resources(REQUEST_2_CPU)

    assert not manager.has_resources_ready(REQUEST_2_CPU)

    # Return
    manager.return_resources(acquired, cancel_request=False)

    assert manager.has_resources_ready(REQUEST_2_CPU)

    # PG still exists
    pg_states = _count_pg_states()
    assert pg_states["CREATED"] == 1
    assert pg_states["REMOVED"] == 0

    # Cancel request
    manager.cancel_resource_request(acquired.request)

    assert not manager.has_resources_ready(REQUEST_2_CPU)

    # PG removed
    pg_states = _count_pg_states()
    assert pg_states["CREATED"] == 0
    assert pg_states["REMOVED"] == 1


def test_request_pending(ray_start_4_cpus):
    """Test that requesting too many resources leads to pending PGs.

    - Cluster of 4 CPUs
    - Request 3 PGs a 2 CPUs
    - Acquire 2 PGs
    - Assert no resources are available anymore
    - Return both PGs
    - Assert resources are available again
    - Cancel request
    - Assert no resources are available again
    """
    manager = PlacementGroupResourceManager(update_interval=0)
    assert not manager.has_resources_ready(REQUEST_2_CPU)

    manager.request_resources(REQUEST_2_CPU)
    manager.request_resources(REQUEST_2_CPU)
    manager.request_resources(REQUEST_2_CPU)

    # Wait until some are ready
    ray.wait(manager.get_resource_futures(), num_returns=2)

    assert manager.has_resources_ready(REQUEST_2_CPU)
    assert len(manager.get_resource_futures()) == 1

    pg_states = _count_pg_states()
    assert pg_states["CREATED"] == 2
    assert pg_states["PENDING"] == 1
    assert pg_states["REMOVED"] == 0

    acq1 = manager.acquire_resources(REQUEST_2_CPU)
    acq2 = manager.acquire_resources(REQUEST_2_CPU)

    assert not manager.has_resources_ready(REQUEST_2_CPU)

    manager.return_resources(acq1, cancel_request=True)
    manager.return_resources(acq2, cancel_request=True)

    # Implementation: The PG manager returns the pending request, not the
    # scheduled one.
    assert not manager.get_resource_futures()
    assert manager.has_resources_ready(REQUEST_2_CPU)

    pg_states = _count_pg_states()
    assert pg_states["CREATED"] == 1
    assert pg_states["PENDING"] == 0
    assert pg_states["REMOVED"] == 2

    manager.cancel_resource_request(REQUEST_2_CPU)
    assert not manager.has_resources_ready(REQUEST_2_CPU)

    pg_states = _count_pg_states()
    assert pg_states["CREATED"] == 0
    assert pg_states["PENDING"] == 0
    assert pg_states["REMOVED"] == 3


def test_acquire_unavailable(ray_start_4_cpus):
    """Test that acquiring resources that are not available returns None.

    - Try to acquire
    - Assert this does not work
    - Request resources
    - Wait until ready
    - Acquire
    - Assert this did work
    """
    manager = PlacementGroupResourceManager(update_interval=0)
    assert not manager.acquire_resources(REQUEST_2_CPU)

    manager.request_resources(REQUEST_2_CPU)
    ray.wait(manager.get_resource_futures(), num_returns=1)
    assert manager.acquire_resources(REQUEST_2_CPU)


def test_bind_two_bundles(ray_start_4_cpus):
    """Test that binding two remote objects to a ready resource works.

    - Request PG with 2 bundles (1 CPU and 2 CPUs)
    - Bind two remote tasks to these bundles, execute
    - Assert that resource allocation returns the correct resources: 1 CPU and 2 CPUs
    """
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
    """Test that binding two remote objects to a ready resource works with empty head.

    - Request PG with 2 bundles (0 CPU and 2 CPUs)
    - Bind two remote tasks to these bundles, execute
    - Assert that resource allocation returns the correct resources: 0 CPU and 2 CPUs
    """
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
