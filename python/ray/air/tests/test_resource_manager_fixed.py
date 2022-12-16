import pytest

import ray
from ray.air.execution.resources.fixed import FixedResourceManager
from ray.air.execution.resources.request import ResourceRequest
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

REQUEST_2_CPU = ResourceRequest([{"CPU": 2}])
REQUEST_4_CPU = ResourceRequest([{"CPU": 4}])
REQUEST_1_2_CPU = ResourceRequest([{"CPU": 1}, {"CPU": 2}])
REQUEST_0_2_CPU = ResourceRequest([{"CPU": 0}, {"CPU": 2}])


@pytest.fixture(scope="module")
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_acquire_return_resources(ray_start_4_cpus):
    manager = FixedResourceManager(total_resources={"CPU": 4})

    assert not manager.has_resources_ready(REQUEST_2_CPU)
    assert not manager.has_resources_ready(REQUEST_4_CPU)

    manager.request_resources(REQUEST_2_CPU)
    manager.request_resources(REQUEST_4_CPU)

    assert manager.has_resources_ready(REQUEST_4_CPU)

    ready_2 = manager.acquire_resources(REQUEST_2_CPU)

    assert manager.has_resources_ready(REQUEST_2_CPU)
    assert not manager.has_resources_ready(REQUEST_4_CPU)

    manager.free_resources(ready_2)

    assert manager.has_resources_ready(REQUEST_4_CPU)


def test_numerical_error(ray_start_4_cpus):
    """Make sure we don't run into numerical errors when using fractional resources.

    Legacy test: test_trial_runner::TrialRunnerTest::testResourceNumericalError
    """
    manager = FixedResourceManager(
        total_resources={"CPU": 0.99, "GPU": 0.99, "a": 0.99}
    )
    resource_request = ResourceRequest([{"CPU": 0.33, "GPU": 0.33, "a": 0.33}])

    for i in range(3):
        manager.request_resources(resource_request)
        assert manager.acquire_resources(
            resource_request=resource_request
        ), manager._available_resources

    assert manager._available_resources["CPU"] == 0
    assert manager._available_resources["GPU"] == 0
    assert manager._available_resources["a"] == 0


def test_bind_two_bundles(ray_start_4_cpus):
    """Test that binding two remote objects to a ready resource works.

    - Request resources with 2 bundles (1 CPU and 2 CPUs)
    - Bind two remote tasks to these bundles, execute
    - Assert that resource allocation returns the correct resources: 1 CPU and 2 CPUs
    """
    manager = FixedResourceManager()
    manager.request_resources(REQUEST_1_2_CPU)

    assert manager.has_resources_ready(REQUEST_1_2_CPU)

    @ray.remote
    def get_assigned_resources():
        return ray.get_runtime_context().get_assigned_resources()

    acq = manager.acquire_resources(REQUEST_1_2_CPU)
    [av1] = acq.annotate_remote_objects([get_assigned_resources])

    res1 = ray.get(av1.remote())

    assert sum(v for k, v in res1.items() if k.startswith("CPU")) == 1

    [av1, av2] = acq.annotate_remote_objects(
        [get_assigned_resources, get_assigned_resources]
    )

    res1, res2 = ray.get([av1.remote(), av2.remote()])
    assert sum(v for k, v in res1.items() if k.startswith("CPU")) == 1
    assert sum(v for k, v in res2.items() if k.startswith("CPU")) == 2


def test_bind_empty_head_bundle(ray_start_4_cpus):
    """Test that binding two remote objects to a ready resource works with empty head.

    - Request resources with 2 bundles (0 CPU and 2 CPUs)
    - Bind two remote tasks to these bundles, execute
    - Assert that resource allocation returns the correct resources: 0 CPU and 2 CPUs
    """
    manager = FixedResourceManager()
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

    assert sum(v for k, v in res1.items() if k.startswith("CPU")) == 0

    [av1, av2] = acq.annotate_remote_objects(
        [get_assigned_resources, get_assigned_resources]
    )

    res1, res2 = ray.get([av1.remote(), av2.remote()])
    assert sum(v for k, v in res1.items() if k.startswith("CPU")) == 0
    assert sum(v for k, v in res2.items() if k.startswith("CPU")) == 2


@pytest.mark.parametrize("strategy", ["STRICT_PACK", "PACK", "SPREAD", "STRICT_SPREAD"])
def test_strategy(ray_start_4_cpus, strategy):
    """The fixed resoure manager does not support STRICT placement strategies."""
    manager = FixedResourceManager()

    req = ResourceRequest([{"CPU": 2}], strategy=strategy)

    if strategy.startswith("STRICT_"):
        with pytest.raises(RuntimeError):
            manager.request_resources(req)
    else:
        manager.request_resources(req)


@pytest.mark.parametrize("strategy", ["STRICT_PACK", "PACK", "SPREAD", "STRICT_SPREAD"])
def test_strategy_nested(ray_start_4_cpus, strategy):
    """The fixed resoure manager does not support STRICT_SPREAD within a PG."""

    @ray.remote
    def nested_test():
        manager = FixedResourceManager()

        req = ResourceRequest([{"CPU": 2}], strategy=strategy)

        if strategy == "STRICT_SPREAD":
            with pytest.raises(RuntimeError):
                manager.request_resources(req)
        else:
            manager.request_resources(req)

    pg = ray.util.placement_group([{"CPU": 2}])
    ray.wait([pg.ready()])

    try:
        ray.get(
            nested_test.options(
                scheduling_strategy=PlacementGroupSchedulingStrategy(
                    placement_group=pg, placement_group_capture_child_tasks=True
                )
            ).remote()
        )
    finally:
        ray.util.remove_placement_group(pg)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
