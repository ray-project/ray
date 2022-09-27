import pytest
from ray.air.execution.resources.placement_group import PlacementGroupResourceManager
from ray.air.execution.resources.request import ResourceRequest

REQUEST_2_CPU = ResourceRequest([{"CPU": 2}])
REQUEST_4_CPU = ResourceRequest([{"CPU": 4}])


def test_acquire_return_resources():
    manager = PlacementGroupResourceManager(total_resources={"CPU": 4})
    assert manager.has_resources_ready(REQUEST_4_CPU)

    ready_2 = manager.acquire_resources(REQUEST_2_CPU)

    assert manager.has_resources_ready(REQUEST_2_CPU)
    assert not manager.has_resources_ready(REQUEST_4_CPU)

    manager.return_resources(ready_2)

    assert manager.has_resources_ready(REQUEST_4_CPU)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
