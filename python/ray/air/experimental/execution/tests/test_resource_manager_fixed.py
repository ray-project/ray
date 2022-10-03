import pytest
from ray.air.experimental.execution.resources.fixed import FixedResourceManager
from ray.air.experimental.execution.resources.request import ResourceRequest

REQUEST_2_CPU = ResourceRequest([{"CPU": 2}])
REQUEST_4_CPU = ResourceRequest([{"CPU": 4}])


def test_acquire_return_resources():
    manager = FixedResourceManager(total_resources={"CPU": 4})
    assert manager.has_resources_ready(REQUEST_4_CPU)

    ready_2 = manager.acquire_resources(REQUEST_2_CPU)

    assert manager.has_resources_ready(REQUEST_2_CPU)
    assert not manager.has_resources_ready(REQUEST_4_CPU)

    manager.return_resources(ready_2)

    assert manager.has_resources_ready(REQUEST_4_CPU)


def test_numerical_error():
    """Make sure we don't run into numerical errors when using fractional resources.

    Legacy test: test_trial_runner::TrialRunnerTest::testResourceNumericalError
    """
    manager = FixedResourceManager(
        total_resources={"CPU": 0.99, "GPU": 0.99, "a": 0.99}
    )
    for i in range(3):
        assert manager.acquire_resources(
            ResourceRequest([{"CPU": 0.33, "GPU": 0.33, "a": 0.33}])
        ), manager._available_resources

    assert manager._available_resources["CPU"] == 0
    assert manager._available_resources["GPU"] == 0
    assert manager._available_resources["a"] == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
