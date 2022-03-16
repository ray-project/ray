import ray
from ray.tune.utils.resource_updater import ResourceUpdater


def test_resource_updater(shutdown_only):
    resource_updater = ResourceUpdater(refresh_period=10)
    assert resource_updater.get_num_cpus() == 0
    assert resource_updater.get_num_gpus() == 0
    ray.init(num_cpus=1, num_gpus=2)
    assert resource_updater.get_num_cpus() == 0
    assert resource_updater.get_num_gpus() == 0

    resource_updater = ResourceUpdater(refresh_period=0)
    assert resource_updater.get_num_cpus() == 1
    assert resource_updater.get_num_gpus() == 2
    ray.shutdown()

    ray.init(num_cpus=2, num_gpus=3)
    assert resource_updater.get_num_cpus() == 2
    assert resource_updater.get_num_gpus() == 3


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
