import ray
from ray.tests.conftest import *  # noqa
from ray.tune.utils.resource_updater import ResourceUpdater


def test_resource_updater(shutdown_only):
    resource_updater = ResourceUpdater(refresh_period=100)
    # Before intialization, all resources are 0.
    assert resource_updater.get_num_cpus() == 0
    assert resource_updater.get_num_gpus() == 0
    ray.shutdown()

    # Resource updater will update resource immediately
    # after ray is initialized for the first time.
    ray.init(num_cpus=1, num_gpus=2)
    assert resource_updater.get_num_cpus() == 1
    assert resource_updater.get_num_gpus() == 2
    ray.shutdown()

    # It will not update the resource before "refresh_period".
    ray.init(num_cpus=2, num_gpus=3)
    assert resource_updater.get_num_cpus() == 1
    assert resource_updater.get_num_gpus() == 2
    ray.shutdown()

    ray.init(num_cpus=1, num_gpus=2)
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
