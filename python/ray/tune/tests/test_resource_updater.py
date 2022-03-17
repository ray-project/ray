import ray
from ray.tests.conftest import *  # noqa
from ray.tune.utils.resource_updater import ResourceUpdater


def test_resource_updater(ray_start_cluster):
    cluster = ray_start_cluster

    resource_updater = ResourceUpdater(refresh_period=100)
    # Before intialization, all resources are 0.
    assert resource_updater.get_num_cpus() == 0
    assert resource_updater.get_num_gpus() == 0

    cluster.add_node(num_cpus=1, num_gpus=2)
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    # Resource updater will update resource immediately
    # after ray is initialized for the first time.
    assert resource_updater.get_num_cpus() == 1
    assert resource_updater.get_num_gpus() == 2

    # It will not update the resource before "refresh_period".
    cluster.add_node(num_cpus=1, num_gpus=1)
    cluster.wait_for_nodes()
    assert resource_updater.get_num_cpus() == 1
    assert resource_updater.get_num_gpus() == 2

    resource_updater = ResourceUpdater(refresh_period=0)
    assert resource_updater.get_num_cpus() == 2
    assert resource_updater.get_num_gpus() == 3

    cluster.add_node(num_cpus=1, num_gpus=1)
    cluster.wait_for_nodes()
    assert resource_updater.get_num_cpus() == 3
    assert resource_updater.get_num_gpus() == 4


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
