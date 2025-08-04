from unittest import mock

import ray
from ray.tests.conftest import *  # noqa
from ray.tune.utils.resource_updater import _Resources, _ResourceUpdater


def test_resources_numerical_error():
    resource = _Resources(cpu=0.99, gpu=0.99, custom_resources={"a": 0.99})
    small_resource = _Resources(cpu=0.33, gpu=0.33, custom_resources={"a": 0.33})
    for i in range(3):
        resource = _Resources.subtract(resource, small_resource)
    assert resource.is_nonnegative()


def test_resources_subtraction():
    resource_1 = _Resources(
        1,
        0,
        0,
        1,
        custom_resources={"a": 1, "b": 2},
        extra_custom_resources={"a": 1, "b": 1},
    )
    resource_2 = _Resources(
        1,
        0,
        0,
        1,
        custom_resources={"a": 1, "b": 2},
        extra_custom_resources={"a": 1, "b": 1},
    )
    new_res = _Resources.subtract(resource_1, resource_2)
    assert new_res.cpu == 0
    assert new_res.gpu == 0
    assert new_res.extra_cpu == 0
    assert new_res.extra_gpu == 0

    assert all(k == 0 for k in new_res.custom_resources.values())
    assert all(k == 0 for k in new_res.extra_custom_resources.values())


def test_resources_different():
    resource_1 = _Resources(1, 0, 0, 1, custom_resources={"a": 1, "b": 2})
    resource_2 = _Resources(1, 0, 0, 1, custom_resources={"a": 1, "c": 2})
    new_res = _Resources.subtract(resource_1, resource_2)
    assert "c" in new_res.custom_resources
    assert "b" in new_res.custom_resources

    assert new_res.cpu == 0
    assert new_res.gpu == 0
    assert new_res.extra_cpu == 0
    assert new_res.extra_gpu == 0
    assert new_res.get("a") == 0


def test_resource_updater(ray_start_cluster):
    cluster = ray_start_cluster

    resource_updater = _ResourceUpdater(refresh_period=100)
    # Before initialization, all resources are 0.
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

    resource_updater = _ResourceUpdater(refresh_period=0)
    assert resource_updater.get_num_cpus() == 2
    assert resource_updater.get_num_gpus() == 3

    cluster.add_node(num_cpus=1, num_gpus=1)
    cluster.wait_for_nodes()
    assert resource_updater.get_num_cpus() == 3
    assert resource_updater.get_num_gpus() == 4


def test_resource_updater_automatic():
    """Test that resources are automatically updated when they get out of sync.

    We instantiate a resource updater. When the reported resources are less than
    what is available, we don't force an update.
    However, if any of the resources (cpu, gpu, or custom) are higher than what
    the updater currently think is available, we force an update from the
    Ray cluster.
    """
    resource_updater = _ResourceUpdater()
    resource_updater._avail_resources = _Resources(
        cpu=2,
        gpu=1,
        memory=1,
        object_store_memory=1,
        custom_resources={"a": 4},
    )
    resource_updater._last_resource_refresh = 2

    # Should not trigger
    with mock.patch.object(
        _ResourceUpdater,
        "update_avail_resources",
        wraps=resource_updater.update_avail_resources,
    ) as upd:
        # No update
        assert "2/2 CPUs" in resource_updater.debug_string(
            total_allocated_resources={"CPU": 2, "GPU": 1, "a": 4}
        )
        assert upd.call_count == 0

        # Too many CPUs
        assert "4/2 CPUs" in resource_updater.debug_string(
            total_allocated_resources={"CPU": 4, "GPU": 1, "a": 0}
        )
        assert upd.call_count == 1

        # Too many GPUs
        assert "8/1 GPUs" in resource_updater.debug_string(
            total_allocated_resources={"CPU": 2, "GPU": 8, "a": 0}
        )
        assert upd.call_count == 2

        # Too many `a`
        assert "6/4 a" in resource_updater.debug_string(
            total_allocated_resources={"CPU": 2, "GPU": 1, "a": 6}
        )
        assert upd.call_count == 3

        # No update again
        assert "2/2 CPUs" in resource_updater.debug_string(
            total_allocated_resources={"CPU": 2, "GPU": 1, "a": 4}
        )
        assert upd.call_count == 3


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
