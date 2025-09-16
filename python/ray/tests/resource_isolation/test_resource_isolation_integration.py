import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

import ray
import ray._private.ray_constants as ray_constants
import ray._private.utils as utils
import ray.scripts.scripts as scripts
from ray._private.resource_isolation_config import ResourceIsolationConfig

# These tests are intended to run in CI inside a container.
# If you want to run this test locally, you will need to create a cgroup that
# the raylet can manage and delegate to the correct user.
#
# TODO(#54703): Once implementation is complete, I will add a fixture to this
# test to check for common errors when running locally (such as cgroup2 not mounted
# correct). It'll follow the example of
# src/ray/common/cgroup2/integration_tests/sysfs_cgroup_driver_integration_test_entrypoint.sh
#
# Run these commands locally before running the test suite:
#  sudo mkdir -p /sys/fs/cgroup/resource_isolation_test
#  sudo chown -R $(whoami):$(whoami) /sys/fs/cgroup/resource_isolation_test/
#  sudo chmod -R u+rwx /sys/fs/cgroup/resource_isolation_test/
#  echo $$ | sudo tee /sys/fs/cgroup/resource_isolation_test/cgroup.procs
#
# Comment the following line out.
_BASE_CGROUP_PATH = "/sys/fs/cgroup"
#
# Uncomment the following line.
# _BASE_CGROUP_PATH = "/sys/fs/cgroup/resource_isolation_test"


def test_resource_isolation_enabled_creates_cgroup_hierarchy(ray_start_cluster):
    cluster = ray_start_cluster
    base_cgroup = _BASE_CGROUP_PATH
    resource_isolation_config = ResourceIsolationConfig(
        enable_resource_isolation=True,
        cgroup_path=base_cgroup,
        system_reserved_memory=1024**3,
        system_reserved_cpu=1,
    )
    # Need to use a worker node because the driver cannot delete the head node.
    cluster.add_node(num_cpus=0)
    ray.init(address=cluster.address)

    worker_node = cluster.add_node(
        num_cpus=1, resource_isolation_config=resource_isolation_config
    )
    worker_node_id = worker_node.node_id
    cluster.wait_for_nodes()

    # Make sure the worker node is up and running.
    @ray.remote
    def task():
        return "hellodarknessmyoldfriend"

    ray.get(task.remote(), timeout=5)

    # TODO(#54703): This test is deliberately overspecified right now. The test shouldn't
    # care about the cgroup hierarchy. It should just verify that application and system processes
    # are started in a cgroup with the correct constraints. This will be updated once cgroup
    # process management is completed.
    node_cgroup = Path(base_cgroup) / f"ray_node_{worker_node_id}"
    system_cgroup = node_cgroup / "system"
    application_cgroup = node_cgroup / "application"

    # 1) Check that the cgroup hierarchy is created correctly for the node.
    assert node_cgroup.is_dir()
    assert system_cgroup.is_dir()
    assert application_cgroup.is_dir()

    # 2) Verify the constraints are applied correctly.
    system_cgroup_memory_min = system_cgroup / "memory.min"
    with open(system_cgroup_memory_min, "r") as memory_min_file:
        contents = memory_min_file.read().strip()
        assert contents == str(resource_isolation_config.system_reserved_memory)
    system_cgroup_cpu_weight = system_cgroup / "cpu.weight"
    with open(system_cgroup_cpu_weight, "r") as cpu_weight_file:
        contents = cpu_weight_file.read().strip()
        assert contents == str(resource_isolation_config.system_reserved_cpu_weight)
    application_cgroup_cpu_weight = application_cgroup / "cpu.weight"
    with open(application_cgroup_cpu_weight, "r") as cpu_weight_file:
        contents = cpu_weight_file.read().strip()
        assert contents == str(
            10000 - resource_isolation_config.system_reserved_cpu_weight
        )

    # 3) Gracefully shutting down the node cleans up everything. Don't need to check
    # everything. If the base_cgroup is deleted, then all clean up succeeded.
    cluster.remove_node(worker_node)
    assert not node_cgroup.is_dir()


# The following tests will test integration of resource isolation
# with the 'ray start' command.
@pytest.fixture
def cleanup_ray():
    """Shutdown all ray instances"""
    yield
    runner = CliRunner()
    runner.invoke(scripts.stop)
    ray.shutdown()


def test_ray_start_invalid_resource_isolation_config(cleanup_ray):
    runner = CliRunner()
    result = runner.invoke(
        scripts.start,
        ["--cgroup-path=/doesnt/matter"],
    )
    assert result.exit_code != 0
    assert isinstance(result.exception, ValueError)


def test_ray_start_resource_isolation_config_default_values(monkeypatch, cleanup_ray):
    monkeypatch.setattr(utils, "get_num_cpus", lambda *args, **kwargs: 16)
    # The DEFAULT_CGROUP_PATH override is only relevant when running locally.
    monkeypatch.setattr(ray_constants, "DEFAULT_CGROUP_PATH", _BASE_CGROUP_PATH)

    runner = CliRunner()
    result = runner.invoke(
        scripts.start,
        ["--head", "--enable-resource-isolation"],
    )
    # TODO(#54703): Need to rewrite this test to check for side-effects on the cgroup
    # hierarchy once the rest of the implemetation is complete.
    assert result.exit_code == 0


# The following tests will test integration of resource isolation
# with the ray.init() function.
@pytest.fixture
def ray_shutdown():
    yield
    ray.shutdown()


def test_ray_init_resource_isolation_disabled_by_default(ray_shutdown):
    ray.init(address="local")
    node = ray._private.worker._global_node
    assert node is not None
    assert not node.resource_isolation_config.is_enabled()


def test_ray_init_with_resource_isolation_default_values(monkeypatch, ray_shutdown):
    total_system_cpu = 10
    monkeypatch.setattr(utils, "get_num_cpus", lambda *args, **kwargs: total_system_cpu)
    # The DEFAULT_CGROUP_PATH override is only relevant when running locally.
    monkeypatch.setattr(ray_constants, "DEFAULT_CGROUP_PATH", _BASE_CGROUP_PATH)
    ray.init(address="local", enable_resource_isolation=True)
    node = ray._private.worker._global_node
    assert node is not None
    assert node.resource_isolation_config.is_enabled()


def test_ray_init_with_resource_isolation_override_defaults(ray_shutdown):
    cgroup_path = _BASE_CGROUP_PATH
    system_reserved_cpu = 1
    system_reserved_memory = 1 * 10**9
    object_store_memory = 1 * 10**9
    resource_isolation_config = ResourceIsolationConfig(
        enable_resource_isolation=True,
        cgroup_path=cgroup_path,
        system_reserved_cpu=system_reserved_cpu,
        system_reserved_memory=system_reserved_memory,
    )
    resource_isolation_config.add_object_store_memory(object_store_memory)
    ray.init(
        address="local",
        enable_resource_isolation=True,
        _cgroup_path=cgroup_path,
        system_reserved_cpu=system_reserved_cpu,
        system_reserved_memory=system_reserved_memory,
        object_store_memory=object_store_memory,
    )
    node = ray._private.worker._global_node
    # TODO(#54703): Need to rewrite this test to check for side-effects on the cgroup
    # hierarchy once the rest of the implemetation is complete.
    assert node is not None
    assert node.resource_isolation_config.is_enabled()
    assert (
        node.resource_isolation_config.system_reserved_cpu_weight
        == resource_isolation_config.system_reserved_cpu_weight
    )
    assert (
        node.resource_isolation_config.system_reserved_memory
        == resource_isolation_config.system_reserved_memory
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
