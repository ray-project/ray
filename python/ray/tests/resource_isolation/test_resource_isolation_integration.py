import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

import ray
import ray._private.ray_constants as ray_constants  # noqa
import ray._private.utils as utils
import ray.scripts.scripts as scripts
from ray._private.resource_isolation_config import ResourceIsolationConfig


# These tests is intended to run in CI inside a container.
# If you want to run this test locally, you will need to create a cgroup that
# the raylet can manage and delegate to the correct user.
#  sudo mkdir -p /sys/fs/cgroup/resource_isolation_test
#  sudo chown -R $(whoami):$(whoami) /sys/fs/cgroup/resource_isolation_test/
#  sudo chmod -R u+rwx /sys/fs/cgroup/resource_isolation_test/
#  echo $$ | sudo tee /sys/fs/cgroup/resource_isolation_test/cgroup.procs
def test_resource_isolation_enabled_creates_cgroup_hierarchy(ray_start_cluster):
    cluster = ray_start_cluster
    # change this to /sys/fs/cgroup/resource_isolation_test if running locally.
    # base_cgroup = "/sys/fs/cgroup/resource_isolation_test"
    base_cgroup = "/sys/fs/cgroup"
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


# Test that resource isolation can be enabled from cli
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
    # Uncomment this line to run this test locally. The default cgroup for ray is
    # /sys/fs/cgroup (defined as ray.constants.DEFAULT_CGROUP_PATH).
    # monkeypatch.setattr(ray_constants, "DEFAULT_CGROUP_PATH", "/sys/fs/cgroup/resource_isolation_test")

    runner = CliRunner()
    result = runner.invoke(
        scripts.start,
        ["--head", "--enable-resource-isolation"],
    )
    # TODO(#54703): This only checks to see that we start up, it doesn't check to see that we start
    # up with default values. Need to test the side-effect.
    assert result.exit_code == 0


# Test that resource isolation can be enabled from ray.init
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
    # Uncomment this line to run this test locally. The default cgroup for ray is
    # /sys/fs/cgroup (defined as ray.constrants.DEFAULT_CGROUP_PATH).
    # monkeypatch.setattr(ray_constants, "DEFAULT_CGROUP_PATH", "/sys/fs/cgroup/resource_isolation_test")
    ray.init(address="local", enable_resource_isolation=True)
    node = ray._private.worker._global_node
    assert node is not None
    assert node.resource_isolation_config.is_enabled()


def test_ray_init_with_resource_isolation_override_defaults(monkeypatch, ray_shutdown):
    # testing locally uncomment
    # cgroup_path = "/sys/fs/cgroup/resource_isolation_test"
    cgroup_path = "/sys/fs/cgroup/"
    system_reserved_cpu = 1
    system_reserved_memory = 1 * 10**9
    total_system_cpu = 10
    total_system_memory = 10 * 10**9
    object_store_memory = 1 * 10**9
    monkeypatch.setattr(utils, "get_num_cpus", lambda *args, **kwargs: total_system_cpu)
    monkeypatch.setattr(
        utils, "get_system_memory", lambda *args, **kwargs: total_system_memory
    )
    ray.init(
        address="local",
        enable_resource_isolation=True,
        _cgroup_path=cgroup_path,
        system_reserved_cpu=system_reserved_cpu,
        system_reserved_memory=system_reserved_memory,
        object_store_memory=object_store_memory,
    )
    node = ray._private.worker._global_node
    # TODO(#54703): Need to check side-effects on cgroups.
    assert node is not None
    assert node.resource_isolation_config.is_enabled()
    assert node.resource_isolation_config.system_reserved_cpu_weight == 1000
    assert (
        node.resource_isolation_config.system_reserved_memory
        == system_reserved_memory + object_store_memory
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
