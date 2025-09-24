import os
import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

import ray
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
#  echo "+cpu +memory" | sudo tee -a /sys/fs/cgroup/resource_isolation_test/cgroup.subtree_control
#  sudo chown -R $(whoami):$(whoami) /sys/fs/cgroup/resource_isolation_test/
#  sudo chmod -R u+rwx /sys/fs/cgroup/resource_isolation_test/
#  echo $$ | sudo tee /sys/fs/cgroup/resource_isolation_test/cgroup.procs
#
# Comment the following line out.
_BASE_CGROUP_PATH = "/sys/fs/cgroup"
#
# Uncomment the following line.
# _BASE_CGROUP_PATH = "/sys/fs/cgroup/resource_isolation_test"


# TODO(#54703): This test is deliberately overspecified right now. The test shouldn't
# care about the cgroup hierarchy. It should just verify that application and system processes
# are started in a cgroup with the correct constraints. This will be updated once cgroup
# process management is completed.
def assert_cgroup_hierarchy_exists_for_node(
    node_id: str, resource_isolation_config: ResourceIsolationConfig
):
    base_cgroup_for_node = resource_isolation_config.cgroup_path
    node_cgroup = Path(base_cgroup_for_node) / f"ray_node_{node_id}"
    system_cgroup = node_cgroup / "system"
    system_leaf_cgroup = system_cgroup / "leaf"
    application_cgroup = node_cgroup / "application"
    application_leaf_cgroup = application_cgroup / "leaf"

    # 1) Check that the cgroup hierarchy is created correctly for the node.
    assert node_cgroup.is_dir()
    assert system_cgroup.is_dir()
    assert system_leaf_cgroup.is_dir()
    assert application_cgroup.is_dir()
    assert application_leaf_cgroup.is_dir()

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

    # 3) Check to see that all system pids are inside the system cgroup
    system_leaf_cgroup_procs = system_leaf_cgroup / "cgroup.procs"
    # At least the raylet process is always moved.
    with open(system_leaf_cgroup_procs, "r") as cgroup_procs_file:
        lines = cgroup_procs_file.readlines()
        assert (
            len(lines) > 0
        ), f"Expected only system process passed into the raylet. Found {lines}"


def assert_cgroup_hierarchy_cleaned_up_for_node(
    node_id: str, resource_isolation_config: ResourceIsolationConfig
):
    base_cgroup_for_node = resource_isolation_config.cgroup_path
    node_cgroup = Path(base_cgroup_for_node) / f"ray_node_{node_id}"
    assert not node_cgroup.is_dir()


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


def test_ray_start_resource_isolation_creates_cgroup_hierarchy_and_cleans_up(
    monkeypatch, cleanup_ray
):
    object_store_memory = 1024**3
    system_reserved_memory = 1024**3
    system_reserved_cpu = 1
    resource_isolation_config = ResourceIsolationConfig(
        cgroup_path=_BASE_CGROUP_PATH,
        enable_resource_isolation=True,
        system_reserved_cpu=system_reserved_cpu,
        system_reserved_memory=system_reserved_memory,
    )
    node_id = ray.NodeID.from_random().hex()
    os.environ["RAY_OVERRIDE_NODE_ID_FOR_TESTING"] = node_id
    runner = CliRunner()
    result = runner.invoke(
        scripts.start,
        [
            "--head",
            "--enable-resource-isolation",
            "--cgroup-path",
            _BASE_CGROUP_PATH,
            "--system-reserved-cpu",
            system_reserved_cpu,
            "--system-reserved-memory",
            system_reserved_memory,
            "--object-store-memory",
            object_store_memory,
        ],
    )
    assert result.exit_code == 0
    resource_isolation_config.add_object_store_memory(object_store_memory)
    assert_cgroup_hierarchy_exists_for_node(node_id, resource_isolation_config)
    runner.invoke(scripts.stop)
    assert_cgroup_hierarchy_cleaned_up_for_node(node_id, resource_isolation_config)


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


def test_ray_init_with_resource_isolation_override_defaults(ray_shutdown):
    system_reserved_cpu = 1
    system_reserved_memory = 1024**3
    object_store_memory = 1024**3
    resource_isolation_config = ResourceIsolationConfig(
        enable_resource_isolation=True,
        cgroup_path=_BASE_CGROUP_PATH,
        system_reserved_cpu=system_reserved_cpu,
        system_reserved_memory=system_reserved_memory,
    )
    resource_isolation_config.add_object_store_memory(object_store_memory)
    ray.init(
        address="local",
        enable_resource_isolation=True,
        _cgroup_path=_BASE_CGROUP_PATH,
        system_reserved_cpu=system_reserved_cpu,
        system_reserved_memory=system_reserved_memory,
        object_store_memory=object_store_memory,
    )
    node = ray._private.worker._global_node
    assert node is not None
    node_id = node.node_id
    assert_cgroup_hierarchy_exists_for_node(node_id, resource_isolation_config)
    ray.shutdown()
    assert_cgroup_hierarchy_cleaned_up_for_node(node_id, resource_isolation_config)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
