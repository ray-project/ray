import os
import platform
import sys
from pathlib import Path
from typing import Set

import pytest
from click.testing import CliRunner

import ray
import ray._common.utils as utils
import ray._private.ray_constants as ray_constants
import ray.scripts.scripts as scripts
from ray._private.resource_isolation_config import ResourceIsolationConfig

# These tests are intended to run in CI inside a container.
#
# If you want to run this test locally, you will need to create a cgroup that
# the raylet can manage and delegate to the correct user.
#
# Run these commands locally before running the test suite:
#
#  sudo mkdir -p /sys/fs/cgroup/resource_isolation_test
#  echo "+cpu +memory" | sudo tee -a /sys/fs/cgroup/resource_isolation_test/cgroup.subtree_control
#  sudo chown -R $(whoami):$(whoami) /sys/fs/cgroup/resource_isolation_test/
#  sudo chmod -R u+rwx /sys/fs/cgroup/resource_isolation_test/
#  echo $$ | sudo tee /sys/fs/cgroup/resource_isolation_test/cgroup.procs
#
# Comment the following line out.
_ROOT_CGROUP = Path("/sys/fs/cgroup")
#
# To run locally, uncomment the following line.
# _ROOT_CGROUP = Path("/sys/fs/cgroup/resource_isolation_test")

# The integration tests assume that the _ROOT_CGROUP exists and that
# the process has read and write access.
#
# This test suite will create the following cgroup hierarchy for the tests
# starting with BASE_CGROUP.
#
#                        ROOT_CGROUP
#                             |
#                        BASE_CGROUP
#                       /           \
#                 TEST_CGROUP   LEAF_CGROUP
#                      |
#               ray_node_<node_id>
#               /               \
#           system            application
#             |                    |
#           leaf                  leaf
#
# NOTE: The test suite does not assume that ROOT_CGROUP is an actual root cgroup. Therefore,
#   1. setup will migrate all processes from the ROOT_CGROUP -> LEAF_CGROUP
#   2. teardown will migrate all processes from the LEAF_CGROUP -> ROOT_CGROUP
#
# NOTE: BASE_CGROUP will have a randomly generated name to isolate tests from each other.
#
# The test suite assumes that
#   1. cpu, memory controllers are available on ROOT_CGROUP i.e. in the ROOT_CGROUP/cgroup.controllers file.
#   2. All processes inside the base_cgroup can be migrated into the leaf_cgroup to avoid not violating
#   the no internal processes contstraint.
#
# All python tests should only have access to the TEST_CGROUP and nothing outside of it.

_BASE_CGROUP = _ROOT_CGROUP / ("testing_" + utils.get_random_alphanumeric_string(5))
_TEST_CGROUP = _BASE_CGROUP / "test"
_LEAF_GROUP = _BASE_CGROUP / "leaf"

_MOUNT_FILE_PATH = "/etc/mtab"

# The list of processes expected to be started in the system cgroup
# with default params for 'ray start' and 'ray.init(...)'
_EXPECTED_SYSTEM_PROCESSES_RAY_START = [
    ray_constants.PROCESS_TYPE_DASHBOARD,
    ray_constants.PROCESS_TYPE_GCS_SERVER,
    ray_constants.PROCESS_TYPE_MONITOR,
    ray_constants.PROCESS_TYPE_LOG_MONITOR,
    ray_constants.PROCESS_TYPE_RAY_CLIENT_SERVER,
    ray_constants.PROCESS_TYPE_RAYLET,
    ray_constants.PROCESS_TYPE_DASHBOARD_AGENT,
    ray_constants.PROCESS_TYPE_RUNTIME_ENV_AGENT,
]
_EXPECTED_SYSTEM_PROCESSES_RAY_INIT = [
    ray_constants.PROCESS_TYPE_DASHBOARD,
    ray_constants.PROCESS_TYPE_GCS_SERVER,
    ray_constants.PROCESS_TYPE_MONITOR,
    ray_constants.PROCESS_TYPE_LOG_MONITOR,
    ray_constants.PROCESS_TYPE_RAYLET,
    ray_constants.PROCESS_TYPE_DASHBOARD_AGENT,
    ray_constants.PROCESS_TYPE_RUNTIME_ENV_AGENT,
]


@pytest.fixture(scope="session", autouse=True)
def test_suite_fixture():
    """Setups up and tears down the cgroup hierachy for the test suite."""
    setup_test_suite()
    yield
    cleanup_test_suite()


def setup_test_suite():
    """Creates the cgroup hierarchy and moves processes out of the _ROOT_CGROUP into the _LEAF_CGROUP.

    The setup involves the following steps:
        1) Check if the platform is Linux.
        2) Check that cgroupv2 is mounted with read, write permissions in unified mode i.e. cgroupv1 is not mounted.
        3) Check that the _ROOT_CGROUP exists and has [cpu, memory] controllers available.
        4) Create the _BASE_CGROUP, _TEST_CGROUP, and _LEAF_CGROUP respectively.
        5) Move processes from the _ROOT_CGROUP to the _LEAF_CGROUP because of the internal processes constraint.
        6) Enable [cpu, memory] controllers in the _ROOT_CGROUP, _BASE_CGROUP, and _TEST_CGROUP respectively.

    If any of the steps fail, teardown will be run. Teardown will perform a subset of these steps (not the checks), in reverse order.
    """
    try:
        # 1) If platform is not linux.
        assert (
            platform.system() == "Linux"
        ), f"Failed because resource isolation integration tests can only run on Linux and not on {platform.system()}."

        # 2) Check that cgroupv2 is mounted in read-write mode in unified mode.
        with open(_MOUNT_FILE_PATH, "r") as mount_file:
            lines = mount_file.readlines()
            found_cgroup_v1 = False
            found_cgroup_v2 = False
            for line in lines:
                found_cgroup_v1 = found_cgroup_v1 or ("cgroup r" in line.strip())
                found_cgroup_v2 = found_cgroup_v2 or ("cgroup2 rw" in line.strip())

            assert found_cgroup_v2, (
                "Failed because cgroupv2 is not mounted on the system in read-write mode."
                " See the following documentation for how to enable cgroupv2 properly:"
                " https://kubernetes.io/docs/concepts/architecture/cgroups/#linux-distribution-cgroup-v2-support"
            )

            assert not found_cgroup_v1, (
                "Failed because cgroupv2 and cgroupv1 is mounted on this system."
                " See the following documentation for how to enable cgroupv2 in properly in unified mode:"
                " https://kubernetes.io/docs/concepts/architecture/cgroups/#linux-distribution-cgroup-v2-support"
            )

        # 3) Check that current user has read-write access to _BASE_CGROUP_PATH by attempting
        # to write the current process into it.
        root_cgroup_procs_file = _ROOT_CGROUP / "cgroup.procs"
        with open(root_cgroup_procs_file, "w") as procs_file:
            procs_file.write(str(os.getpid()))
            procs_file.flush()

        # 4) Check to see that _ROOT_CGROUP has the [cpu, memory] controllers are available.
        root_cgroup_controllers_path = _ROOT_CGROUP / "cgroup.controllers"
        expected_controllers = {"cpu", "memory"}
        with open(root_cgroup_controllers_path, "r") as available_controllers_file:
            available_controllers = set(
                available_controllers_file.readline().strip().split(" ")
            )
            assert expected_controllers.issubset(available_controllers), (
                f"Failed because the cpu and memory controllers are not available in {root_cgroup_controllers_path}."
                " To enable a controller, you need to add it to the cgroup.controllers file of the parent cgroup of {_ROOT_CGROUP}."
                " See: https://docs.kernel.org/admin-guide/cgroup-v2.html#enabling-and-disabling."
            )

        # 5) Create the leaf cgroup and move all processes from _BASE_CGROUP_PATH into it.
        os.mkdir(_BASE_CGROUP)
        os.mkdir(_TEST_CGROUP)
        os.mkdir(_LEAF_GROUP)

        # 6) Move all processes into the leaf cgroup.
        with open(_ROOT_CGROUP / "cgroup.procs", "r") as root_procs_file, open(
            _LEAF_GROUP / "cgroup.procs", "w"
        ) as leaf_procs_file:
            root_cgroup_lines = root_procs_file.readlines()
            for line in root_cgroup_lines:
                leaf_procs_file.write(line.strip())
                leaf_procs_file.flush()

        # 7) Enable [cpu, memory] controllers on the base and test cgroup.
        with open(
            _ROOT_CGROUP / "cgroup.subtree_control", "w"
        ) as base_subtree_control_file:
            base_subtree_control_file.write("+cpu +memory")
            base_subtree_control_file.flush()
        with open(
            _BASE_CGROUP / "cgroup.subtree_control", "w"
        ) as base_subtree_control_file:
            base_subtree_control_file.write("+cpu +memory")
            base_subtree_control_file.flush()
        with open(
            _TEST_CGROUP / "cgroup.subtree_control", "w"
        ) as test_subtree_control_file:
            test_subtree_control_file.write("+cpu +memory")
            test_subtree_control_file.flush()
    except Exception as e:
        print(
            f"Failed to setup the test suite with error {str(e)}. Attempting to run teardown."
        )
        cleanup_test_suite()


def cleanup_test_suite():
    """Cleans up the cgroup hierarchy and moves processes out of the _LEAF_CGROUP into the _ROOT_CGROUP.

    The setup involves the following steps:
        1) Disable [cpu, memory] controllers in the _ROOT_CGROUP, _BASE_CGROUP, and _TEST_CGROUP respectively.
        2) Move processes from the _LEAF_CGROUP to the _ROOT_CGROUP so the hierarchy can be deleted.
        3) Create the _BASE_CGROUP, _TEST_CGROUP, and _LEAF_CGROUP respectively.

    If any of the steps fail, teardown will fail an assertion.
    """
    # 1) Disable the controllers.
    try:
        with open(
            _TEST_CGROUP / "cgroup.subtree_control", "w"
        ) as test_subtree_control_file:
            test_subtree_control_file.write("-cpu -memory")
            test_subtree_control_file.flush()
        with open(
            _BASE_CGROUP / "cgroup.subtree_control", "w"
        ) as base_subtree_control_file:
            base_subtree_control_file.write("-cpu -memory")
            base_subtree_control_file.flush()
        with open(
            _ROOT_CGROUP / "cgroup.subtree_control", "w"
        ) as base_subtree_control_file:
            base_subtree_control_file.write("-cpu -memory")
            base_subtree_control_file.flush()
        # 2) Move processes back into the leaf cgroup.
        with open(_ROOT_CGROUP / "cgroup.procs", "w") as root_procs_file, open(
            _LEAF_GROUP / "cgroup.procs", "r"
        ) as leaf_procs_file:
            leaf_cgroup_lines = leaf_procs_file.readlines()
            for line in leaf_cgroup_lines:
                root_procs_file.write(line.strip())
                root_procs_file.flush()
        # 3) Delete the cgroups.
        os.rmdir(_LEAF_GROUP)
        os.rmdir(_TEST_CGROUP)
        os.rmdir(_BASE_CGROUP)
    except Exception as e:
        assert False, (
            f"Failed to cleanup test suite's cgroup hierarchy because of {str(e)}."
            "You may have to manually clean up the hierachy under ${_ROOT_CGROUP}"
        )


@pytest.fixture
def cleanup_ray():
    """Shutdown all ray instances"""
    yield
    runner = CliRunner()
    runner.invoke(scripts.stop)
    ray.shutdown()


@pytest.fixture
def ray_shutdown():
    yield
    ray.shutdown()


def generate_node_id():
    """Returns a random node id."""
    return ray.NodeID.from_random().hex()


def assert_cgroup_hierarchy_exists_for_node(
    node_id: str, resource_isolation_config: ResourceIsolationConfig
):
    """Asserts that the cgroup hierarchy was created correctly for the node.

    The cgroup hierarchy looks like:

           _TEST_CGROUP
                |
        ray_node_<node_id>
          |            |
        system     application
          |            |
        leaf         leaf

    Args:
        node_id: used to find the path of the cgroup subtree
        resource_isolation_config: used to verify constraints enabled on the system
            and application cgroups
    """
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


def assert_system_processes_are_in_system_cgroup(
    node_id, resource_isolation_config, expected_count
):
    base_cgroup_for_node = resource_isolation_config.cgroup_path
    node_cgroup = Path(base_cgroup_for_node) / f"ray_node_{node_id}"
    system_cgroup = node_cgroup / "system"
    system_leaf_cgroup = system_cgroup / "leaf"

    # At least the raylet process is always moved.
    with open(system_leaf_cgroup / "cgroup.procs", "r") as cgroup_procs_file:
        lines = cgroup_procs_file.readlines()
        assert (
            len(lines) == expected_count
        ), f"Expected only system process passed into the raylet. Found {lines}"


def assert_worker_processes_are_in_application_cgroup(
    node_id: str,
    resource_isolation_config: ResourceIsolationConfig,
    worker_pids: Set[str],
):
    """Asserts that the cgroup hierarchy was deleted correctly for the node.

    Args:
        node_id: used to construct the path of the cgroup subtree
        resource_isolation_config: used to construct the path of the cgroup
            subtree
        worker_pids: a set of pids that are expected inside the application
            leaf cgroup.
    """
    base_cgroup_for_node = resource_isolation_config.cgroup_path
    node_cgroup = Path(base_cgroup_for_node) / f"ray_node_{node_id}"
    application_leaf_cgroup_procs = (
        node_cgroup / "application" / "leaf" / "cgroup.procs"
    )
    with open(application_leaf_cgroup_procs, "r") as cgroup_procs_file:
        pids_in_cgroup = set()
        lines = cgroup_procs_file.readlines()
        for line in lines:
            pids_in_cgroup.add(line.strip())
        assert pids_in_cgroup == worker_pids


def assert_cgroup_hierarchy_cleaned_up_for_node(
    node_id: str, resource_isolation_config: ResourceIsolationConfig
):
    """Asserts that the cgroup hierarchy was deleted correctly for the node.

    Args:
        node_id: used to construct the path of the cgroup subtree
        resource_isolation_config: used to construct the path of the cgroup
            subtree
    """
    base_cgroup_for_node = resource_isolation_config.cgroup_path
    node_cgroup = Path(base_cgroup_for_node) / f"ray_node_{node_id}"
    # If the root cgroup is deleted, there's no need to check anything else.
    assert (
        not node_cgroup.is_dir()
    ), f"Root cgroup node at {node_cgroup} was not deleted. Cgroup cleanup failed. You may have to manually delete the cgroup subtree."


# The following tests check for cgroup setup and cleanup with the
# ray cli.
def test_ray_cli_start_invalid_resource_isolation_config(cleanup_ray):
    runner = CliRunner()
    result = runner.invoke(
        scripts.start,
        ["--cgroup-path=/doesnt/matter"],
    )
    assert result.exit_code != 0
    assert isinstance(result.exception, ValueError)


def test_ray_cli_start_resource_isolation_creates_cgroup_hierarchy_and_cleans_up(
    cleanup_ray,
):
    cgroup_path = str(_TEST_CGROUP)
    object_store_memory = 1024**3
    system_reserved_memory = 1024**3
    num_cpus = 4
    system_reserved_cpu = 1
    resource_isolation_config = ResourceIsolationConfig(
        cgroup_path=cgroup_path,
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
            "--num-cpus",
            num_cpus,
            "--enable-resource-isolation",
            "--cgroup-path",
            cgroup_path,
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
    assert_system_processes_are_in_system_cgroup(
        node_id, resource_isolation_config, len(_EXPECTED_SYSTEM_PROCESSES_RAY_START)
    )

    @ray.remote(num_cpus=1)
    class Actor:
        def __init__(self):
            pass

        def get_pid(self):
            return os.getpid()

    actor_refs = []
    for _ in range(num_cpus):
        actor_refs.append(Actor.remote())
    worker_pids = set()
    for actor in actor_refs:
        worker_pids.add(str(ray.get(actor.get_pid.remote())))
    assert_worker_processes_are_in_application_cgroup(
        node_id, resource_isolation_config, worker_pids
    )
    runner.invoke(scripts.stop)
    assert_cgroup_hierarchy_cleaned_up_for_node(node_id, resource_isolation_config)


# The following tests will test integration of resource isolation
# with the ray.init() function.
def test_ray_init_resource_isolation_disabled_by_default(ray_shutdown):
    ray.init(address="local")
    node = ray._private.worker._global_node
    assert node is not None
    assert not node.resource_isolation_config.is_enabled()


def test_ray_init_resource_isolation_creates_cgroup_hierarchy_and_cleans_up(
    ray_shutdown,
):
    cgroup_path = str(_TEST_CGROUP)
    system_reserved_cpu = 1
    system_reserved_memory = 1024**3
    object_store_memory = 1024**3
    num_cpus = 4
    resource_isolation_config = ResourceIsolationConfig(
        enable_resource_isolation=True,
        cgroup_path=cgroup_path,
        system_reserved_cpu=system_reserved_cpu,
        system_reserved_memory=system_reserved_memory,
    )
    resource_isolation_config.add_object_store_memory(object_store_memory)
    node_id = generate_node_id()
    os.environ["RAY_OVERRIDE_NODE_ID_FOR_TESTING"] = node_id
    ray.init(
        address="local",
        num_cpus=num_cpus,
        enable_resource_isolation=True,
        _cgroup_path=cgroup_path,
        system_reserved_cpu=system_reserved_cpu,
        system_reserved_memory=system_reserved_memory,
        object_store_memory=object_store_memory,
    )
    assert_cgroup_hierarchy_exists_for_node(node_id, resource_isolation_config)
    assert_system_processes_are_in_system_cgroup(
        node_id, resource_isolation_config, len(_EXPECTED_SYSTEM_PROCESSES_RAY_INIT)
    )

    @ray.remote(num_cpus=1)
    class Actor:
        def __init__(self):
            pass

        def get_pid(self):
            return os.getpid()

    actor_refs = []
    for _ in range(num_cpus):
        actor_refs.append(Actor.remote())
    worker_pids = set()
    for actor in actor_refs:
        worker_pids.add(str(ray.get(actor.get_pid.remote())))
    assert_worker_processes_are_in_application_cgroup(
        node_id, resource_isolation_config, worker_pids
    )
    ray.shutdown()
    assert_cgroup_hierarchy_cleaned_up_for_node(node_id, resource_isolation_config)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
