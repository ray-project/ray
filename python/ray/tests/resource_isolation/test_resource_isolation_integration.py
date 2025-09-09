import sys
from pathlib import Path

import pytest

import ray
from ray._private.resource_isolation_config import ResourceIsolationConfig


# This test is intended to run in CI inside a container.
# If you want to run this test locally, you will need to create a cgroup that
# the raylet can manage and delegate to the correct user.
#  sudo mkdir -p /sys/fs/cgroup/resource_isolation_test
#  sudo chown -R $(whoami):$(whoami) /sys/fs/cgroup/resource_isolation_test/
#  sudo chmod -R u+rwx /sys/fs/cgroup/resource_isolation_test/
#  echo $$ | sudo tee /sys/fs/cgroup/resource_isolation_test/cgroup.procs
def test_resource_isolation_enabled_creates_cgroup_hierarchy(ray_start_cluster):
    cluster = ray_start_cluster
    # change this to /sys/fs/cgroup/resource_isolation_test if running locally.
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
