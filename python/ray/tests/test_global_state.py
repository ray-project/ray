import os
import sys
import time
from typing import Dict, Optional

import pytest

import ray
import ray._private.gcs_utils as gcs_utils
from ray._common.test_utils import wait_for_condition
from ray._private.test_utils import (
    make_global_state_accessor,
)
from ray._raylet import GcsClient
from ray.core.generated import autoscaler_pb2
from ray.util.state import list_actors


def test_replenish_resources(ray_start_regular):
    cluster_resources = ray.cluster_resources()
    available_resources = ray.available_resources()
    assert cluster_resources == available_resources

    @ray.remote
    def cpu_task():
        pass

    ray.get(cpu_task.remote())

    wait_for_condition(lambda: ray.available_resources() == cluster_resources)


def test_uses_resources(ray_start_regular):
    cluster_resources = ray.cluster_resources()

    @ray.remote(num_cpus=1)
    class Actor:
        pass

    actor = Actor.remote()
    ray.get(actor.__ray_ready__.remote())

    wait_for_condition(
        lambda: ray.available_resources().get("CPU", 0)
        == cluster_resources.get("CPU", 0) - 1
    )


def test_available_resources_per_node(ray_start_cluster_head):
    cluster = ray_start_cluster_head

    @ray.remote
    def get_node_id():
        return ray.get_runtime_context().get_node_id()

    head_node_id = ray.get(get_node_id.remote())

    worker_node = cluster.add_node(num_cpus=3, resources={"worker": 1})

    @ray.remote(num_cpus=1, resources={"worker": 1})
    class Actor:
        def ping(self):
            return ray.get_runtime_context().get_node_id()

    actor = Actor.remote()
    worker_node_id = ray.get(actor.ping.remote())

    def available_resources_per_node_check1():
        available_resources_per_node = ray._private.state.available_resources_per_node()
        assert len(available_resources_per_node) == 2
        assert available_resources_per_node[head_node_id]["CPU"] == 1
        assert available_resources_per_node[worker_node_id]["CPU"] == 2
        assert available_resources_per_node[worker_node_id].get("worker", 0) == 0
        return True

    wait_for_condition(available_resources_per_node_check1)

    cluster.remove_node(worker_node)
    cluster.wait_for_nodes()

    def available_resources_per_node_check2():
        # Make sure worker node is not returned
        available_resources_per_node = ray._private.state.available_resources_per_node()
        assert len(available_resources_per_node) == 1
        assert available_resources_per_node[head_node_id]["CPU"] == 1
        return True

    wait_for_condition(available_resources_per_node_check2)


def test_total_resources_per_node(ray_start_cluster_head):
    cluster = ray_start_cluster_head

    @ray.remote
    def get_node_id():
        return ray.get_runtime_context().get_node_id()

    head_node_id = ray.get(get_node_id.remote())

    worker_node = cluster.add_node(num_cpus=3, resources={"worker": 1})

    @ray.remote(num_cpus=1, resources={"worker": 1})
    class Actor:
        def ping(self):
            return ray.get_runtime_context().get_node_id()

    actor = Actor.remote()
    worker_node_id = ray.get(actor.ping.remote())

    def total_resources_per_node_check1():
        total_resources_per_node = ray._private.state.total_resources_per_node()
        assert len(total_resources_per_node) == 2
        assert total_resources_per_node[head_node_id]["CPU"] == 1
        assert total_resources_per_node[worker_node_id]["CPU"] == 3
        assert total_resources_per_node[worker_node_id].get("worker", 0) == 1
        return True

    wait_for_condition(total_resources_per_node_check1)

    cluster.remove_node(worker_node)
    cluster.wait_for_nodes()

    def total_resources_per_node_check2():
        # Make sure worker node is not returned
        total_resources_per_node = ray._private.state.total_resources_per_node()
        assert len(total_resources_per_node) == 1
        assert total_resources_per_node[head_node_id]["CPU"] == 1
        return True

    wait_for_condition(total_resources_per_node_check2)


def test_add_remove_cluster_resources(ray_start_cluster_head):
    """Tests that Global State API is consistent with actual cluster."""
    cluster = ray_start_cluster_head
    assert ray.cluster_resources()["CPU"] == 1
    nodes = []
    nodes += [cluster.add_node(num_cpus=1)]
    cluster.wait_for_nodes()
    assert ray.cluster_resources()["CPU"] == 2

    cluster.remove_node(nodes.pop())
    cluster.wait_for_nodes()
    assert ray.cluster_resources()["CPU"] == 1

    for i in range(5):
        nodes += [cluster.add_node(num_cpus=1)]
    cluster.wait_for_nodes()
    assert ray.cluster_resources()["CPU"] == 6


@pytest.mark.parametrize(
    "ray_start_regular",
    [{"include_dashboard": True}],
    indirect=True,
)
def test_global_state_actor_table(ray_start_regular):
    @ray.remote
    class Actor:
        def ready(self):
            return os.getpid()

    # actor table should be empty at first
    assert len(list_actors()) == 0

    a = Actor.remote()
    pid = ray.get(a.ready.remote())
    assert len(list_actors()) == 1
    assert list_actors()[0].pid == pid

    # actor table should contain only this entry
    # even when the actor goes out of scope
    del a

    for _ in range(10):
        if list_actors()[0].state == "DEAD":
            break
        else:
            time.sleep(0.5)
    assert list_actors()[0].state == "DEAD"


def test_global_state_worker_table(ray_start_regular):
    def worker_initialized():
        # Get worker table from gcs.
        workers_data = ray._private.state.workers()
        return len(workers_data) == 1

    wait_for_condition(worker_initialized)


@pytest.mark.parametrize(
    "ray_start_regular",
    [{"include_dashboard": True}],
    indirect=True,
)
def test_global_state_actor_entry(ray_start_regular):
    @ray.remote
    class Actor:
        def ready(self):
            pass

    # actor table should be empty at first
    assert len(list_actors()) == 0

    a = Actor.remote()
    b = Actor.remote()
    ray.get(a.ready.remote())
    ray.get(b.ready.remote())
    assert len(list_actors()) == 2
    a_actor_id = a._actor_id.hex()
    b_actor_id = b._actor_id.hex()
    assert ray.util.state.get_actor(id=a_actor_id).actor_id == a_actor_id
    assert ray.util.state.get_actor(id=a_actor_id).state == "ALIVE"
    assert ray.util.state.get_actor(id=b_actor_id).actor_id == b_actor_id
    assert ray.util.state.get_actor(id=b_actor_id).state == "ALIVE"


def test_node_name_cluster(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(node_name="head_node", include_dashboard=False)
    head_context = ray.init(address=cluster.address, include_dashboard=False)
    cluster.add_node(node_name="worker_node", include_dashboard=False)
    cluster.wait_for_nodes()

    global_state_accessor = make_global_state_accessor(head_context)
    node_table = global_state_accessor.get_node_table()
    assert len(node_table) == 2
    for node in node_table:
        if node["NodeID"] == head_context.address_info["node_id"]:
            assert node["NodeName"] == "head_node"
        else:
            assert node["NodeName"] == "worker_node"

    global_state_accessor.disconnect()
    ray.shutdown()
    cluster.shutdown()


def test_node_name_init():
    # Test ray.init with _node_name directly
    new_head_context = ray.init(_node_name="new_head_node", include_dashboard=False)

    global_state_accessor = make_global_state_accessor(new_head_context)
    node = global_state_accessor.get_node_table()[0]
    assert node["NodeName"] == "new_head_node"
    ray.shutdown()


def test_no_node_name():
    # Test that starting ray with no node name will result in a node_name=ip_address
    new_head_context = ray.init(include_dashboard=False)
    global_state_accessor = make_global_state_accessor(new_head_context)
    node = global_state_accessor.get_node_table()[0]
    assert node["NodeName"] == ray.util.get_node_ip_address()
    ray.shutdown()


@pytest.mark.parametrize("max_shapes", [0, 2, -1])
def test_load_report(shutdown_only, max_shapes):
    resource1 = "A"
    resource2 = "B"
    cluster = ray.init(
        num_cpus=1,
        resources={resource1: 1},
        _system_config={
            "max_resource_shapes_per_load_report": max_shapes,
        },
    )

    global_state_accessor = make_global_state_accessor(cluster)

    @ray.remote
    def sleep():
        time.sleep(1000)

    sleep.remote()
    for _ in range(3):
        sleep.remote()
        sleep.options(resources={resource1: 1}).remote()
        sleep.options(resources={resource2: 1}).remote()

    class Checker:
        def __init__(self):
            self.report = None

        def check_load_report(self):
            message = global_state_accessor.get_all_resource_usage()
            if message is None:
                return False

            resource_usage = gcs_utils.ResourceUsageBatchData.FromString(message)
            self.report = resource_usage.resource_load_by_shape.resource_demands
            if max_shapes == 0:
                return True
            elif max_shapes == 2:
                return len(self.report) >= 2
            else:
                return len(self.report) >= 3

    # Wait for load information to arrive.
    checker = Checker()
    wait_for_condition(checker.check_load_report)

    # Check that we respect the max shapes limit.
    if max_shapes != -1:
        assert len(checker.report) <= max_shapes

    print(checker.report)

    if max_shapes > 0:
        # Check that we differentiate between infeasible and ready tasks.
        for demand in checker.report:
            if resource2 in demand.shape:
                assert demand.num_infeasible_requests_queued > 0
                assert demand.num_ready_requests_queued == 0
            else:
                assert demand.num_ready_requests_queued > 0
                assert demand.num_infeasible_requests_queued == 0
    global_state_accessor.disconnect()


def test_placement_group_load_report(ray_start_cluster):
    cluster = ray_start_cluster
    # Add a head node that doesn't have gpu resource.
    cluster.add_node(num_cpus=4)

    global_state_accessor = make_global_state_accessor(
        ray.init(address=cluster.address)
    )

    class PgLoadChecker:
        def nothing_is_ready(self):
            resource_usage = self._read_resource_usage()
            if not resource_usage:
                return False
            if resource_usage.HasField("placement_group_load"):
                pg_load = resource_usage.placement_group_load
                return len(pg_load.placement_group_data) == 2
            return False

        def only_first_one_ready(self):
            resource_usage = self._read_resource_usage()
            if not resource_usage:
                return False
            if resource_usage.HasField("placement_group_load"):
                pg_load = resource_usage.placement_group_load
                return len(pg_load.placement_group_data) == 1
            return False

        def two_infeasible_pg(self):
            resource_usage = self._read_resource_usage()
            if not resource_usage:
                return False
            if resource_usage.HasField("placement_group_load"):
                pg_load = resource_usage.placement_group_load
                return len(pg_load.placement_group_data) == 2
            return False

        def _read_resource_usage(self):
            message = global_state_accessor.get_all_resource_usage()
            if message is None:
                return False

            resource_usage = gcs_utils.ResourceUsageBatchData.FromString(message)
            return resource_usage

    checker = PgLoadChecker()

    # Create 2 placement groups that are infeasible.
    pg_feasible = ray.util.placement_group([{"A": 1}])
    pg_infeasible = ray.util.placement_group([{"B": 1}])
    _, unready = ray.wait([pg_feasible.ready(), pg_infeasible.ready()], timeout=0)
    assert len(unready) == 2
    wait_for_condition(checker.nothing_is_ready)

    # Add a node that makes pg feasible. Make sure load include this change.
    cluster.add_node(resources={"A": 1})
    ray.get(pg_feasible.ready())
    wait_for_condition(checker.only_first_one_ready)
    # Create one more infeasible pg and make sure load is properly updated.
    pg_infeasible_second = ray.util.placement_group([{"C": 1}])
    _, unready = ray.wait([pg_infeasible_second.ready()], timeout=0)
    assert len(unready) == 1
    wait_for_condition(checker.two_infeasible_pg)
    global_state_accessor.disconnect()


def test_backlog_report(shutdown_only):
    cluster = ray.init(
        num_cpus=1,
        _system_config={"max_pending_lease_requests_per_scheduling_category": 1},
    )

    global_state_accessor = make_global_state_accessor(cluster)

    @ray.remote(num_cpus=1)
    def foo(x):
        print(".")
        time.sleep(x)
        return None

    def backlog_size_set():
        message = global_state_accessor.get_all_resource_usage()
        if message is None:
            return False

        resource_usage = gcs_utils.ResourceUsageBatchData.FromString(message)
        aggregate_resource_load = resource_usage.resource_load_by_shape.resource_demands
        if len(aggregate_resource_load) == 1:
            backlog_size = aggregate_resource_load[0].backlog_size
            print(backlog_size)
            # Ideally we'd want to assert backlog_size == 8, but guaranteeing
            # the order the order that submissions will occur is too
            # hard/flaky.
            return backlog_size > 0
        return False

    # We want this first task to finish
    refs = [foo.remote(0.5)]
    # These tasks should all start _before_ the first one finishes.
    refs.extend([foo.remote(1000) for _ in range(9)])
    # Now there's 1 request running, 1 queued in the raylet, and 8 queued in
    # the worker backlog.

    ray.get(refs[0])
    # First request finishes, second request is now running, third lease
    # request is sent to the raylet with backlog=7

    wait_for_condition(backlog_size_set, timeout=2)
    global_state_accessor.disconnect()


def test_default_load_reports(shutdown_only):
    """Despite the fact that default actors release their cpu after being
    placed, they should still require 1 CPU for laod reporting purposes.
    https://github.com/ray-project/ray/issues/26806
    """
    cluster = ray.init(
        num_cpus=0,
    )

    global_state_accessor = make_global_state_accessor(cluster)

    @ray.remote
    def foo():
        return None

    @ray.remote
    class Foo:
        pass

    def actor_and_task_queued_together():
        message = global_state_accessor.get_all_resource_usage()
        if message is None:
            return False

        resource_usage = gcs_utils.ResourceUsageBatchData.FromString(message)
        aggregate_resource_load = resource_usage.resource_load_by_shape.resource_demands
        print(f"Num shapes {len(aggregate_resource_load)}")
        if len(aggregate_resource_load) == 1:
            num_infeasible = aggregate_resource_load[0].num_infeasible_requests_queued
            print(f"num in shape {num_infeasible}")
            # Ideally we'd want to assert backlog_size == 8, but guaranteeing
            # the order the order that submissions will occur is too
            # hard/flaky.
            return num_infeasible == 2
        return False

    # Assign to variables to keep the ref counter happy.
    handle = Foo.remote()
    ref = foo.remote()

    wait_for_condition(actor_and_task_queued_together, timeout=2)
    global_state_accessor.disconnect()

    # Do something with the variables so lint is happy.
    del handle
    del ref


def test_heartbeat_ip(shutdown_only):
    cluster = ray.init(num_cpus=1)
    global_state_accessor = make_global_state_accessor(cluster)
    self_ip = ray.util.get_node_ip_address()

    def self_ip_is_set():
        message = global_state_accessor.get_all_resource_usage()
        if message is None:
            return False

        resource_usage = gcs_utils.ResourceUsageBatchData.FromString(message)
        resources_data = resource_usage.batch[0]
        return resources_data.node_manager_address == self_ip

    wait_for_condition(self_ip_is_set, timeout=2)
    global_state_accessor.disconnect()


def test_next_job_id(ray_start_regular):
    job_id_1 = ray._private.state.next_job_id()
    job_id_2 = ray._private.state.next_job_id()
    assert job_id_1.int() + 1 == job_id_2.int()


def test_get_cluster_config(shutdown_only):
    ray.init(num_cpus=1)
    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)

    cluster_config = ray._private.state.state.get_cluster_config()
    assert cluster_config is None

    cluster_config = autoscaler_pb2.ClusterConfig()
    cluster_config.max_resources["CPU"] = 100
    node_group_config = autoscaler_pb2.NodeGroupConfig()
    node_group_config.name = "m5.large"
    node_group_config.resources["CPU"] = 5
    node_group_config.max_count = -1
    cluster_config.node_group_configs.append(node_group_config)
    gcs_client.report_cluster_config(cluster_config.SerializeToString())
    assert ray._private.state.state.get_cluster_config() == cluster_config


@pytest.mark.parametrize(
    "description, cluster_config, num_cpu",
    [
        (
            "should return 0 since empty config is provided",
            autoscaler_pb2.ClusterConfig(),
            0,
        ),
        (
            "should return 0 since no node_group_config is provided",
            autoscaler_pb2.ClusterConfig(
                max_resources={"CPU": 100},
            ),
            0,
        ),
        (
            "should return 0 since no CPU is provided under node_group_configs",
            autoscaler_pb2.ClusterConfig(
                max_resources={"CPU": 100},
                node_group_configs=[autoscaler_pb2.NodeGroupConfig(name="m5.large")],
            ),
            0,
        ),
        (
            "should return None since 0 instance is provided under node_group_configs",
            autoscaler_pb2.ClusterConfig(
                max_resources={"CPU": 100},
                node_group_configs=[
                    autoscaler_pb2.NodeGroupConfig(
                        resources={"CPU": 50},
                        name="m5.large",
                        max_count=0,
                    )
                ],
            ),
            0,
        ),
        (
            "should return max since max_count=-1 under node_group_configs",
            autoscaler_pb2.ClusterConfig(
                max_resources={"CPU": 100},
                node_group_configs=[
                    autoscaler_pb2.NodeGroupConfig(
                        resources={"CPU": 50},
                        name="m5.large",
                        max_count=-1,
                    )
                ],
            ),
            sys.maxsize,
        ),
        (
            "should return the total under node_group_configs since it is less than max_resources",
            autoscaler_pb2.ClusterConfig(
                max_resources={"CPU": 100},
                node_group_configs=[
                    autoscaler_pb2.NodeGroupConfig(
                        resources={"CPU": 50},
                        name="m5.large",
                        max_count=1,
                    )
                ],
            ),
            50,
        ),
        (
            "should return the total under max_resources since it is less than node_group_configs total",
            autoscaler_pb2.ClusterConfig(
                max_resources={"CPU": 30},
                node_group_configs=[
                    autoscaler_pb2.NodeGroupConfig(
                        resources={"CPU": 50},
                        name="m5.large",
                        max_count=1,
                    )
                ],
            ),
            30,
        ),
        (
            "should return the total under node_group_configs - no max_resources",
            autoscaler_pb2.ClusterConfig(
                node_group_configs=[
                    autoscaler_pb2.NodeGroupConfig(
                        resources={"CPU": 50},
                        name="m5.large",
                        max_count=1,
                    )
                ],
            ),
            50,
        ),
        (
            "should return the total under node_group_configs - multiple node_group_config",
            autoscaler_pb2.ClusterConfig(
                node_group_configs=[
                    autoscaler_pb2.NodeGroupConfig(
                        resources={"CPU": 50},
                        name="m5.large",
                        max_count=1,
                    ),
                    autoscaler_pb2.NodeGroupConfig(
                        resources={"CPU": 10},
                        name="m5.small",
                        max_count=4,
                    ),
                ],
            ),
            90,
        ),
    ],
)
def test_get_max_cpus_from_cluster_config(
    shutdown_only,
    description: str,
    cluster_config: autoscaler_pb2.ClusterConfig,
    num_cpu: Optional[int],
):
    ray.init(num_cpus=1)
    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)

    gcs_client.report_cluster_config(cluster_config.SerializeToString())
    max_resources = ray._private.state.state.get_max_resources_from_cluster_config()
    num_cpu_from_max_resources = max_resources.get("CPU", 0) if max_resources else 0
    assert num_cpu_from_max_resources == num_cpu, description


@pytest.mark.parametrize(
    "description, cluster_config, expected_resources",
    [
        (
            "should return CPU/GPU/TPU as None since empty config is provided",
            autoscaler_pb2.ClusterConfig(),
            None,
        ),
        (
            "should return CPU/GPU/TPU as None since no node_group_config is provided",
            autoscaler_pb2.ClusterConfig(
                max_resources={"CPU": 100, "memory": 1000},
            ),
            None,
        ),
        (
            "should return CPU/GPU/TPU plus resources from node_group_configs",
            autoscaler_pb2.ClusterConfig(
                node_group_configs=[
                    autoscaler_pb2.NodeGroupConfig(
                        name="m5.large",
                        resources={"CPU": 50, "memory": 500},
                        max_count=1,
                    )
                ],
            ),
            {"CPU": 50, "memory": 500},
        ),
        (
            "should return resources from both node_group_configs and max_resources",
            autoscaler_pb2.ClusterConfig(
                max_resources={"GPU": 8},
                node_group_configs=[
                    autoscaler_pb2.NodeGroupConfig(
                        name="m5.large",
                        resources={"CPU": 50, "memory": 500},
                        max_count=1,
                    )
                ],
            ),
            {
                "CPU": 50,
                "memory": 500,
            },  # GPU and TPU are None because not in node_group_configs
        ),
        (
            "should return limited by max_resources when node_group total exceeds it",
            autoscaler_pb2.ClusterConfig(
                max_resources={"CPU": 30, "memory": 200},
                node_group_configs=[
                    autoscaler_pb2.NodeGroupConfig(
                        name="m5.large",
                        resources={"CPU": 50, "memory": 500},
                        max_count=1,
                    )
                ],
            ),
            {"CPU": 30, "memory": 200},
        ),
        (
            "should return sys.maxsize when max_count=-1",
            autoscaler_pb2.ClusterConfig(
                node_group_configs=[
                    autoscaler_pb2.NodeGroupConfig(
                        name="m5.large",
                        resources={"CPU": 50, "custom_resource": 10},
                        max_count=-1,
                    )
                ],
            ),
            {
                "CPU": sys.maxsize,
                "custom_resource": sys.maxsize,
            },
        ),
        (
            "should sum across multiple node_group_configs",
            autoscaler_pb2.ClusterConfig(
                node_group_configs=[
                    autoscaler_pb2.NodeGroupConfig(
                        name="m5.large",
                        resources={"CPU": 50, "memory": 500},
                        max_count=1,
                    ),
                    autoscaler_pb2.NodeGroupConfig(
                        name="m5.small",
                        resources={"CPU": 10, "GPU": 1},
                        max_count=4,
                    ),
                ],
            ),
            {
                "CPU": 90,
                "GPU": 4,
                "memory": 500,
            },  # 50 + (10*4), 500 + 0
        ),
        (
            "should return 0 for resources with 0 count or 0 resources",
            autoscaler_pb2.ClusterConfig(
                node_group_configs=[
                    autoscaler_pb2.NodeGroupConfig(
                        name="m5.large",
                        resources={"CPU": 50, "memory": 0},
                        max_count=0,  # This makes all resources None
                    ),
                    autoscaler_pb2.NodeGroupConfig(
                        name="m5.small",
                        resources={"GPU": 1},
                        max_count=2,
                    ),
                ],
            ),
            {
                "CPU": 0,
                "GPU": 2,
                "memory": 0,
            },  # CPU is None due to max_count=0, GPU has valid count
        ),
        (
            "should discover all resource types including custom ones",
            autoscaler_pb2.ClusterConfig(
                max_resources={"TPU": 16, "special_resource": 100},
                node_group_configs=[
                    autoscaler_pb2.NodeGroupConfig(
                        name="gpu-node",
                        resources={
                            "CPU": 32,
                            "GPU": 8,
                            "memory": 1000,
                            "custom_accelerator": 4,
                        },
                        max_count=2,
                    ),
                    autoscaler_pb2.NodeGroupConfig(
                        name="cpu-node",
                        resources={"CPU": 96, "memory": 2000, "disk": 500},
                        max_count=1,
                    ),
                ],
            ),
            {
                "CPU": 160,  # (32*2) + (96*1)
                "GPU": 16,  # (8*2) + 0
                "memory": 4000,  # (1000*2) + (2000*1)
                "custom_accelerator": 8,  # (4*2) + 0
                "disk": 500,  # 0 + (500*1)
            },
        ),
    ],
)
def test_get_max_resources_from_cluster_config(
    shutdown_only,
    description: str,
    cluster_config: autoscaler_pb2.ClusterConfig,
    expected_resources: Dict[str, Optional[int]],
):
    """Test get_max_resources_from_cluster_config method.

    This test verifies that the method correctly:
    1. Always includes CPU/GPU/TPU in the results
    2. Discovers additional resource types from node_group_configs and max_resources
    3. Calculates maximum values for each resource type
    4. Handles edge cases like empty configs, zero counts, unlimited resources
    5. Supports resource types beyond CPU/GPU/TPU
    """
    ray.init(num_cpus=1)
    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)

    gcs_client.report_cluster_config(cluster_config.SerializeToString())
    max_resources = ray._private.state.state.get_max_resources_from_cluster_config()

    assert (
        max_resources == expected_resources
    ), f"{description}\nExpected: {expected_resources}\nActual: {max_resources}"


def test_get_draining_nodes(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node()
    ray.init(address=cluster.address)
    cluster.add_node(resources={"worker1": 1})
    cluster.add_node(resources={"worker2": 1})
    cluster.wait_for_nodes()

    @ray.remote
    def get_node_id():
        return ray.get_runtime_context().get_node_id()

    worker1_node_id = ray.get(get_node_id.options(resources={"worker1": 1}).remote())
    worker2_node_id = ray.get(get_node_id.options(resources={"worker2": 1}).remote())

    # Initially there is no draining node.
    assert ray._private.state.state.get_draining_nodes() == {}

    @ray.remote
    class Actor:
        def ping(self):
            pass

    actor1 = Actor.options(num_cpus=1, resources={"worker1": 1}).remote()
    actor2 = Actor.options(num_cpus=1, resources={"worker2": 1}).remote()
    ray.get(actor1.ping.remote())
    ray.get(actor2.ping.remote())

    gcs_client = GcsClient(address=ray.get_runtime_context().gcs_address)

    # Drain the worker nodes.
    is_accepted, _ = gcs_client.drain_node(
        worker1_node_id,
        autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_PREEMPTION"),
        "preemption",
        2**63 - 2,
    )
    assert is_accepted

    is_accepted, _ = gcs_client.drain_node(
        worker2_node_id,
        autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_PREEMPTION"),
        "preemption",
        0,
    )
    assert is_accepted

    def get_draining_nodes_check():
        draining_nodes = ray._private.state.state.get_draining_nodes()
        if (
            draining_nodes[worker1_node_id] == (2**63 - 2)
            and draining_nodes[worker2_node_id] == 0
        ):
            return True
        else:
            return False

    wait_for_condition(get_draining_nodes_check)

    # Kill the actors running on the draining worker nodes so
    # that the worker nodes become idle and can be drained.
    ray.kill(actor1)
    ray.kill(actor2)

    wait_for_condition(lambda: ray._private.state.state.get_draining_nodes() == {})


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
