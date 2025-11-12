import os
import subprocess
import sys
import time
from typing import Dict

import pytest

import ray
from ray._common.constants import HEAD_NODE_RESOURCE_NAME
from ray._common.test_utils import wait_for_condition
from ray._common.usage.usage_lib import get_extra_usage_tags_to_report
from ray._private.test_utils import run_string_as_driver_nonblocking
from ray._raylet import GcsClient
from ray.autoscaler.v2.sdk import get_cluster_status
from ray.cluster_utils import AutoscalingCluster
from ray.core.generated.usage_pb2 import TagKey
from ray.util.placement_group import (
    placement_group,
    remove_placement_group,
)
from ray.util.state.api import (
    list_actors,
    list_placement_groups,
    list_tasks,
)


def is_head_node_from_resource_usage(usage: Dict[str, float]) -> bool:
    if HEAD_NODE_RESOURCE_NAME in usage:
        return True
    return False


@pytest.mark.parametrize("autoscaler_v2", [False, True], ids=["v1", "v2"])
def test_autoscaler_no_churn(autoscaler_v2):
    num_cpus_per_node = 4
    expected_nodes = 6
    cluster = AutoscalingCluster(
        head_resources={"CPU": num_cpus_per_node},
        worker_node_types={
            "type-1": {
                "resources": {"CPU": num_cpus_per_node},
                "node_config": {},
                "min_workers": 0,
                "max_workers": 2 * expected_nodes,
            },
        },
        autoscaler_v2=autoscaler_v2,
    )

    driver_script = f"""
import time
import ray
@ray.remote(num_cpus=1)
def foo():
  time.sleep(60)
  return True

ray.init("auto")

print("start")
assert(ray.get([foo.remote() for _ in range({num_cpus_per_node * expected_nodes})]))
print("end")
"""

    try:
        cluster.start()
        ray.init("auto")
        gcs_address = ray.get_runtime_context().gcs_address

        def tasks_run():
            tasks = list_tasks()
            # Waiting til the driver in the run_string_as_driver_nonblocking is running
            assert len(tasks) > 0
            return True

        run_string_as_driver_nonblocking(driver_script)
        wait_for_condition(tasks_run)

        reached_threshold = False
        for _ in range(30):
            # verify no pending task + with resource used.
            status = get_cluster_status(gcs_address)
            has_task_demand = len(status.resource_demands.ray_task_actor_demand) > 0

            # Check that we don't overscale
            assert len(status.active_nodes) <= expected_nodes

            # Check there's no demand if we've reached the expected number of nodes
            if reached_threshold:
                assert not has_task_demand

            # Load disappears in the next cycle after we've fully scaled up.
            if len(status.active_nodes) == expected_nodes:
                reached_threshold = True

            time.sleep(1)

        assert reached_threshold
    finally:
        # TODO(rickyx): refactor into a fixture for autoscaling cluster.
        ray.shutdown()
        cluster.shutdown()


# TODO(rickyx): We are NOT able to counter multi-node inconsistency yet. The problem is
# right now, when node A (head node) has an infeasible task,
# node B just finished running previous task.
# the actual cluster view will be:
#   node A: 1 pending task (infeasible)
#   node B: 0 pending task, CPU used = 0
#
# However, when node B's state is not updated on node A, the cluster view will be:
#  node A: 1 pending task (infeasible)
#  node B: 0 pending task, but **CPU used = 1**
#
@pytest.mark.parametrize("mode", (["single_node", "multi_node"]))
@pytest.mark.parametrize("autoscaler_v2", [False, True], ids=["v1", "v2"])
def test_scheduled_task_no_pending_demand(mode, autoscaler_v2):

    # So that head node will need to dispatch tasks to worker node.
    num_head_cpu = 0 if mode == "multi_node" else 1

    cluster = AutoscalingCluster(
        head_resources={"CPU": num_head_cpu},
        worker_node_types={
            "type-1": {
                "resources": {"CPU": 1},
                "node_config": {},
                "min_workers": 0,
                "max_workers": 1,
            },
        },
        autoscaler_v2=autoscaler_v2,
    )

    driver_script = """
import time
import ray
@ray.remote(num_cpus=1)
def foo():
  return True

ray.init("auto")

while True:
    assert(ray.get(foo.remote()))
"""

    try:
        cluster.start()
        ray.init("auto")
        gcs_address = ray.get_runtime_context().gcs_address

        run_string_as_driver_nonblocking(driver_script)

        def tasks_run():
            tasks = list_tasks()

            # Waiting til the driver in the run_string_as_driver_nonblocking is running
            assert len(tasks) > 0

            return True

        wait_for_condition(tasks_run)

        for _ in range(30):
            # verify no pending task + with resource used.
            status = get_cluster_status(gcs_address)
            has_task_demand = len(status.resource_demands.ray_task_actor_demand) > 0
            has_task_usage = False

            for usage in status.cluster_resource_usage:
                if usage.resource_name == "CPU":
                    has_task_usage = usage.used > 0
            print(status.cluster_resource_usage)
            print(status.resource_demands.ray_task_actor_demand)
            assert not (has_task_demand and has_task_usage), status
            time.sleep(0.1)
    finally:
        # TODO(rickyx): refactor into a fixture for autoscaling cluster.
        ray.shutdown()
        cluster.shutdown()


@pytest.mark.parametrize("autoscaler_v2", [False, True], ids=["v1", "v2"])
def test_placement_group_consistent(autoscaler_v2):
    # Test that continuously creating and removing placement groups
    # does not leak pending resource requests.
    import time

    cluster = AutoscalingCluster(
        head_resources={"CPU": 0},
        worker_node_types={
            "type-1": {
                "resources": {"CPU": 1},
                "node_config": {},
                "min_workers": 0,
                "max_workers": 2,
            },
        },
        autoscaler_v2=autoscaler_v2,
    )
    driver_script = """

import ray
import time
# Import placement group APIs.
from ray.util.placement_group import (
    placement_group,
    placement_group_table,
    remove_placement_group,
)

ray.init("auto")

# Reserve all the CPUs of nodes, X= num of cpus, N = num of nodes
while True:
    pg = placement_group([{"CPU": 1}])
    ray.get(pg.ready())
    time.sleep(0.5)
    remove_placement_group(pg)
    time.sleep(0.5)
"""

    try:
        cluster.start()
        ray.init("auto")
        gcs_address = ray.get_runtime_context().gcs_address

        run_string_as_driver_nonblocking(driver_script)

        def pg_created():
            pgs = list_placement_groups()
            assert len(pgs) > 0

            return True

        wait_for_condition(pg_created)

        for _ in range(30):
            # verify no pending request + resource used.
            status = get_cluster_status(gcs_address)
            has_pg_demand = len(status.resource_demands.placement_group_demand) > 0
            has_pg_usage = False
            for usage in status.cluster_resource_usage:
                has_pg_usage = has_pg_usage or "bundle" in usage.resource_name
            print(has_pg_demand, has_pg_usage)
            assert not (has_pg_demand and has_pg_usage), status
            time.sleep(0.1)
    finally:
        ray.shutdown()
        cluster.shutdown()


def test_autoscaler_v2_usage_report():

    # Test that nodes become idle after placement group removal.
    cluster = AutoscalingCluster(
        head_resources={"CPU": 2},
        worker_node_types={
            "type-1": {
                "resources": {"CPU": 2},
                "node_config": {},
                "min_workers": 0,
                "max_workers": 2,
            },
        },
        autoscaler_v2=True,
    )
    try:
        cluster.start()
        ray.init("auto")
        gcs_client = GcsClient(ray.get_runtime_context().gcs_address)

        def verify():
            tags = get_extra_usage_tags_to_report(gcs_client)
            print(tags)
            assert tags[TagKey.Name(TagKey.AUTOSCALER_VERSION).lower()] == "v2", tags
            return True

        wait_for_condition(verify)
    finally:
        ray.shutdown()
        cluster.shutdown()


@pytest.mark.parametrize("autoscaler_v2", [False, True], ids=["v1", "v2"])
def test_placement_group_removal_idle_node(autoscaler_v2):
    # Test that nodes become idle after placement group removal.
    cluster = AutoscalingCluster(
        head_resources={"CPU": 2},
        worker_node_types={
            "type-1": {
                "resources": {"CPU": 2},
                "node_config": {},
                "min_workers": 0,
                "max_workers": 2,
            },
        },
        autoscaler_v2=autoscaler_v2,
    )
    try:
        cluster.start()
        ray.init("auto")
        gcs_address = ray.get_runtime_context().gcs_address

        # Schedule a pg on nodes
        pg = placement_group([{"CPU": 2}] * 3, strategy="STRICT_SPREAD")
        ray.get(pg.ready())

        time.sleep(2)
        remove_placement_group(pg)

        from ray.autoscaler.v2.sdk import get_cluster_status

        def verify():
            cluster_state = get_cluster_status(gcs_address)

            # Verify that nodes are idle.
            assert len((cluster_state.idle_nodes)) == 3
            for node in cluster_state.idle_nodes:
                assert node.node_status == "IDLE"
                assert node.resource_usage.idle_time_ms >= 1000

            return True

        wait_for_condition(verify)
    finally:
        ray.shutdown()
        cluster.shutdown()


@pytest.mark.parametrize("autoscaler_v2", [False, True], ids=["v1", "v2"])
def test_placement_group_reschedule_node_dead(autoscaler_v2):
    # Test autoscaler reschedules placement group when node dies.
    # Note that it should only provision nodes for the bundles that haven't been placed.

    cluster = AutoscalingCluster(
        head_resources={"CPU": 0},
        worker_node_types={
            "type-1": {
                "resources": {"R1": 1},
                "node_config": {},
                "min_workers": 0,
                "max_workers": 2,
            },
            "type-2": {
                "resources": {"R2": 1},
                "node_config": {},
                "min_workers": 0,
                "max_workers": 2,
            },
            "type-3": {
                "resources": {"R3": 1},
                "node_config": {},
                "min_workers": 0,
                "max_workers": 2,
            },
        },
        autoscaler_v2=autoscaler_v2,
    )

    try:
        cluster.start()
        ray.init("auto")
        gcs_address = ray.get_runtime_context().gcs_address

        pg = placement_group([{"R1": 1}, {"R2": 1}, {"R3": 1}])

        ray.get(pg.ready())

        def verify_nodes(active, idle):
            cluster_state = get_cluster_status(gcs_address)
            assert len(cluster_state.active_nodes) == active
            assert len(cluster_state.idle_nodes) == idle
            return True

        # 3 worker nodes, 1 head node (idle)
        wait_for_condition(lambda: verify_nodes(3, 1))

        def kill_node(node_id):
            cmd = f"ps aux | grep {node_id} | grep -v grep | awk '{{print $2}}'"
            pid = subprocess.check_output(cmd, shell=True).decode("utf-8").strip()
            print(f"Killing pid {pid}")
            # kill the pid
            cmd = f"kill -9 {pid}"
            subprocess.check_output(cmd, shell=True)

        # Kill a worker node with 'R1' in resources
        for n in ray.nodes():
            if "R1" in n["Resources"]:
                node = n
                break

        # TODO(mimi): kill_raylet won't trigger reschedule in autoscaler v1
        kill_node(node["NodeID"])

        # Wait for the node to be removed
        wait_for_condition(lambda: verify_nodes(2, 1), 20)

        # Only provision nodes for unplaced bundles;
        # avoid rescheduling the whole placement group.
        wait_for_condition(lambda: verify_nodes(3, 1))

        # Verify that the R1 node is recreated and has a different NodeID.
        assert any(
            [
                "R1" in n["Resources"] and node["NodeID"] != n["NodeID"]
                for n in ray.nodes()
            ]
        ), "R1 node is not recreated."

    finally:
        ray.shutdown()
        cluster.shutdown()


def test_object_store_memory_idle_node(shutdown_only):

    ray.init()

    obj = ray.put("hello")
    gcs_address = ray.get_runtime_context().gcs_address

    def verify():
        state = get_cluster_status(gcs_address)
        for node in state.active_nodes:
            assert node.node_status == "RUNNING"
            assert node.used_resources()["object_store_memory"] > 0
        assert len(state.idle_nodes) == 0
        return True

    wait_for_condition(verify)

    del obj

    import time

    time.sleep(1)

    def verify():
        state = get_cluster_status(gcs_address)
        for node in state.idle_nodes:
            assert node.node_status == "IDLE"
            assert node.used_resources()["object_store_memory"] == 0
            assert node.resource_usage.idle_time_ms >= 1000
        assert len(state.active_nodes) == 0
        return True

    wait_for_condition(verify)


@pytest.mark.parametrize("autoscaler_v2", [False, True], ids=["v1", "v2"])
def test_non_corrupted_resources(autoscaler_v2):
    """
    Test that when node's local gc happens due to object store pressure,
    the message doesn't corrupt the resource view on the gcs.
    See issue https://github.com/ray-project/ray/issues/39644
    """
    num_worker_nodes = 5
    cluster = AutoscalingCluster(
        head_resources={"CPU": 2, "object_store_memory": 100 * 1024 * 1024},
        worker_node_types={
            "type-1": {
                "resources": {"CPU": 2},
                "node_config": {},
                "min_workers": num_worker_nodes,
                "max_workers": num_worker_nodes,
            },
        },
        idle_timeout_minutes=999,
        autoscaler_v2=autoscaler_v2,
    )

    driver_script = """

import ray
import time

ray.init("auto")

@ray.remote(num_cpus=1)
def foo():
    ray.put(bytearray(1024*1024* 50))


while True:
    ray.get([foo.remote() for _ in range(50)])
"""

    try:
        # This should trigger many COMMANDS messages from NodeManager.
        cluster.start(
            _system_config={
                "debug_dump_period_milliseconds": 10,
                "raylet_report_resources_period_milliseconds": 10000,
                "global_gc_min_interval_s": 1,
                "local_gc_interval_s": 1,
                "high_plasma_storage_usage": 0.2,
                "raylet_check_gc_period_milliseconds": 10,
            },
        )
        ctx = ray.init("auto")
        gcs_address = ctx.address_info["gcs_address"]

        from ray.autoscaler.v2.sdk import get_cluster_status

        def nodes_up():
            cluster_state = get_cluster_status(gcs_address)
            assert len(cluster_state.idle_nodes) == num_worker_nodes + 1
            return True

        wait_for_condition(nodes_up)

        # Schedule tasks
        run_string_as_driver_nonblocking(driver_script)
        start = time.time()

        # Check the cluster state for 10 seconds
        while time.time() - start < 10:
            cluster_state = get_cluster_status(gcs_address)

            # Verify total cluster resources never change
            assert (
                len(cluster_state.idle_nodes) + len(cluster_state.active_nodes)
            ) == num_worker_nodes + 1
            assert cluster_state.total_resources()["CPU"] == 2 * (num_worker_nodes + 1)

    finally:
        ray.shutdown()
        cluster.shutdown()


# Helper function to vaidate that a node's labels satisfy a `label_selector`.
def _verify_node_labels_for_selector(
    node_labels: Dict[str, str], selector: Dict[str, str]
) -> bool:
    for key, value in selector.items():
        node_val = node_labels.get(key)

        if "!in(" in value:
            options_str = value.replace("!in(", "").replace(")", "")
            options = {opt.strip() for opt in options_str.split(",")}
            if node_val in options:
                return False
        elif "in(" in value:
            options_str = value.replace("in(", "").replace(")", "")
            options = {opt.strip() for opt in options_str.split(",")}
            if node_val not in options:
                return False
        elif value.startswith("!"):
            if node_val == value[1:]:
                return False
        else:
            if node_val != value:
                return False
    # If all checks pass for all key-value pairs in the selector, return True.
    return True


@pytest.mark.parametrize("autoscaler_v2", [True])
def test_task_scheduled_on_node_with_label_selector(autoscaler_v2):
    cluster = AutoscalingCluster(
        head_resources={"CPU": 0},
        worker_node_types={
            "node1": {
                "resources": {"CPU": 1},
                "node_config": {},
                "labels": {"accelerator-type": "A100", "market-type": "spot"},
                "min_workers": 0,
                "max_workers": 1,
            },
            "node2": {
                "resources": {"CPU": 1},
                "node_config": {},
                "labels": {
                    "region": "us-east1",
                    "accelerator-type": "TPU",
                    "market-type": "spot",
                },
                "min_workers": 0,
                "max_workers": 1,
            },
            "node3": {
                "resources": {"CPU": 1},
                "node_config": {},
                "labels": {"accelerator-type": "B200", "market-type": "spot"},
                "min_workers": 0,
                "max_workers": 1,
            },
            "node4": {
                "resources": {"CPU": 1},
                "node_config": {},
                "labels": {"market-type": "on-demand", "accelerator-type": "TPU"},
                "min_workers": 0,
                "max_workers": 1,
            },
        },
        idle_timeout_minutes=999,
        autoscaler_v2=autoscaler_v2,
    )

    driver_script = """
import ray
import time

@ray.remote(num_cpus=1)
def labels_task():
    time.sleep(20)
    return True

ray.init("auto")

label_selectors = [
    {"accelerator-type": "A100"},
    {"region": "in(us-east1,me-central1)"},
    {"accelerator-type": "!in(A100,TPU)"},
    {"market-type": "!spot"},
]

results = [
    labels_task.options(name=f"task_{i}", label_selector=sel).remote()
    for i, sel in enumerate(label_selectors)
]
assert all(ray.get(results))
"""

    try:
        cluster.start()
        ray.init("auto")
        gcs_address = ray.get_runtime_context().gcs_address
        expected_nodes = 4

        def all_tasks_submitted():
            return len(list_tasks()) == expected_nodes

        proc = run_string_as_driver_nonblocking(driver_script)
        wait_for_condition(all_tasks_submitted)

        def all_nodes_launched():
            status = get_cluster_status(gcs_address)
            return len(status.active_nodes) == expected_nodes

        wait_for_condition(all_nodes_launched, timeout=30)
        proc.wait(timeout=30)
        assert proc.returncode == 0, "The driver script failed."

        # Validate Tasks are scheduled on nodes with required labels.
        tasks_by_name = {
            task.name: task for task in list_tasks(detail=True) if hasattr(task, "name")
        }
        nodes = {node["NodeID"]: node["Labels"] for node in ray.nodes()}
        task_selectors = {
            "task_0": {"accelerator-type": "A100"},
            "task_1": {"region": "in(me-central1,us-east1)"},
            "task_2": {"accelerator-type": "!in(A100,TPU)"},
            "task_3": {"market-type": "!spot"},
        }

        for task_name, expected_selector in task_selectors.items():
            assert (
                task_name in tasks_by_name
            ), f"Task with name '{task_name}' was not found."
            task = tasks_by_name[task_name]

            # Verify actual label selector from the Task matches the expected.
            actual_selector = task.get("label_selector")
            assert (
                actual_selector is not None
            ), f"Task '{task_name}' did not have a 'label_selector' field."

            assert actual_selector == expected_selector, (
                f"Task '{task_name}' has an incorrect label selector. "
                f"Expected: {expected_selector}, Got: {actual_selector}"
            )

            # Verify Ray node created for Task.
            node_id = task["node_id"]
            assert (
                node_id in nodes
            ), f"Node with ID '{node_id}' for task '{task_name}' was not found."

            # Validate node labels satisfy `label_selector` for Task.
            node_labels = nodes[node_id]
            assert _verify_node_labels_for_selector(
                node_labels, actual_selector
            ), f"Verification failed for task '{task_name}' on node '{node_id}'"

    finally:
        ray.shutdown()
        cluster.shutdown()


@pytest.mark.parametrize("autoscaler_v2", [True])
def test_actor_scheduled_on_node_with_label_selector(autoscaler_v2):
    cluster = AutoscalingCluster(
        head_resources={"CPU": 0},
        worker_node_types={
            "node1": {
                "resources": {"CPU": 1},
                "node_config": {},
                "labels": {"accelerator-type": "A100", "market-type": "spot"},
                "min_workers": 0,
                "max_workers": 1,
            },
            "node2": {
                "resources": {"CPU": 1},
                "node_config": {},
                "labels": {
                    "region": "us-east1",
                    "accelerator-type": "TPU",
                    "market-type": "spot",
                },
                "min_workers": 0,
                "max_workers": 1,
            },
            "node3": {
                "resources": {"CPU": 1},
                "node_config": {},
                "labels": {"accelerator-type": "B200", "market-type": "spot"},
                "min_workers": 0,
                "max_workers": 1,
            },
            "node4": {
                "resources": {"CPU": 1},
                "node_config": {},
                "labels": {"market-type": "on-demand", "accelerator-type": "TPU"},
                "min_workers": 0,
                "max_workers": 1,
            },
        },
        idle_timeout_minutes=999,
        autoscaler_v2=autoscaler_v2,
    )

    driver_script = """
import ray

@ray.remote(num_cpus=1)
class Actor:
    def ready(self):
        return True

ray.init("auto")

label_selectors = [
    {"accelerator-type": "A100"},
    {"region": "in(us-east1,me-central1)"},
    {"accelerator-type": "!in(A100,TPU)"},
    {"market-type": "!spot"},
]

actors = [
    Actor.options(name=f"actor_{i}", label_selector=sel).remote()
    for i, sel in enumerate(label_selectors)
]

ray.get([a.ready.remote() for a in actors])
"""

    try:
        cluster.start()
        ray.init("auto")
        gcs_address = ray.get_runtime_context().gcs_address
        expected_nodes = 4

        def all_actors_submitted():
            return len(list_actors()) == expected_nodes

        proc = run_string_as_driver_nonblocking(driver_script)
        wait_for_condition(all_actors_submitted)

        def all_actors_scheduled():
            # Verify the nodes launched for the Actors are as expected.
            status = get_cluster_status(gcs_address)
            if len(status.active_nodes) != expected_nodes:
                return False

            active_node_types = {
                node.ray_node_type_name for node in status.active_nodes
            }
            expected_node_types = {"node1", "node2", "node3", "node4"}
            return active_node_types == expected_node_types

        # All Actors with label selectors should be scheduled, scaling
        # 4 nodes with the required labels.
        wait_for_condition(all_actors_scheduled, timeout=30)
        proc.wait(timeout=30)
        assert proc.returncode == 0, "The driver script failed to submit actors."

        # Finally, validate the Actors are scheduled on the node with matching labels.
        actors_by_name = {
            actor.name: actor
            for actor in list_actors(detail=True)
            if hasattr(actor, "name")
        }
        nodes = {node["NodeID"]: node["Labels"] for node in ray.nodes()}
        actor_selectors = {
            "actor_0": {"accelerator-type": "A100"},
            "actor_1": {"region": "in(me-central1,us-east1)"},
            "actor_2": {"accelerator-type": "!in(A100,TPU)"},
            "actor_3": {"market-type": "!spot"},
        }

        for actor_name, expected_selector in actor_selectors.items():
            assert (
                actor_name in actors_by_name
            ), f"Actor with name '{actor_name}' was not found."
            actor = actors_by_name[actor_name]

            # Verify actual label selector from the Actor matches the expected.
            actual_selector = actor.get("label_selector")
            assert (
                actual_selector is not None
            ), f"Actor '{actor_name}' did not have a 'label_selector' field."

            assert actual_selector == expected_selector, (
                f"Actor '{actor_name}' has an incorrect label selector. "
                f"Expected: {expected_selector}, Got: {actual_selector}"
            )

            # Verify Ray node created for Actor.
            node_id = actor["node_id"]
            assert (
                node_id in nodes
            ), f"Node with ID '{node_id}' for Actor '{actor_name}' was not found."

            # Validate node labels satisfy `label_selector` for Actor.
            node_labels = nodes[node_id]
            assert _verify_node_labels_for_selector(
                node_labels, actual_selector
            ), f"Verification failed for Actor '{actor_name}' on node '{node_id}'"

    finally:
        ray.shutdown()
        cluster.shutdown()


@pytest.mark.parametrize("autoscaler_v2", [True])
def test_pg_scheduled_on_node_with_bundle_label_selector(autoscaler_v2):
    cluster = AutoscalingCluster(
        head_resources={"CPU": 0},
        worker_node_types={
            "unlabelled_node": {
                "resources": {"CPU": 1, "GPU": 1, "TPU": 1},
                "node_config": {},
                "min_workers": 0,
                "max_workers": 1,
            },
            "not_matching_labels": {
                "resources": {"CPU": 1},
                "labels": {"unrelated": "labels"},
                "node_config": {},
                "min_workers": 0,
                "max_workers": 1,
            },
            "a100_node": {
                "resources": {"CPU": 1, "GPU": 1},
                "node_config": {},
                "labels": {"accelerator-type": "A100"},
                "min_workers": 0,
                "max_workers": 1,
            },
            "tpu_node": {
                "resources": {"CPU": 1, "TPU": 1},
                "node_config": {},
                "labels": {"accelerator-type": "TPU_V6E"},
                "min_workers": 0,
                "max_workers": 1,
            },
        },
        idle_timeout_minutes=999,
        autoscaler_v2=autoscaler_v2,
    )

    try:
        cluster.start()
        ray.init("auto")
        gcs_address = ray.get_runtime_context().gcs_address
        # We expect one GPU and one TPU node to scale.
        expected_nodes = 2

        # Define a placement group where each bundle should scale a node of a different type.
        pg = placement_group(
            name="label_selector_pg",
            bundles=[
                {"CPU": 1},
                {"CPU": 1},
            ],
            bundle_label_selector=[
                {"accelerator-type": "A100"},  # a100_node
                {"accelerator-type": "TPU_V6E"},  # tpu_node
            ],
            strategy="SPREAD",
        )

        # Wait for the placement group to be ready.
        ray.get(pg.ready())

        # Validate the number and types of the auto-scaled nodes are as expected.
        # Add a wait here to avoid flaky test behavior.
        def check_nodes_active():
            status = get_cluster_status(gcs_address)
            return len(status.active_nodes) == expected_nodes

        try:
            wait_for_condition(check_nodes_active, timeout=30, retry_interval_ms=500)
        except Exception as e:
            latest_status = get_cluster_status(gcs_address)
            raise AssertionError(
                f"Timed out waiting for {expected_nodes} active nodes. "
                f"Got: {len(latest_status.active_nodes)}. "
                f"Full status: {latest_status}"
            ) from e

        status = get_cluster_status(gcs_address)
        actual_node_types = {node.ray_node_type_name for node in status.active_nodes}
        expected_node_types = {"a100_node", "tpu_node"}
        assert actual_node_types == expected_node_types

        # Validate the placement group is scheduled to nodes with the required labels.
        pgs = list_placement_groups(detail=True)
        assert len(pgs) == 1
        pg_state = pgs[0]
        bundles_list = pg_state.bundles
        assert (
            bundles_list is not None
        ), "PlacementGroupState did not have a 'bundles' field."

        actual_bundle_selectors = []
        for bundle in bundles_list:
            actual_bundle_selectors.append(bundle["label_selector"])

        expected_bundle_selectors = [
            {"accelerator-type": "A100"},
            {"accelerator-type": "TPU_V6E"},
        ]
        assert actual_bundle_selectors == expected_bundle_selectors, (
            f"Placement group has incorrect bundle selectors. "
            f"Expected: {expected_bundle_selectors}, Got: {actual_bundle_selectors}"
        )

        nodes = {node["NodeID"]: node["Labels"] for node in ray.nodes()}
        for bundle_index, bundle in enumerate(bundles_list):
            # Verify bundle placed on expected node.
            bundle_node_id = bundle.get("node_id")
            assert (
                bundle_node_id in nodes
            ), f"Node with ID '{bundle_node_id}' for bundle {bundle_index} was not found."

            # Verify node's labels satisfy the bundle's label_selector.
            bundle_selector = actual_bundle_selectors[bundle_index]
            node_labels = nodes[bundle_node_id]
            assert _verify_node_labels_for_selector(node_labels, bundle_selector)

    finally:
        ray.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
