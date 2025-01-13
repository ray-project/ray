import logging
import os
import socket
import tempfile
import time

import pytest
import requests

import ray
from ray._private.ray_constants import DEFAULT_DASHBOARD_AGENT_LISTEN_PORT
from ray._private.test_utils import (
    format_web_url,
    get_resource_usage,
    wait_for_condition,
    wait_until_server_available,
)
from ray.cluster_utils import Cluster
from ray.dashboard.tests.conftest import *  # noqa
from ray.job_submission import JobStatus, JobSubmissionClient

logger = logging.getLogger(__name__)


def create_or_update_virtual_cluster(
    webui_url, virtual_cluster_id, divisible, replica_sets, revision
):
    try:
        resp = requests.post(
            webui_url + "/virtual_clusters",
            json={
                "virtualClusterId": virtual_cluster_id,
                "divisible": divisible,
                "replicaSets": replica_sets,
                "revision": revision,
            },
            timeout=10,
        )
        result = resp.json()
        print(result)
        return result
    except Exception as ex:
        logger.info(ex)


def remove_virtual_cluster(webui_url, virtual_cluster_id):
    try:
        resp = requests.delete(
            webui_url + "/virtual_clusters/" + virtual_cluster_id, timeout=10
        )
        result = resp.json()
        print(result)
        return result
    except Exception as ex:
        logger.info(ex)


@ray.remote
class SmallActor:
    def pid(self):
        return os.getpid()


@pytest.mark.parametrize(
    "ray_start_cluster_head",
    [
        {
            "include_dashboard": True,
        }
    ],
    indirect=True,
)
def test_create_and_update_virtual_cluster(
    disable_aiohttp_cache, ray_start_cluster_head
):
    cluster: Cluster = ray_start_cluster_head
    assert wait_until_server_available(cluster.webui_url) is True
    webui_url = cluster.webui_url
    webui_url = format_web_url(webui_url)

    # Add two nodes to the primary cluster.
    cluster.add_node(env_vars={"RAY_NODE_TYPE_NAME": "4c8g"})
    cluster.add_node(env_vars={"RAY_NODE_TYPE_NAME": "8c16g"})
    hostname = socket.gethostname()
    revision = 0

    def _check_create_or_update_virtual_cluster(
        virtual_cluster_id, divisible, replica_sets
    ):
        nonlocal revision
        resp = requests.post(
            webui_url + "/virtual_clusters",
            json={
                "virtualClusterId": virtual_cluster_id,
                "divisible": divisible,
                "replicaSets": replica_sets,
                "revision": revision,
            },
            timeout=10,
        )
        resp.raise_for_status()
        result = resp.json()
        print(result)
        assert result["result"] is True, resp.text
        assert result["data"]["virtualClusterId"] == virtual_cluster_id
        current_revision = result["data"]["revision"]
        assert current_revision > revision
        revision = current_revision
        virtual_cluster_replica_sets = {}
        for _, node_instance in result["data"]["nodeInstances"].items():
            assert node_instance["hostname"] == hostname
            virtual_cluster_replica_sets[node_instance["templateId"]] = (
                virtual_cluster_replica_sets.get(node_instance["templateId"], 0) + 1
            )
        # The virtual cluster has the same node types and count as expected.
        assert replica_sets == virtual_cluster_replica_sets

    # Create a new divisible virtual cluster.
    _check_create_or_update_virtual_cluster(
        virtual_cluster_id="virtual_cluster_1",
        divisible=True,
        replica_sets={"4c8g": 1, "8c16g": 1},
    )

    # Update the virtual cluster with less nodes (scale down).
    _check_create_or_update_virtual_cluster(
        virtual_cluster_id="virtual_cluster_1",
        divisible=True,
        replica_sets={"4c8g": 1},
    )

    # Update the virtual cluster with more nodes (scale up).
    _check_create_or_update_virtual_cluster(
        virtual_cluster_id="virtual_cluster_1",
        divisible=True,
        replica_sets={"4c8g": 1, "8c16g": 1},
    )

    # Update the virtual cluster with zero node (make it empty).
    _check_create_or_update_virtual_cluster(
        virtual_cluster_id="virtual_cluster_1",
        divisible=True,
        replica_sets={},
    )

    # `virtual_cluster_1` has released all nodes, so we can now
    # create a new indivisible virtual cluster with two nodes.
    _check_create_or_update_virtual_cluster(
        virtual_cluster_id="virtual_cluster_2",
        divisible=False,
        replica_sets={"4c8g": 1, "8c16g": 1},
    )

    # Update the virtual cluster with less nodes.
    _check_create_or_update_virtual_cluster(
        virtual_cluster_id="virtual_cluster_2",
        divisible=False,
        replica_sets={"4c8g": 1},
    )

    # Update the virtual cluster with more nodes.
    _check_create_or_update_virtual_cluster(
        virtual_cluster_id="virtual_cluster_2",
        divisible=False,
        replica_sets={"4c8g": 1, "8c16g": 1},
    )

    # Update the virtual cluster with zero node (make it empty).
    _check_create_or_update_virtual_cluster(
        virtual_cluster_id="virtual_cluster_2", divisible=False, replica_sets={}
    )


@pytest.mark.parametrize(
    "ray_start_cluster_head",
    [
        {
            "include_dashboard": True,
        }
    ],
    indirect=True,
)
@pytest.mark.parametrize("divisible", [True, False])
def test_create_and_update_virtual_cluster_with_exceptions(
    disable_aiohttp_cache, ray_start_cluster_head, divisible
):
    cluster: Cluster = ray_start_cluster_head
    assert wait_until_server_available(cluster.webui_url) is True
    webui_url = cluster.webui_url
    webui_url = format_web_url(webui_url)

    # Add two nodes to the primary cluster.
    cluster.add_node(env_vars={"RAY_NODE_TYPE_NAME": "4c8g"}, resources={"4c8g": 1})
    cluster.add_node(env_vars={"RAY_NODE_TYPE_NAME": "8c16g"}, resources={"8c16g": 1})

    # Create a new virtual cluster with a non-exist node type.
    result = create_or_update_virtual_cluster(
        webui_url=webui_url,
        virtual_cluster_id="virtual_cluster_1",
        divisible=divisible,
        replica_sets={"16c32g": 1},
        revision=0,
    )
    assert result["result"] is False
    assert "No enough nodes to add to the virtual cluster" in result["msg"]
    replica_sets = result["data"].get("replicaSetsToRecommend", {})
    # The primary cluster can fulfill none `16c32g` node to meet the
    # virtual cluster's requirement.
    assert replica_sets == {"16c32g": 0}

    # Create a new virtual cluster with node count that the primary cluster
    # can not provide.
    result = create_or_update_virtual_cluster(
        webui_url=webui_url,
        virtual_cluster_id="virtual_cluster_1",
        divisible=divisible,
        replica_sets={"4c8g": 2, "8c16g": 1},
        revision=0,
    )
    assert result["result"] is False
    assert "No enough nodes to add to the virtual cluster" in result["msg"]
    replica_sets = result["data"].get("replicaSetsToRecommend", {})
    # The primary cluster can only fulfill one `4c8g` node and one `8c16g` to meet the
    # virtual cluster's requirement.
    assert replica_sets == {"4c8g": 1, "8c16g": 1}

    # Create a new virtual cluster with one `4c8g` node, which shall succeed.
    result = create_or_update_virtual_cluster(
        webui_url=webui_url,
        virtual_cluster_id="virtual_cluster_1",
        divisible=divisible,
        replica_sets={"4c8g": 1},
        revision=0,
    )
    assert result["result"] is True
    revision = result["data"]["revision"]

    # Update the virtual cluster with an expired revision.
    result = create_or_update_virtual_cluster(
        webui_url=webui_url,
        virtual_cluster_id="virtual_cluster_1",
        divisible=divisible,
        replica_sets={"4c8g": 2, "8c16g": 2},
        revision=0,
    )
    assert result["result"] is False
    assert "The revision (0) is expired" in str(result["msg"])

    # Update the virtual cluster with node count that the primary cluster
    # can not provide.
    result = create_or_update_virtual_cluster(
        webui_url=webui_url,
        virtual_cluster_id="virtual_cluster_1",
        divisible=divisible,
        replica_sets={"4c8g": 2, "8c16g": 2},
        revision=revision,
    )
    assert result["result"] is False
    assert "No enough nodes to add to the virtual cluster" in result["msg"]
    replica_sets = result["data"].get("replicaSetsToRecommend", {})
    # The primary cluster can only fulfill one `8c16g`
    # node to meet the virtual cluster's requirement.
    assert replica_sets == {"4c8g": 0, "8c16g": 1}

    if not divisible:
        actor = SmallActor.options(resources={"4c8g": 1}).remote()
        ray.get(actor.pid.remote(), timeout=10)

        # Update (scale down) the virtual cluster with one node in use.
        result = create_or_update_virtual_cluster(
            webui_url=webui_url,
            virtual_cluster_id="virtual_cluster_1",
            divisible=divisible,
            replica_sets={},
            revision=revision,
        )
        assert result["result"] is False
        assert "No enough nodes to remove from the virtual cluster" in result["msg"]
        replica_sets = result["data"].get("replicaSetsToRecommend", {})
        # The virtual cluster has one `4c8g` node in use. So we can fulfill none node.
        assert replica_sets == {"4c8g": 0}

    # Create a new virtual cluster that the remaining nodes in the primary cluster
    # are not enough.
    result = create_or_update_virtual_cluster(
        webui_url=webui_url,
        virtual_cluster_id="virtual_cluster_2",
        divisible=divisible,
        replica_sets={"4c8g": 1, "8c16g": 1},
        revision=0,
    )
    assert result["result"] is False
    assert "No enough nodes to add to the virtual cluster" in result["msg"]
    replica_sets = result["data"].get("replicaSetsToRecommend", {})
    # The primary cluster lacks one `4c8g` node to meet the
    # virtual cluster's requirement.
    assert replica_sets == {"4c8g": 0, "8c16g": 1}


@pytest.mark.parametrize(
    "ray_start_cluster_head",
    [
        {
            "include_dashboard": True,
        },
    ],
    indirect=True,
)
def test_remove_virtual_cluster(disable_aiohttp_cache, ray_start_cluster_head):
    cluster: Cluster = ray_start_cluster_head
    assert wait_until_server_available(cluster.webui_url) is True
    webui_url = cluster.webui_url
    webui_url = format_web_url(webui_url)

    cluster.add_node(env_vars={"RAY_NODE_TYPE_NAME": "4c8g"}, resources={"4c8g": 1})
    cluster.add_node(env_vars={"RAY_NODE_TYPE_NAME": "8c16g"}, resources={"8c16g": 1})

    # Create a new divisible virtual cluster.
    result = create_or_update_virtual_cluster(
        webui_url=webui_url,
        virtual_cluster_id="virtual_cluster_1",
        divisible=True,
        replica_sets={"4c8g": 1, "8c16g": 1},
        revision=0,
    )
    assert result["result"] is True

    # Try removing a non-exist virtual cluster.
    result = remove_virtual_cluster(
        webui_url=webui_url, virtual_cluster_id="virtual_cluster_2"
    )
    assert result["result"] is False
    # The error msg should tell us the virtual cluster does not exit.
    assert str(result["msg"]).endswith("does not exist.")

    # Remove the virtual cluster. This will release all nodes.
    result = remove_virtual_cluster(
        webui_url=webui_url, virtual_cluster_id="virtual_cluster_1"
    )
    assert result["result"] is True

    # Create a new indivisible virtual cluster.
    result = create_or_update_virtual_cluster(
        webui_url=webui_url,
        virtual_cluster_id="virtual_cluster_2",
        divisible=False,
        replica_sets={"4c8g": 1, "8c16g": 1},
        revision=0,
    )
    assert result["result"] is True

    # Create an actor that requires some resources.
    actor = SmallActor.options(num_cpus=1, resources={"4c8g": 1}).remote()
    ray.get(actor.pid.remote(), timeout=10)

    # Remove the virtual cluster.
    result = remove_virtual_cluster(
        webui_url=webui_url, virtual_cluster_id="virtual_cluster_2"
    )
    # The virtual cluster can not be removed because some nodes
    # are still in use.
    assert result["result"] is False
    assert "still in use" in result["msg"]

    ray.kill(actor, no_restart=True)

    # Wait for RESOURCE_VIEW sync message consumed by gcs.
    time.sleep(3)

    # Remove the virtual cluster.
    result = remove_virtual_cluster(
        webui_url=webui_url, virtual_cluster_id="virtual_cluster_2"
    )
    assert result["result"] is True


@pytest.mark.parametrize(
    "ray_start_cluster_head",
    [
        {
            "include_dashboard": True,
        }
    ],
    indirect=True,
)
def test_get_virtual_clusters(disable_aiohttp_cache, ray_start_cluster_head):
    cluster: Cluster = ray_start_cluster_head
    assert wait_until_server_available(cluster.webui_url) is True
    webui_url = cluster.webui_url
    webui_url = format_web_url(webui_url)
    hostname = socket.gethostname()

    # Add two `4c8g` nodes and two `8c16g` nodes to the primary cluster.
    cluster.add_node(env_vars={"RAY_NODE_TYPE_NAME": "4c8g"})
    cluster.add_node(env_vars={"RAY_NODE_TYPE_NAME": "4c8g"})
    cluster.add_node(env_vars={"RAY_NODE_TYPE_NAME": "8c16g"})
    cluster.add_node(env_vars={"RAY_NODE_TYPE_NAME": "8c16g"})

    # Create a new indivisible virtual cluster with two `4c8g` nodes.
    result = create_or_update_virtual_cluster(
        webui_url=webui_url,
        virtual_cluster_id="virtual_cluster_1",
        divisible=False,
        replica_sets={"4c8g": 2},
        revision=0,
    )
    assert result["result"] is True

    # Create a new divisible virtual cluster with two `8c16g` nodes.
    result = create_or_update_virtual_cluster(
        webui_url=webui_url,
        virtual_cluster_id="virtual_cluster_2",
        divisible=True,
        replica_sets={"8c16g": 2},
        revision=0,
    )
    assert result["result"] is True

    def _get_virtual_clusters():
        try:
            resp = requests.get(webui_url + "/virtual_clusters")
            resp.raise_for_status()
            result = resp.json()
            print(result)
            assert result["result"] is True, resp.text
            for virtual_cluster in result["data"]["virtualClusters"]:
                if virtual_cluster["virtualClusterId"] == "virtual_cluster_1":
                    assert virtual_cluster["divisible"] == "false"
                    assert len(virtual_cluster["nodeInstances"]) == 2
                    for _, node_instance in virtual_cluster["nodeInstances"].items():
                        assert node_instance["hostname"] == hostname
                        assert node_instance["templateId"] == "4c8g"
                    revision_1 = virtual_cluster["revision"]
                    assert revision_1 > 0
                elif virtual_cluster["virtualClusterId"] == "virtual_cluster_2":
                    assert virtual_cluster["divisible"] == "true"
                    assert len(virtual_cluster["nodeInstances"]) == 2
                    for _, node_instance in virtual_cluster["nodeInstances"].items():
                        assert node_instance["hostname"] == hostname
                        assert node_instance["templateId"] == "8c16g"
                    revision_2 = virtual_cluster["revision"]
                    assert revision_2 > 0
                else:
                    return False
            # `virtual_cluster_2` should have a more recent revision because it was
            # created later than `virtual_cluster_1`.
            assert revision_2 > revision_1
            return True
        except Exception as ex:
            logger.info(ex)
            return False

    wait_for_condition(_get_virtual_clusters, timeout=10)


# Because raylet is responsible for task scheduling, gcs depends on the resource_view
# sync to get to know the pending/running tasks at each node. If the resource_view sync
# lags, gcs may mistakenly consider one node idle when removing node instances from
# a virtual cluster. In this case, we have to clean up the pending/running tasks at
# the node (just removed). This test makes sure the cleanup is correctly enforced.
@pytest.mark.parametrize(
    "ray_start_cluster_head",
    [
        {
            "include_dashboard": True,
            "_system_config": {
                "gcs_actor_scheduling_enabled": False,
                # Make the resource_view sync message lag.
                "raylet_report_resources_period_milliseconds": 30000,
                "local_node_cleanup_delay_interval_ms": 10,
            },
        }
    ],
    indirect=True,
)
def test_cleanup_tasks_after_removing_node_instance(
    disable_aiohttp_cache, ray_start_cluster_head
):
    cluster: Cluster = ray_start_cluster_head
    ip, _ = cluster.webui_url.split(":")
    agent_address = f"{ip}:{DEFAULT_DASHBOARD_AGENT_LISTEN_PORT}"
    assert wait_until_server_available(agent_address)
    assert wait_until_server_available(cluster.webui_url)
    webui_url = cluster.webui_url
    webui_url = format_web_url(webui_url)

    # Add one 4c8g node to the primary cluster.
    cluster.add_node(env_vars={"RAY_NODE_TYPE_NAME": "4c8g"}, num_cpus=4)
    cluster.wait_for_nodes()

    # Create a virtual cluster with one 4c8g node.
    result = create_or_update_virtual_cluster(
        webui_url=webui_url,
        virtual_cluster_id="virtual_cluster_1",
        divisible=False,
        replica_sets={"4c8g": 1},
        revision=0,
    )
    assert result["result"] is True
    revision = result["data"]["revision"]

    client = JobSubmissionClient(webui_url)
    temp_dir = None
    file_path = None

    try:
        # Create a temporary directory
        temp_dir = tempfile.mkdtemp()

        # Define driver: create two actors, requiring 4 cpus each.
        driver_content = """
import ray
import time

@ray.remote
class SmallActor():
    def __init__(self):
        pass

actors = []
for _ in range(2):
    actors.append(SmallActor.options(num_cpus=4).remote())
time.sleep(600)
        """

        # Create a temporary Python file.
        file_path = os.path.join(temp_dir, "test_driver.py")

        with open(file_path, "w") as file:
            file.write(driver_content)

        absolute_path = os.path.abspath(file_path)

        # Submit the job to the virtual cluster.
        job = client.submit_job(
            entrypoint=f"python {absolute_path}",
            virtual_cluster_id="virtual_cluster_1",
        )

        def check_job_running():
            status = client.get_job_status(job)
            return status == JobStatus.RUNNING

        wait_for_condition(check_job_running)

        def check_actors():
            actors = ray._private.state.actors()
            # There is only one 4c8g node in the virtual cluster, we shall see
            # one alive actor and one actor pending creation.
            expected_states = {"ALIVE": 1, "PENDING_CREATION": 1}
            actor_states = {}
            for _, actor in actors.items():
                if actor["ActorClassName"] == "SmallActor":
                    actor_states[actor["State"]] = (
                        actor_states.get(actor["State"], 0) + 1
                    )
            if actor_states == expected_states:
                return True
            return False

        wait_for_condition(check_actors)

        # Scale down the virtual cluster, removing one node instance.
        result = create_or_update_virtual_cluster(
            webui_url=webui_url,
            virtual_cluster_id="virtual_cluster_1",
            divisible=False,
            replica_sets={"4c8g": 0},
            revision=revision,
        )
        # Because resource_view sync is lagging, the node instance was
        # successfully removed.
        assert result["result"] is True

        def check_actors_after_update():
            actors = ray._private.state.actors()
            # If the node (just removed from the virtual cluster) cleans up
            # its pending and running tasks, we shall see two dead actors now.
            expected_states = {"DEAD": 2}
            actor_states = {}
            for _, actor in actors.items():
                if actor["ActorClassName"] == "SmallActor":
                    actor_states[actor["State"]] = (
                        actor_states.get(actor["State"], 0) + 1
                    )
            if actor_states == expected_states:
                return True
            return False

        wait_for_condition(check_actors_after_update)

        def check_running_and_pending_tasks():
            resources_batch = get_resource_usage(
                gcs_address=cluster.head_node.gcs_address
            )
            # Check each node's resource usage, making sure no running
            # or pending tasks left.
            for node in resources_batch.batch:
                if "CPU" not in node.resources_available:
                    return False
                if (
                    len(node.resource_load_by_shape.resource_demands) > 0
                    and node.resource_load_by_shape.resource_demands[
                        0
                    ].num_ready_requests_queued
                    > 0
                ):
                    return False
            return True

        wait_for_condition(check_running_and_pending_tasks)

    finally:
        if file_path:
            os.remove(file_path)
        if temp_dir:
            os.rmdir(temp_dir)


if __name__ == "__main__":
    pass
