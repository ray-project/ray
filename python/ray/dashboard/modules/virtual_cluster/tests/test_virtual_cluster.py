import logging
import os
import socket
import time

import pytest
import requests

import ray
from ray._private.test_utils import (
    format_web_url,
    wait_for_condition,
    wait_until_server_available,
)
from ray.cluster_utils import Cluster
from ray.dashboard.tests.conftest import *  # noqa

logger = logging.getLogger(__name__)


def create_or_update_virtual_cluster(
    webui_url, virtual_cluster_id, allocation_mode, replica_sets, revision
):
    try:
        resp = requests.post(
            webui_url + "/virtual_clusters",
            json={
                "virtualClusterId": virtual_cluster_id,
                "allocationMode": allocation_mode,
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
        virtual_cluster_id, allocation_mode, replica_sets
    ):
        nonlocal revision
        resp = requests.post(
            webui_url + "/virtual_clusters",
            json={
                "virtualClusterId": virtual_cluster_id,
                "allocationMode": allocation_mode,
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

    # Create a new virtual cluster with exclusive allocation mode.
    _check_create_or_update_virtual_cluster(
        virtual_cluster_id="virtual_cluster_1",
        allocation_mode="exclusive",
        replica_sets={"4c8g": 1, "8c16g": 1},
    )

    # Update the virtual cluster with less nodes (scale down).
    _check_create_or_update_virtual_cluster(
        virtual_cluster_id="virtual_cluster_1",
        allocation_mode="exclusive",
        replica_sets={"4c8g": 1},
    )

    # Update the virtual cluster with more nodes (scale up).
    _check_create_or_update_virtual_cluster(
        virtual_cluster_id="virtual_cluster_1",
        allocation_mode="exclusive",
        replica_sets={"4c8g": 1, "8c16g": 1},
    )

    # Update the virtual cluster with zero node (make it empty).
    _check_create_or_update_virtual_cluster(
        virtual_cluster_id="virtual_cluster_1",
        allocation_mode="exclusive",
        replica_sets={},
    )

    # `virtual_cluster_1` has released all nodes, so we can now
    # create a new (mixed) virtual cluster with two nodes.
    _check_create_or_update_virtual_cluster(
        virtual_cluster_id="virtual_cluster_2",
        allocation_mode="mixed",
        replica_sets={"4c8g": 1, "8c16g": 1},
    )

    # Update the virtual cluster with less nodes.
    _check_create_or_update_virtual_cluster(
        virtual_cluster_id="virtual_cluster_2",
        allocation_mode="mixed",
        replica_sets={"4c8g": 1},
    )

    # Update the virtual cluster with more nodes.
    _check_create_or_update_virtual_cluster(
        virtual_cluster_id="virtual_cluster_2",
        allocation_mode="mixed",
        replica_sets={"4c8g": 1, "8c16g": 1},
    )

    # Update the virtual cluster with zero node (make it empty).
    _check_create_or_update_virtual_cluster(
        virtual_cluster_id="virtual_cluster_2", allocation_mode="mixed", replica_sets={}
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
@pytest.mark.parametrize("allocation_mode", ["exclusive", "mixed"])
def test_create_and_update_virtual_cluster_with_exceptions(
    disable_aiohttp_cache, ray_start_cluster_head, allocation_mode
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
        allocation_mode=allocation_mode,
        replica_sets={"16c32g": 1},
        revision=0,
    )
    assert result["result"] is False
    assert "No enough nodes to add to the virtual cluster" in result["msg"]
    replica_sets = result["data"].get("replicaSetsToRecommend", {})
    # The primary cluster can fulfill none `16c32g` node to meet the
    # virtual cluster's requirement.
    assert replica_sets == {}

    # Create a new virtual cluster with node count that the primary cluster
    # can not provide.
    result = create_or_update_virtual_cluster(
        webui_url=webui_url,
        virtual_cluster_id="virtual_cluster_1",
        allocation_mode=allocation_mode,
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
        allocation_mode=allocation_mode,
        replica_sets={"4c8g": 1},
        revision=0,
    )
    assert result["result"] is True
    revision = result["data"]["revision"]

    # Update the virtual cluster with an expired revision.
    result = create_or_update_virtual_cluster(
        webui_url=webui_url,
        virtual_cluster_id="virtual_cluster_1",
        allocation_mode=allocation_mode,
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
        allocation_mode=allocation_mode,
        replica_sets={"4c8g": 2, "8c16g": 2},
        revision=revision,
    )
    assert result["result"] is False
    assert "No enough nodes to add to the virtual cluster" in result["msg"]
    replica_sets = result["data"].get("replicaSetsToRecommend", {})
    # The primary cluster can only fulfill one `8c16g`
    # node to meet the virtual cluster's requirement.
    assert replica_sets == {"8c16g": 1}

    if allocation_mode == "mixed":
        actor = SmallActor.options(resources={"4c8g": 1}).remote()
        ray.get(actor.pid.remote(), timeout=10)

        # Update (scale down) the virtual cluster with one node in use.
        result = create_or_update_virtual_cluster(
            webui_url=webui_url,
            virtual_cluster_id="virtual_cluster_1",
            allocation_mode=allocation_mode,
            replica_sets={},
            revision=revision,
        )
        assert result["result"] is False
        assert "No enough nodes to remove from the virtual cluster" in result["msg"]
        replica_sets = result["data"].get("replicaSetsToRecommend", {})
        # The virtual cluster has one `4c8g` node in use. So we can fulfill none node.
        assert replica_sets == {}

    # Create a new virtual cluster that the remaining nodes in the primary cluster
    # are not enough.
    result = create_or_update_virtual_cluster(
        webui_url=webui_url,
        virtual_cluster_id="virtual_cluster_2",
        allocation_mode=allocation_mode,
        replica_sets={"4c8g": 1, "8c16g": 1},
        revision=0,
    )
    assert result["result"] is False
    assert "No enough nodes to add to the virtual cluster" in result["msg"]
    replica_sets = result["data"].get("replicaSetsToRecommend", {})
    # The primary cluster lacks one `4c8g` node to meet the
    # virtual cluster's requirement.
    assert replica_sets == {"8c16g": 1}


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

    # Create a new virtual cluster with exclusive allocation mode.
    result = create_or_update_virtual_cluster(
        webui_url=webui_url,
        virtual_cluster_id="virtual_cluster_1",
        allocation_mode="exclusive",
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

    # Create a new virtual cluster with mixed mode.
    result = create_or_update_virtual_cluster(
        webui_url=webui_url,
        virtual_cluster_id="virtual_cluster_2",
        allocation_mode="mixed",
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

    # Create a new virtual cluster with mixed allocation mode and
    # two `4c8g` nodes.
    result = create_or_update_virtual_cluster(
        webui_url=webui_url,
        virtual_cluster_id="virtual_cluster_1",
        allocation_mode="mixed",
        replica_sets={"4c8g": 2},
        revision=0,
    )
    assert result["result"] is True

    # Create a new virtual cluster with exclusive allocation mode
    # and two `8c16g` nodes.
    result = create_or_update_virtual_cluster(
        webui_url=webui_url,
        virtual_cluster_id="virtual_cluster_2",
        allocation_mode="exclusive",
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
                    assert virtual_cluster["allocationMode"] == "mixed"
                    assert len(virtual_cluster["nodeInstances"]) == 2
                    for _, node_instance in virtual_cluster["nodeInstances"].items():
                        assert node_instance["hostname"] == hostname
                        assert node_instance["templateId"] == "4c8g"
                    revision_1 = virtual_cluster["revision"]
                    assert revision_1 > 0
                elif virtual_cluster["virtualClusterId"] == "virtual_cluster_2":
                    assert virtual_cluster["allocationMode"] == "exclusive"
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
