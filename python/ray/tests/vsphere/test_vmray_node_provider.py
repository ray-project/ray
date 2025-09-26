import copy
import sys
import threading
from unittest.mock import MagicMock, patch

import pytest

from ray.autoscaler._private.vsphere.node_provider import VsphereWcpNodeProvider
from ray.autoscaler.tags import (
    STATUS_SETTING_UP,
    TAG_RAY_CLUSTER_NAME,
    TAG_RAY_NODE_NAME,
    TAG_RAY_NODE_STATUS,
)

_CLUSTER_NAME = "test"
_PROVIDER_CONFIG = {
    "vsphere_config": {
        "namespace": "test",
        "ca_cert": "",
        "api_server": "10.10.10.10",
    }
}


def mock_vmray_node_provider():
    def __init__(self, provider_config: dict, cluster_name: str):
        self.cluster_name = cluster_name
        self.tag_cache = {}
        self.vsphere_config = provider_config["vsphere_config"]
        self.client = MagicMock()

    with patch.object(VsphereWcpNodeProvider, "__init__", __init__):
        node_provider = VsphereWcpNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)
    return copy.deepcopy(node_provider)


def test_non_terminated_nodes():
    """Should return list of running nodes and update tag_cache"""
    vnp = mock_vmray_node_provider()
    vnp.tag_cache_lock = threading.Lock()
    tag_filters = {
        "ray-node-name": "node_1",
        "ray-node-type": "worker",
        "ray-user-node-type": "ray.head.default",
    }
    tag_cache = {"node_1": tag_filters}
    nodes = ["node_1"]
    vnp.client.list_vms = MagicMock(return_value=(nodes, tag_cache))
    non_terminated_nodes = vnp.non_terminated_nodes(tag_filters)
    assert len(non_terminated_nodes) == 1
    assert len(vnp.tag_cache) == 1


def test_is_running():
    """Should return true if a node is not in RUNNING state"""
    vnp = mock_vmray_node_provider()
    vnp.client.is_vm_power_on.return_value = True
    assert vnp.is_running("test_node") is True


def test_is_terminated():
    """Should return true if a cached node is not in POWERED_ON state"""
    vnp = mock_vmray_node_provider()
    vnp.client.is_vm_power_on.return_value = False
    vnp.client.is_vm_creating.return_value = False
    is_terminated = vnp.is_terminated("node1")
    assert is_terminated is True


def test_node_tags():
    """Should return cached tags of a node"""
    vnp = mock_vmray_node_provider()
    vnp.tag_cache_lock = threading.Lock()
    vnp.tag_cache = {
        "test_vm_id_1": {
            "ray-cluster-name": "test",
            "ray-launch-config": "test_id",
            "ray-node-type": "head",
            "ray-node-name": "test-node",
        }
    }

    tags = vnp.node_tags("test_vm_id_1")
    assert tags == vnp.tag_cache["test_vm_id_1"]


def test_external_ip():
    """Should return external ip of a node"""
    vnp = mock_vmray_node_provider()
    vnp.client.get_vm_external_ip.return_value = "10.10.10.10"
    assert vnp.external_ip("test_node") == "10.10.10.10"


def test_internal_ip():
    """Should return external ip of a node"""
    vnp = mock_vmray_node_provider()
    vnp.client.get_vm_external_ip.return_value = "10.10.10.10"
    assert vnp.internal_ip("test_node") == "10.10.10.10"


def test_set_node_tags():
    """Should update old tags with new ones"""
    vnp = mock_vmray_node_provider()
    vnp.tag_cache_lock = threading.Lock()
    vnp.tag_cache = {"vm1": {"tag1": ""}, "vm2": {"tag1": "tag2"}}
    new_tags = {"tag1": "new_tag"}
    vnp.set_node_tags("vm1", new_tags)
    assert vnp.tag_cache["vm1"]["tag1"] == "new_tag"
    assert vnp.tag_cache["vm2"]["tag1"] == "tag2"


def test_create_node():
    vnp = mock_vmray_node_provider()
    vnp.tag_cache_lock = threading.Lock()
    node_config = {}

    def mock_create_node():
        created_nodes_dict = {"vm-1": "vm-1", "vm-2": "vm-2"}
        yield created_nodes_dict
        # for mock_node in created_nodes_dict:
        #     mock_node.vm = "vm-test"
        #     yield mock_node

    # vnp.create_instant_clone_node = MagicMock(
    #     side_effect=mock_create_node()
    # )
    vnp.client.create_nodes = MagicMock(side_effect=mock_create_node())

    created_nodes_dict = vnp.create_node(
        node_config,
        {
            "ray-node-name": "ray-node-1",
            "ray-node-type": "head",
            "ray-node-status": "uninitialised",
        },
        2,
    )
    assert len(created_nodes_dict) == 2
    assert len(vnp.tag_cache) == 2
    assert vnp.tag_cache["vm-1"][TAG_RAY_NODE_STATUS] == STATUS_SETTING_UP
    assert vnp.tag_cache["vm-1"][TAG_RAY_NODE_NAME] == "vm-1"
    assert vnp.tag_cache["vm-1"][TAG_RAY_CLUSTER_NAME] == vnp.cluster_name

    assert vnp.tag_cache["vm-2"][TAG_RAY_NODE_STATUS] == STATUS_SETTING_UP
    assert vnp.tag_cache["vm-2"][TAG_RAY_NODE_NAME] == "vm-2"
    assert vnp.tag_cache["vm-2"][TAG_RAY_CLUSTER_NAME] == vnp.cluster_name


def test_terminate_node():
    vnp = mock_vmray_node_provider()
    vnp.tag_cache_lock = threading.Lock()
    vnp.tag_cache = {"vm1": ["tag1", "tag2"], "vm2": ["tag1", "tag2"]}
    # If node is None
    vnp.terminate_node(None)
    assert len(vnp.tag_cache) == 2
    # If node is still getting created so not delete
    vnp.client.is_vm_creating = MagicMock(return_value=True)
    vnp.terminate_node("vm2")
    assert len(vnp.tag_cache) == 2
    # If node is either in a running or a failure state then delete it
    vnp.client.is_vm_creating = MagicMock(return_value=False)
    vnp.client.delete_node = MagicMock()
    vnp.terminate_node("vm2")
    assert len(vnp.tag_cache) == 1


def test_terminate_nodes():
    vnp = mock_vmray_node_provider()
    vnp.tag_cache_lock = threading.Lock()
    vnp.tag_cache = {"vm1": ["tag1", "tag2"], "vm2": ["tag1", "tag2"]}
    # If node is None
    vnp.terminate_nodes(None)
    assert len(vnp.tag_cache) == 2
    # If node is still getting created so not delete
    vnp.client.is_vm_creating = MagicMock(return_value=True)
    vnp.terminate_nodes(["vm2"])
    assert len(vnp.tag_cache) == 2
    # If node is either in a running or a failure state then delete it
    vnp.client.is_vm_creating = MagicMock(return_value=False)
    vnp.client.delete_node = MagicMock()
    vnp.terminate_nodes(["vm1", "vm2"])
    assert len(vnp.tag_cache) == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
