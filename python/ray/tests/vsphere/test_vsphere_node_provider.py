import copy
from threading import RLock
import pytest
import threading
from unittest.mock import MagicMock, patch

from ray.autoscaler._private.vsphere.node_provider import VsphereNodeProvider
from com.vmware.vcenter_client import VM
from com.vmware.vcenter.vm_client import Power as HardPower

_CLUSTER_NAME = "test"
_PROVIDER_CONFIG = {
    "vsphere_config": {
        "credentials": {
            "user": "test@vsphere.local",
            "password": "test",
            "server": "10.188.17.50",
        },
        "datacenter": "test",
    }
}


def mock_vsphere_node_provider():
    def __init__(self, provider_config: dict, cluster_name: str):
        self.vsphere_sdk_client = MagicMock()
        self.cluster_name = cluster_name
        self.tag_cache = {}
        self.cached_nodes = {}

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)
    return copy.deepcopy(node_provider)


def test_non_terminated_nodes_returns_no_node():
    """There is no node in vSphere"""

    vnp = mock_vsphere_node_provider()
    vnp.lock = RLock()
    vnp.vsphere_sdk_client.vcenter.VM.list.return_value = []

    nodes = vnp.non_terminated_nodes({})  # assert
    assert len(nodes) == 0


def test_non_terminated_nodes_returns_nodes_in_powered_off_creating_state():
    vnp = mock_vsphere_node_provider()
    vnp.lock = RLock()
    vnp.vsphere_sdk_client.vcenter.VM.list.return_value = [
        MagicMock(vm="vm1"),
        MagicMock(vm="vm2"),
    ]
    vnp.vsphere_sdk_client.vcenter.vm.Power.get.side_effect = [
        MagicMock(state="POWERED_ON"),
        MagicMock(state="POWERED_OFF"),
    ]
    # The 2nd vm is powered off but is in creating status
    vnp.get_matched_tags = MagicMock(
        return_value=(
            {"ray-cluster-name": "test"},
            {
                "ray-cluster-name": "test",
                "custom-tag": "custom-value",
                "vsphere-node-status": "creating",
            },
        )
    )
    # The tag filter is none
    nodes = vnp.non_terminated_nodes({})
    assert len(nodes) == 2


def test_non_terminated_nodes_with_custom_tag_filters():
    """Test nodes with custom tag filters"""
    vnp = mock_vsphere_node_provider()
    vnp.lock = RLock()
    vnp.vsphere_sdk_client.vcenter.VM.list.return_value = [
        MagicMock(vm="vm1"),
        MagicMock(vm="vm2"),
    ]
    vnp.vsphere_sdk_client.vcenter.vm.Power.get.side_effect = [
        MagicMock(state="POWERED_ON"),
        MagicMock(state="POWERED_OFF"),
    ]
    vnp.get_matched_tags = MagicMock(
        return_value=(
            {"ray-cluster-name": "test", "custom-tag": "custom-value"},
            {
                "ray-cluster-name": "test",
                "custom-tag": "custom-value",
                "vsphere-node-status": "blabla",
            },
        )
    )
    # This time we applied a tag filter, but the 2nd vm is not in creating
    # status so only the first vm will be returned
    nodes = vnp.non_terminated_nodes({"custom-tag": "custom-value"})  # assert
    assert len(nodes) == 1
    assert nodes[0] == "vm1"


def test_non_terminated_nodes_with_multiple_filters_not_matching():
    """Test nodes with tag filters not matching"""
    vnp = mock_vsphere_node_provider()
    vnp.lock = RLock()
    vnp.vsphere_sdk_client.vcenter.VM.list.return_value = [
        MagicMock(vm="vm1"),
        MagicMock(vm="vm2"),
    ]
    vnp.vsphere_sdk_client.vcenter.vm.Power.get.side_effect = [
        MagicMock(state="POWERED_ON"),
        MagicMock(state="POWERED_OFF"),
    ]
    vnp.get_matched_tags = MagicMock(
        return_value=(
            {"ray-cluster-name": "test"},
            {"ray-cluster-name": "test", "vsphere-node-status": "blabla"},
        )
    )
    # tag not much so no VM should be returned this time
    nodes = vnp.non_terminated_nodes({"custom-tag": "another-value"})
    assert len(nodes) == 0


def test_is_running():
    """Should return true if a cached node is in POWERED_ON state"""
    vnp = mock_vsphere_node_provider()
    node1 = MagicMock()
    node1.power_state = HardPower.State.POWERED_ON
    vnp.cached_nodes = {"node1": node1}
    is_running = vnp.is_running("node1")
    assert is_running is True


def test_is_terminated():
    """Should return true if a cached node is not in POWERED_ON state"""
    vnp = mock_vsphere_node_provider()
    node1 = MagicMock()
    node1.power_state = HardPower.State.POWERED_OFF
    vnp.cached_nodes = {"node1": node1}
    is_terminated = vnp.is_terminated("node1")
    assert is_terminated is True


def test_node_tags():
    """Should return cached tags of a node"""
    vnp = mock_vsphere_node_provider()
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
    vnp = mock_vsphere_node_provider()
    vm = MagicMock()
    vm.ip_address = "10.123.234.255"
    vnp.vsphere_sdk_client.vcenter.vm.guest.Identity.get.return_value = vm

    ip_address = vnp.external_ip("test_id")
    assert ip_address == "10.123.234.255"


def test_create_nodes():
    vnp = mock_vsphere_node_provider()
    vnp.lock = RLock()
    vnp.cache_stopped_nodes = True

    mock_vm1 = MagicMock()
    mock_vm1.name = "test-node-1"
    mock_vm1.vm = "node-1"

    mock_vm2 = MagicMock()
    mock_vm2.name = "test-node-2"
    mock_vm2.vm = "node-2"

    c = vnp.vsphere_sdk_client
    c.vcenter.VM.list.return_value = [mock_vm1, mock_vm2]
    c.tagging.TagAssociation.list_attached_tags.return_value = ["test_tag_id1"]

    mock_tag = MagicMock()
    mock_tag.name = "ray-cluster-name:test"
    c.tagging.Tag.get.return_value = mock_tag

    vnp.get_category = MagicMock()
    vnp.get_category.return_value = "category_id"
    vnp.get_tag = MagicMock()
    vnp.get_tag.return_value = "tag_id"

    c.vcenter.vm.Power.start = MagicMock()
    vnp.set_node_tags = MagicMock()
    vnp._create_node = MagicMock()
    vnp._create_node.return_value = {"test-3": "New VM Instance"}

    # test
    all_nodes = vnp.create_node({}, {"ray-cluster-name": "test"}, 3)
    # assert
    assert len(all_nodes) == 3
    # assert that two nodes are reused and one is created
    vnp._create_node.assert_called_with({}, {"ray-cluster-name": "test"}, 1)


def test_get_vm():
    vnp = mock_vsphere_node_provider()

    vnp.cached_nodes = {"test-1": "instance-1", "test-2": "instance-2"}
    vm = vnp.get_vm("test-2")
    assert vm == "instance-2"


@patch("ray.autoscaler._private.vsphere.node_provider.vim.vm.RelocateSpec")
@patch("ray.autoscaler._private.vsphere.node_provider.vim.vm.InstantCloneSpec")
def test_create_instant_clone_node(mock_ic_spec, mock_relo_spec):
    vnp = mock_vsphere_node_provider()
    VM.InstantCloneSpec = MagicMock(return_value="Clone Spec")
    vnp.vsphere_sdk_client.vcenter.VM.instant_clone.return_value = "test_id_1"
    vnp.vsphere_sdk_client.vcenter.vm.Power.stop.return_value = None
    vnp.get_pyvmomi_obj = MagicMock(return_value=MagicMock())
    vnp.set_node_tags = MagicMock(return_value=None)
    vnp.vsphere_sdk_client.vcenter.VM.list = MagicMock(
        return_value=[MagicMock(vm="test VM")]
    )
    vnp.connect_nics = MagicMock(return_value=None)
    vnp.vsphere_sdk_client.vcenter.vm.hardware.Ethernet.list.return_value = None
    vnp.get_vm = MagicMock(return_value="test VM")

    vm_clone_from = MagicMock(vm="test-1")
    node_config = {"resource_pool": "rp1", "datastore": "ds1", "resources": {}}
    tags = {"key": "value"}

    mock_ic_spec.return_value = MagicMock()
    mock_relo_spec.return_value = MagicMock()
    vm = vnp.create_instant_clone_node(vm_clone_from, "target-vm", node_config, tags)
    # assert
    assert vm == "test VM"


def test__create_node():
    vnp = mock_vsphere_node_provider()
    node_config = {}

    def mock_create_instant_clone_node():
        mock_nodes = [MagicMock(), MagicMock()]
        for mock_node in mock_nodes:
            mock_node.vm = "vm-test"
            yield mock_node

    vnp.create_instant_clone_node = MagicMock(
        side_effect=mock_create_instant_clone_node()
    )

    vnp.vsphere_sdk_client.vcenter.vm.Power.get.return_value = HardPower.Info(
        state=HardPower.State.POWERED_OFF
    )
    vnp.vsphere_sdk_client.vcenter.vm.Power.start.return_value = None

    vnp.get_category = MagicMock(return_value=None)
    vnp.create_category = MagicMock(return_value="category_id")
    vnp.get_tag = MagicMock(return_value=None)
    vnp.create_node_tag = MagicMock(return_value="tag_id")
    vnp.attach_tag = MagicMock(return_value=None)
    vnp.get_frozen_vm_obj = MagicMock(return_value=MagicMock())
    vnp.delete_vm = MagicMock(return_value=None)
    vnp.lock = RLock()

    created_nodes_dict = vnp._create_node(
        node_config, {"ray-node-name": "ray-node-1", "ray-node-type": "head"}, 2
    )

    assert len(created_nodes_dict) == 2
    vnp.delete_vm.assert_not_called()
    vnp.create_category.assert_called()
    vnp.attach_tag.assert_called_with("vm-test", "VirtualMachine", tag_id="tag_id")
    assert vnp.attach_tag.call_count == 2


def test_get_tag():
    vnp = mock_vsphere_node_provider()
    mock_tag = MagicMock()
    mock_tag.name = "ray-node-name"
    vnp.vsphere_sdk_client.tagging.Tag.list_tags_for_category.return_value = ["tag_1"]
    vnp.vsphere_sdk_client.tagging.Tag.get.return_value = mock_tag
    assert vnp.get_tag("ray-node-name", "test_category_id") == "tag_1"


def test_get_tag_return_none():
    vnp = mock_vsphere_node_provider()
    mock_tag = MagicMock()
    mock_tag.name = "ray-node-name"
    vnp.vsphere_sdk_client.tagging.Tag.list_tags_for_category.return_value = ["tag_1"]
    vnp.vsphere_sdk_client.tagging.Tag.get.return_value = mock_tag
    assert vnp.get_tag("ray-node-name1", "test_category_id") is None


def test_create_node_tag():
    vnp = mock_vsphere_node_provider()
    vnp.vsphere_sdk_client.tagging.Tag.CreateSpec.return_value = "tag_spec"
    vnp.vsphere_sdk_client.tagging.Tag.create.return_value = "tag_id_1"
    tag_id = vnp.create_node_tag("ray_node_tag", "test_category_id")
    assert tag_id == "tag_id_1"


def test_get_category():
    vnp = mock_vsphere_node_provider()
    vnp.vsphere_sdk_client.tagging.Category.list.return_value = ["category_1"]

    mock_category = MagicMock()
    mock_category.name = "ray"
    vnp.vsphere_sdk_client.tagging.Category.get.return_value = mock_category
    category = vnp.get_category()
    assert category == "category_1"
    mock_category.name = "default"
    category = vnp.get_category()
    assert category is None


def test_create_category():
    vnp = mock_vsphere_node_provider()

    vnp.vsphere_sdk_client.tagging.Category.CreateSpec.return_value = "category_spec"
    vnp.vsphere_sdk_client.tagging.Category.create.return_value = "category_id_1"
    category_id = vnp.create_category()
    assert category_id == "category_id_1"


def test_terminate_node():
    vnp = mock_vsphere_node_provider()

    mock_vm1 = MagicMock(vm="vm1")
    mock_vm2 = MagicMock(vm="vm2")
    cached_vms = {"vm_1": mock_vm1, "vm_2": mock_vm2}

    def side_effect(arg):
        return cached_vms[arg]

    vnp._get_cached_node = MagicMock(side_effect=side_effect)
    vnp.cached_nodes = cached_vms
    vnp.vsphere_sdk_client.vcenter.vm.Power.stop = MagicMock()
    vnp.tag_cache = {}
    vnp.terminate_node("vm_2")
    vnp.vsphere_sdk_client.vcenter.vm.Power.stop.assert_called_once_with("vm_2")
    assert len(vnp.cached_nodes) == 1


def test__get_node():
    vnp = mock_vsphere_node_provider()
    vnp.non_terminated_nodes = MagicMock()
    vnp.vsphere_sdk_client.vcenter.VM.list.return_value = ["vm_1"]
    vm = vnp._get_node("node_id_1")
    assert vm == "vm_1"
    vnp.vsphere_sdk_client.vcenter.VM.list.return_value = []
    vm = vnp._get_node("node_id_1")
    assert vm is None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
