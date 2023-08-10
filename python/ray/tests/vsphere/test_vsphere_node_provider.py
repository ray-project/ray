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


def test_non_terminated_nodes_returns_no_node():
    """If all nodes are in POWERED_ONmode then the function should return no node"""

    def __init__(self, provider_config: dict, cluster_name: str):
        self.lock = RLock()
        self.vsphere_sdk_client = MagicMock()
        self.cluster_name = cluster_name
        self.vsphere_sdk_client.vcenter.VM.list.return_value = []

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    # test
    nodes = node_provider.non_terminated_nodes({})

    # assert
    assert node_provider is not None
    assert len(nodes) == 0


def test_non_terminated_nodes_returns_node():
    """If there is a POWERED_ON node and it matches `ray-cluster-name` tag
    function should return that node"""

    def __init__(self, provider_config: dict, cluster_name: str):
        self.lock = RLock()
        self.tag_cache = {}
        self.cached_nodes = {}
        self.vsphere_sdk_client = MagicMock()
        self.cluster_name = cluster_name

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    mock_vm1 = MagicMock()
    mock_vm1.vm = "test-1"

    c = node_provider.vsphere_sdk_client
    c.vcenter.VM.list.return_value = [mock_vm1]
    c.tagging.TagAssociation.list_attached_tags.return_value = ["test_tag_id"]
    mock_power_status = MagicMock()
    mock_power_status.state = HardPower.State.POWERED_ON
    c.vcenter.vm.Power.get.return_value = mock_power_status

    mock_tag = MagicMock()
    mock_tag.name = "ray-cluster-name:test"
    c.tagging.Tag.get.return_value = mock_tag

    # test
    nodes = node_provider.non_terminated_nodes({})
    # assert
    assert node_provider is not None
    assert len(nodes) == 1
    assert "test-1" in node_provider.cached_nodes.keys()
    assert nodes[0] == "test-1"


def test_non_terminated_nodes_returns_no_node_if_tag_not_matched():
    """If there is a POWERED_ON node and it does not matches any tag then
    function should return that node"""

    def __init__(self, provider_config: dict, cluster_name: str):
        self.lock = RLock()
        self.tag_cache = {}
        self.cached_nodes = {}
        self.vsphere_sdk_client = MagicMock()
        self.cluster_name = cluster_name

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    mock_vm1 = MagicMock()
    mock_vm1.vm = "test-1"
    c = node_provider.vsphere_sdk_client
    c.vcenter.VM.list.return_value = [mock_vm1]
    c.tagging.TagAssociation.list_attached_tags.return_value = ["test_tag_id"]

    mock_tag = MagicMock()
    mock_tag.name = "ray-cluster-name:default"
    c.tagging.Tag.get.return_value = mock_tag

    # test
    nodes = node_provider.non_terminated_nodes({})
    # assert
    assert node_provider is not None
    assert len(nodes) == 0


def test_is_running():
    """Should return true if a cached node is in POWERED_ON state"""

    def __init__(self, provider_config: dict, cluster_name: str):
        pass

    def _get_cached_node(self, node_id: str):
        mock_vm1 = MagicMock()
        mock_vm1.power_state = HardPower.State.POWERED_ON
        # self._get_cached_node.return_value = mock_vm1
        return mock_vm1

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    with patch.object(VsphereNodeProvider, "_get_cached_node", _get_cached_node):
        is_running = node_provider.is_running("test_node_id")
    # assert
    assert is_running is True


def test_is_terminated():
    """Should return true if a cached node is not in POWERED_ON state"""

    def __init__(self, provider_config: dict, cluster_name: str):
        pass

    def _get_cached_node(self, node_id: str):
        mock_vm1 = MagicMock()
        mock_vm1.power_state = HardPower.State.POWERED_ON
        # self._get_cached_node.return_value = mock_vm1
        return mock_vm1

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    with patch.object(VsphereNodeProvider, "_get_cached_node", _get_cached_node):
        is_terminated = node_provider.is_running("test_node_id")

    # assert
    assert is_terminated is True


def test_node_tags():
    """Should return chached tags of a node"""

    def __init__(self, provider_config: dict, cluster_name: str):
        self.tag_cache_lock = threading.Lock()
        self.tag_cache = {}
        self.tag_cache["test_vm_id_1"] = {
            "ray-cluster-name": "test",
            "ray-launch-config": "test_id",
            "ray-node-type": "head",
            "ray-node-name": "test-node",
        }

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)
    # test
    tags = node_provider.node_tags("test_vm_id_1")
    # assert
    assert tags == node_provider.tag_cache["test_vm_id_1"]


def test_external_ip():
    """If there is a POWERED_ON node and it does not matches any tag then
    function should return that node"""

    def __init__(self, provider_config: dict, cluster_name: str):
        self.vsphere_sdk_client = MagicMock()

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    vm = MagicMock()
    vm.ip_address = "10.123.234.255"
    node_provider.vsphere_sdk_client.vcenter.vm.guest.Identity.get.return_value = vm

    # test
    ip_address = node_provider.external_ip("test_id")
    # assert
    assert node_provider is not None
    assert ip_address == "10.123.234.255"


def test_create_nodes():
    def __init__(self, provider_config: dict, cluster_name: str):
        self.lock = RLock()
        self.cache_stopped_nodes = True
        self.cluster_name = cluster_name

        self.vsphere_sdk_client = MagicMock()

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    mock_vm1 = MagicMock()
    mock_vm1.name = "test-node-1"
    mock_vm1.vm = "node-1"

    mock_vm2 = MagicMock()
    mock_vm2.name = "test-node-2"
    mock_vm2.vm = "node-2"

    c = node_provider.vsphere_sdk_client
    c.vcenter.VM.list.return_value = [mock_vm1, mock_vm2]
    c.tagging.TagAssociation.list_attached_tags.return_value = ["test_tag_id1"]

    mock_tag = MagicMock()
    mock_tag.name = "ray-cluster-name:test"
    c.tagging.Tag.get.return_value = mock_tag

    node_provider.get_category = MagicMock()
    node_provider.get_category.return_value = "category_id"
    node_provider.get_tag = MagicMock()
    node_provider.get_tag.return_value = "tag_id"

    c.vcenter.vm.Power.start = MagicMock()
    node_provider.set_node_tags = MagicMock()
    node_provider._create_node = MagicMock()
    node_provider._create_node.return_value = {"test-3": "New VM Instance"}

    # test
    all_nodes = node_provider.create_node({}, {"ray-cluster-name": "test"}, 3)
    # assert
    assert node_provider is not None
    assert len(all_nodes) == 3
    # assert that two nodes are reused and one is created
    node_provider._create_node.assert_called_with({}, {"ray-cluster-name": "test"}, 1)


def test_get_vm():
    def __init__(self, provider_config: dict, cluster_name: str):
        pass

    def _get_cached_node(self, node_id: str):
        nodes = {"test-1": "instance-1", "test-2": "instance-2"}
        return nodes[node_id]

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    with patch.object(VsphereNodeProvider, "_get_cached_node", _get_cached_node):
        vm = node_provider.get_vm("test-2")

    # assert
    assert node_provider is not None
    assert vm == "instance-2"


@patch("ray.autoscaler._private.vsphere.node_provider.vim.vm.RelocateSpec")
@patch("ray.autoscaler._private.vsphere.node_provider.vim.vm.InstantCloneSpec")
def test_create_instant_clone_node(mock_ic_spec, mock_relo_spec):
    def __init__(self, provider_config: dict, cluster_name: str):
        VM.InstantCloneSpec = MagicMock()
        VM.InstantCloneSpec.return_value = "Clone Spec"
        self.vsphere_sdk_client = MagicMock()
        self.vsphere_sdk_client.vcenter.VM.instant_clone = MagicMock()
        self.vsphere_sdk_client.vcenter.VM.instant_clone.return_value = "test_id_1"
        self.vsphere_sdk_client.vcenter.vm.Power.stop = MagicMock()
        self.get_pyvmomi_obj = MagicMock()
        self.get_pyvmomi_obj.return_value = MagicMock()
        mock_vm_obj = MagicMock()
        mock_vm_obj.vm = "test VM"
        self.set_node_tags = MagicMock()
        self.set_node_tags.return_value = None
        self.vsphere_sdk_client.vcenter.VM.list = MagicMock()
        self.vsphere_sdk_client.vcenter.VM.list.return_value = [mock_vm_obj]
        self.connect_nics = MagicMock()
        self.connect_nics.return_value = None
        self.vsphere_sdk_client.vcenter.vm.hardware.Ethernet.list.return_value = None
        vsphere_credentials = provider_config["vsphere_config"]["credentials"]
        self.vsphere_credentials = vsphere_credentials
        self.get_vm = MagicMock()
        self.get_vm.return_value = "test VM"

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    vm_clone_from = MagicMock()
    vm_clone_from.vm = "test-1"
    node_config = {"resource_pool": "rp1", "datastore": "ds1", "resources": {}}
    tags = {"key": "value"}

    mock_ic_spec.return_value = MagicMock()
    mock_relo_spec.return_value = MagicMock()
    vm = node_provider.create_instant_clone_node(
        vm_clone_from, "target-vm", node_config, tags
    )
    # assert
    assert vm == "test VM"


def test__create_node():
    def __init__(self, provider_config: dict, cluster_name: str):
        self.vsphere_sdk_client = MagicMock()

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    node_config = {}

    def mock_create_instant_clone_node():
        mock_nodes = [MagicMock(), MagicMock()]
        for mock_node in mock_nodes:
            mock_node.vm = "vm-test"
            yield mock_node

    node_provider.create_instant_clone_node = MagicMock(
        side_effect=mock_create_instant_clone_node()
    )

    node_provider.vsphere_sdk_client.vcenter.vm.Power.get = MagicMock()
    node_provider.vsphere_sdk_client.vcenter.vm.Power.get.return_value = HardPower.Info(
        state=HardPower.State.POWERED_OFF
    )
    node_provider.vsphere_sdk_client.vcenter.vm.Power.start = MagicMock()

    node_provider.get_category = MagicMock()
    node_provider.get_category.return_value = None
    node_provider.create_category = MagicMock()
    node_provider.create_category.return_value = "category_id"
    node_provider.get_tag = MagicMock()
    node_provider.get_tag.return_value = None
    node_provider.create_node_tag = MagicMock()
    node_provider.create_node_tag.return_value = "tag_id"
    node_provider.attach_tag = MagicMock()
    node_provider.get_frozen_vm_obj = MagicMock()
    node_provider.get_frozen_vm_obj.return_value = MagicMock()
    node_provider.delete_vm = MagicMock()

    node_provider.lock = RLock()

    # test
    created_nodes_dict = node_provider._create_node(
        node_config, {"ray-node-name": "ray-node-1", "ray-node-type": "head"}, 2
    )
    # assert
    # Make sure 2 nodes are created
    assert len(created_nodes_dict) == 2
    node_provider.delete_vm.assert_not_called()
    node_provider.create_category.assert_called()
    node_provider.attach_tag.assert_called_with(
        "vm-test", "VirtualMachine", tag_id="tag_id"
    )
    # attaching "vsphere-node-status:created" tag on 2 nodes
    assert node_provider.attach_tag.call_count == 2


def test_get_tag():
    def __init__(self, provider_config: dict, cluster_name: str):
        self.vsphere_sdk_client = MagicMock()

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    mock_tag = MagicMock()
    mock_tag.name = "ray-node-name"

    node_provider.vsphere_sdk_client.tagging.Tag.list_tags_for_category = MagicMock()
    node_provider.vsphere_sdk_client.tagging.Tag.list_tags_for_category.return_value = [
        "tag_1"
    ]
    node_provider.vsphere_sdk_client.tagging.Tag.get = MagicMock()
    node_provider.vsphere_sdk_client.tagging.Tag.get.return_value = mock_tag

    # test
    id = node_provider.get_tag("ray-node-name", "test_category_id")
    # assert
    assert id == "tag_1"


def test_get_tag_return_none():
    def __init__(self, provider_config: dict, cluster_name: str):
        self.vsphere_sdk_client = MagicMock()

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    mock_tag = MagicMock()
    mock_tag.name = "ray-node-name"

    node_provider.vsphere_sdk_client.tagging.Tag.list_tags_for_category = MagicMock()
    node_provider.vsphere_sdk_client.tagging.Tag.list_tags_for_category.return_value = [
        "tag_1"
    ]
    node_provider.vsphere_sdk_client.tagging.Tag.get = MagicMock()
    node_provider.vsphere_sdk_client.tagging.Tag.get.return_value = mock_tag

    # test
    id = node_provider.get_tag("ray-node-name1", "test_category_id")
    # assert
    assert id is None


def test_create_node_tag():
    def __init__(self, provider_config: dict, cluster_name: str):
        self.vsphere_sdk_client = MagicMock()

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    node_provider.vsphere_sdk_client.tagging.Tag.CreateSpec = MagicMock()
    node_provider.vsphere_sdk_client.tagging.Tag.CreateSpec.return_value = "tag_spec"

    node_provider.vsphere_sdk_client.tagging.Tag.create = MagicMock()
    node_provider.vsphere_sdk_client.tagging.Tag.create.return_value = "tag_id_1"

    # test
    tag_id = node_provider.create_node_tag("ray_node_tag", "test_category_id")
    # assert
    assert tag_id == "tag_id_1"


def test_get_category():
    def __init__(self, provider_config: dict, cluster_name: str):
        self.vsphere_sdk_client = MagicMock()

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    node_provider.vsphere_sdk_client.tagging.Category.list = MagicMock()
    node_provider.vsphere_sdk_client.tagging.Category.list.return_value = ["category_1"]

    mock_category = MagicMock()
    mock_category.name = "ray"
    node_provider.vsphere_sdk_client.tagging.Category.get = MagicMock()
    node_provider.vsphere_sdk_client.tagging.Category.get.return_value = mock_category

    # test
    id = node_provider.get_category()
    # assert
    assert id == "category_1"

    mock_category.name = "default"
    # test when category doesn't match
    id = node_provider.get_category()
    # assert
    assert id is None


def test_create_category():
    def __init__(self, provider_config: dict, cluster_name: str):
        self.vsphere_sdk_client = MagicMock()

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    node_provider.vsphere_sdk_client.tagging.Category.CreateSpec = MagicMock()
    node_provider.vsphere_sdk_client.tagging.Category.CreateSpec.return_value = (
        "category_spec"
    )
    node_provider.vsphere_sdk_client.tagging.Category.create = MagicMock()
    node_provider.vsphere_sdk_client.tagging.Category.create.return_value = (
        "category_id_1"
    )

    # test
    category_id = node_provider.create_category()
    # assert
    assert category_id == "category_id_1"


def test_terminate_node():
    def __init__(self, provider_config: dict, cluster_name: str):
        self.vsphere_sdk_client = MagicMock()

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    mock_vm1 = MagicMock()
    mock_vm1.vm = "vm1"
    mock_vm2 = MagicMock()
    mock_vm2.vm = "vm2"
    cached_vms = {"vm_1": mock_vm1, "vm_2": mock_vm2}

    def side_effect(arg):
        return cached_vms[arg]

    node_provider._get_cached_node = MagicMock(side_effect=side_effect)
    node_provider.cached_nodes = cached_vms
    node_provider.vsphere_sdk_client.vcenter.vm.Power.stop = MagicMock()
    node_provider.tag_cache = {}
    # test
    node_provider.terminate_node("vm_2")
    # assert
    node_provider.vsphere_sdk_client.vcenter.vm.Power.stop.assert_called_once_with(
        "vm_2"
    )
    assert len(node_provider.cached_nodes) == 1


def test__get_node():
    def __init__(self, provider_config: dict, cluster_name: str):
        self.vsphere_sdk_client = MagicMock()

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    node_provider.non_terminated_nodes = MagicMock()
    node_provider.vsphere_sdk_client.vcenter.VM.list = MagicMock()
    node_provider.vsphere_sdk_client.vcenter.VM.list.return_value = ["vm_1"]

    # test
    vm = node_provider._get_node("node_id_1")
    # assert
    assert vm == "vm_1"
    # if no node available
    node_provider.vsphere_sdk_client.vcenter.VM.list.return_value = []
    # test
    vm = node_provider._get_node("node_id_1")
    # assert
    assert vm is None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
