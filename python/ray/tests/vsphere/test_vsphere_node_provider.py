from threading import RLock
import pytest
import threading
from unittest.mock import MagicMock, patch

from com.vmware.vcenter.vm_client import Power as HardPower
from python.ray.autoscaler._private.vsphere.node_provider import VsphereNodeProvider
from com.vmware.vcenter_client import VM
from com.vmware.vcenter_client import ResourcePool
from com.vmware.content.library_client import Item
from com.vmware.vcenter.ovf_client import LibraryItem

_CLUSTER_NAME = "test"
_PROVIDER_CONFIG = {
    "vsphere_config": {
        "admin_user": "test@vsphere.local",
        "admin_password": "test",
        "server": "10.188.17.50",
        "datacenter": "test",
        "datastore": "testDatastore",
    }
}


def test_non_terminated_nodes_returns_no_node():
    """If all nodes are in POWERED_ONmode then the function should return no node"""

    def __init__(self, provider_config: dict, cluster_name: str):
        self.lock = RLock()
        self.vsphere_client = MagicMock()
        self.cluster_name = cluster_name
        self.vsphere_client.vcenter.VM.list.return_value = []

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
        self.vsphere_client = MagicMock()
        self.cluster_name = cluster_name

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    mock_vm1 = MagicMock()
    mock_vm1.vm = "test-1"

    node_provider.vsphere_client.vcenter.VM.list.return_value = [mock_vm1]
    node_provider.vsphere_client.tagging.TagAssociation.list_attached_tags.return_value = [
        "test_tag_id "
    ]

    mock_tag = MagicMock()
    mock_tag.name = "ray-cluster-name:test"
    node_provider.vsphere_client.tagging.Tag.get.return_value = mock_tag

    # test
    nodes = node_provider.non_terminated_nodes({})
    # assert
    assert node_provider is not None
    assert len(nodes) == 1
    assert node_provider.cached_nodes.__contains__("test-1")
    assert nodes[0] == "test-1"


def test_non_terminated_nodes_returns_no_node_if_tag_not_matched():
    """If there is a POWERED_ON node and it does not matches any tag then
    function should return that node"""

    def __init__(self, provider_config: dict, cluster_name: str):
        self.lock = RLock()
        self.tag_cache = {}
        self.cached_nodes = {}
        self.vsphere_client = MagicMock()
        self.cluster_name = cluster_name

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    mock_vm1 = MagicMock()
    mock_vm1.vm = "test-1"

    node_provider.vsphere_client.vcenter.VM.list.return_value = [mock_vm1]
    node_provider.vsphere_client.tagging.TagAssociation.list_attached_tags.return_value = [
        "test_tag_id "
    ]

    mock_tag = MagicMock()
    mock_tag.name = "ray-cluster-name:default"
    node_provider.vsphere_client.tagging.Tag.get.return_value = mock_tag

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
    assert is_running == True


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
        self.vsphere_client = MagicMock()

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    vm = MagicMock()
    vm.ip_address = "10.123.234.255"
    node_provider.vsphere_client.vcenter.vm.guest.Identity.get.return_value = vm

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

        self.vsphere_client = MagicMock()

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    mock_vm1 = MagicMock()
    mock_vm1.name = "test-node-1"
    mock_vm1.vm = "node-1"

    mock_vm2 = MagicMock()
    mock_vm2.name = "test-node-2"
    mock_vm2.vm = "node-2"

    node_provider.vsphere_client.vcenter.VM.list.return_value = [mock_vm1, mock_vm2]
    node_provider.vsphere_client.tagging.TagAssociation.list_attached_tags.return_value = [
        "test_tag_id1"
    ]

    mock_tag = MagicMock()
    mock_tag.name = "ray-cluster-name:test"
    node_provider.vsphere_client.tagging.Tag.get.return_value = mock_tag

    node_provider.get_category = MagicMock()
    node_provider.get_category.return_value = "category_id"
    node_provider.get_tag = MagicMock()
    node_provider.get_tag.return_value = "tag_id"

    node_provider.vsphere_client.vcenter.vm.Power.start = MagicMock()
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


def test_create_instant_clone_node():
    def __init__(self, provider_config: dict, cluster_name: str):
        VM.InstantCloneSpec = MagicMock()
        VM.InstantCloneSpec.return_value = "Clone Spec"
        self.vsphere_client = MagicMock()
        self.vsphere_client.vcenter.VM.instant_clone = MagicMock()
        self.vsphere_client.vcenter.VM.instant_clone.return_value = "test_id_1"
        self.vsphere_client.vcenter.vm.Power.stop = MagicMock()
        self.get_vm = MagicMock()
        self.get_vm.return_value = "test VM"

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    vm_clone_from = MagicMock()
    vm_clone_from.vm = "test-1"
    # test
    vm = node_provider.create_instant_clone_node(vm_clone_from, "target-vm")
    # assert
    assert vm == "test VM"
    node_provider.vsphere_client.vcenter.VM.instant_clone.assert_called_with(
        "Clone Spec"
    )


def test_create_ovf_node_rp_summaries_not_found():
    def __init__(self, provider_config: dict, cluster_name: str):
        self.resourcepoolname = "test_resource_pool"
        self.vsphere_client = MagicMock()

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)
        ResourcePool.FilterSpec = MagicMock()
        ResourcePool.FilterSpec.return_value = "resource_pool_spec"
        node_provider.vsphere_client.vcenter.ResourcePool.list = MagicMock()
        node_provider.vsphere_client.vcenter.ResourcePool.list.return_value = None

    # test
    with pytest.raises(ValueError) as error:
        node_provider.create_ovf_node({"resource_pool": "test_rp"}, "target_vm")
        # assert
        assert (
            error.value.message
            == f"Resource pool with name {node_provider.resourcepoolname} not found"
        )


def test_create_ovf_node_OVF_deployment_failed():
    def __init__(self, provider_config: dict, cluster_name: str):
        self.resourcepoolname = "test_resource_pool"
        self.vsphere_client = MagicMock()

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    ResourcePool.FilterSpec = MagicMock()
    ResourcePool.FilterSpec.return_value = "resource_pool_spec"
    node_provider.vsphere_client.vcenter.ResourcePool.list = MagicMock()

    resource_pool_summary = MagicMock()
    resource_pool_summary.resource_pool = "resource_pool_1"
    node_provider.vsphere_client.vcenter.ResourcePool.list.return_value = (
        resource_pool_summary
    )

    Item.FindSpec = MagicMock()
    Item.FindSpec.return_value = "item_spec"

    node_provider.vsphere_client.content.library.Item.find = MagicMock()
    items = ["item_1", "item_2", "item_3"]
    node_provider.vsphere_client.content.library.Item.find.return_value = items
    LibraryItem.DeploymentTarget = MagicMock()
    LibraryItem.DeploymentTarget.return_value = "deployment_target"

    mock_summary = MagicMock()
    mock_summary.name = "ovf_summary"
    node_provider.vsphere_client.vcenter.ovf.LibraryItem.filter = MagicMock()
    node_provider.vsphere_client.vcenter.ovf.LibraryItem.filter.return_value = (
        mock_summary
    )

    LibraryItem.ResourcePoolDeploymentSpec = MagicMock()
    LibraryItem.ResourcePoolDeploymentSpec.return_value = "deployment_spec"

    mock_result = MagicMock()
    mock_result.succeeded = False
    mock_error = MagicMock()
    mock_error.message = "error_1"
    mock_result.error.errors = [mock_error]
    node_provider.vsphere_client.vcenter.ovf.LibraryItem.deploy = MagicMock()
    node_provider.vsphere_client.vcenter.ovf.LibraryItem.deploy.return_value = (
        mock_result
    )

    vm_name_target = "target_vm"
    # test
    with pytest.raises(ValueError) as error:
        node_provider.create_ovf_node(
            {"resource_pool": "test_rp", "library_item": "item"}, vm_name_target
        )
        # assert
        assert error.value.message == f"OVF deployment failed for VM {vm_name_target}"


def test_create_ovf_node():
    def __init__(self, provider_config: dict, cluster_name: str):
        self.resourcepoolname = "test_resource_pool"
        self.vsphere_client = MagicMock()

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    ResourcePool.FilterSpec = MagicMock()
    ResourcePool.FilterSpec.return_value = "resource_pool_spec"
    node_provider.vsphere_client.vcenter.ResourcePool.list = MagicMock()

    resource_pool_summary = MagicMock()
    resource_pool_summary.resource_pool = "resource_pool_1"
    node_provider.vsphere_client.vcenter.ResourcePool.list.return_value = (
        resource_pool_summary
    )

    Item.FindSpec = MagicMock()
    Item.FindSpec.return_value = "item_spec"

    node_provider.vsphere_client.content.library.Item.find = MagicMock()
    items = ["item_1", "item_2", "item_3"]
    node_provider.vsphere_client.content.library.Item.find.return_value = items
    LibraryItem.DeploymentTarget = MagicMock()
    LibraryItem.DeploymentTarget.return_value = "deployment_target"

    mock_summary = MagicMock()
    mock_summary.name = "ovf_summary"
    node_provider.vsphere_client.vcenter.ovf.LibraryItem.filter = MagicMock()
    node_provider.vsphere_client.vcenter.ovf.LibraryItem.filter.return_value = (
        mock_summary
    )

    LibraryItem.ResourcePoolDeploymentSpec = MagicMock()
    LibraryItem.ResourcePoolDeploymentSpec.return_value = "deployment_spec"

    mock_result = MagicMock()
    mock_result.succeeded = True
    mock_result.resource_id.id = "resource_id"
    node_provider.vsphere_client.vcenter.ovf.LibraryItem.deploy = MagicMock()
    node_provider.vsphere_client.vcenter.ovf.LibraryItem.deploy.return_value = (
        mock_result
    )

    mock_vm = MagicMock()
    mock_vm.vm = "vm_1"
    node_provider.get_vm = MagicMock()
    node_provider.get_vm.return_value = mock_vm
    node_provider.set_cloudinit_userdata = MagicMock()

    vm_name_target = "target_vm"
    # test
    vm = node_provider.create_ovf_node(
        {"resource_pool": "test_rp", "library_item": "item"}, vm_name_target
    )
    # assert
    assert vm.vm == mock_vm.vm
    node_provider.get_vm.assert_called_with(mock_result.resource_id.id)
    node_provider.set_cloudinit_userdata.assert_called_with(vm.vm)


def test__create_node():
    def __init__(self, provider_config: dict, cluster_name: str):
        self.vsphere_client = MagicMock()

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    node_config = {"clone_from": "vm_1", "clone": True}

    mock_vm = MagicMock()
    mock_vm.vm = "vm-1"
    node_provider.get_vm = MagicMock()
    node_provider.get_vm.return_value = mock_vm
    node_provider.create_instant_clone_node = MagicMock()
    node_provider.create_instant_clone_node.return_value = mock_vm

    node_provider.vsphere_client.vcenter.vm.Power.get = MagicMock()
    node_provider.vsphere_client.vcenter.vm.Power.get.return_value = HardPower.Info(
        state=HardPower.State.POWERED_OFF
    )
    node_provider.vsphere_client.vcenter.vm.Power.start = MagicMock()

    node_provider.get_category = MagicMock()
    node_provider.get_category.return_value = None
    node_provider.create_category = MagicMock()
    node_provider.create_category.return_value = "category_id"
    node_provider.get_tag = MagicMock()
    node_provider.get_tag.return_value = None
    node_provider.create_node_tag = MagicMock()
    node_provider.create_node_tag.return_value = "tag_id"
    node_provider.attach_tag = MagicMock()
    node_provider.create_ovf_node = MagicMock()

    # test
    created_nodes_dict = node_provider._create_node(
        node_config, {"ray-node-name": "ray-node-1", "ray-node-type": "head"}, 2
    )
    # assert
    # Make sure 2 nodes are created
    assert len(created_nodes_dict) == 2
    node_provider.create_instant_clone_node.assert_called
    node_provider.create_ovf_node.assert_not_called
    node_provider.vsphere_client.vcenter.vm.Power.start.assert_called_with(mock_vm.vm)
    node_provider.create_category.assert_called
    node_provider.attach_tag.assert_called_with(
        mock_vm.vm, "VirtualMachine", tag_id="tag_id"
    )
    # attaching 2 tags on 2 nodes
    assert node_provider.attach_tag.call_count == 4


def test_get_tag():
    def __init__(self, provider_config: dict, cluster_name: str):
        self.vsphere_client = MagicMock()

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    mock_tag = MagicMock()
    mock_tag.name = "ray-node-name"

    node_provider.vsphere_client.tagging.Tag.list_tags_for_category = MagicMock()
    node_provider.vsphere_client.tagging.Tag.list_tags_for_category.return_value = [
        "tag_1"
    ]
    node_provider.vsphere_client.tagging.Tag.get = MagicMock()
    node_provider.vsphere_client.tagging.Tag.get.return_value = mock_tag

    # test
    id = node_provider.get_tag("ray-node-name", "test_category_id")
    # assert
    assert id == "tag_1"


def test_get_tag_return_none():
    def __init__(self, provider_config: dict, cluster_name: str):
        self.vsphere_client = MagicMock()

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    mock_tag = MagicMock()
    mock_tag.name = "ray-node-name"

    node_provider.vsphere_client.tagging.Tag.list_tags_for_category = MagicMock()
    node_provider.vsphere_client.tagging.Tag.list_tags_for_category.return_value = [
        "tag_1"
    ]
    node_provider.vsphere_client.tagging.Tag.get = MagicMock()
    node_provider.vsphere_client.tagging.Tag.get.return_value = mock_tag

    # test
    id = node_provider.get_tag("ray-node-name1", "test_category_id")
    # assert
    assert id is None


def test_create_node_tag():
    def __init__(self, provider_config: dict, cluster_name: str):
        self.vsphere_client = MagicMock()

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    node_provider.vsphere_client.tagging.Tag.CreateSpec = MagicMock()
    node_provider.vsphere_client.tagging.Tag.CreateSpec.return_value = "tag_spec"

    node_provider.vsphere_client.tagging.Tag.create = MagicMock()
    node_provider.vsphere_client.tagging.Tag.create.return_value = "tag_id_1"

    # test
    tag_id = node_provider.create_node_tag("ray_node_tag", "test_category_id")
    # assert
    assert tag_id == "tag_id_1"


def test_get_category():
    def __init__(self, provider_config: dict, cluster_name: str):
        self.vsphere_client = MagicMock()

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    node_provider.vsphere_client.tagging.Category.list = MagicMock()
    node_provider.vsphere_client.tagging.Category.list.return_value = ["category_1"]

    mock_category = MagicMock()
    mock_category.name = "ray"
    node_provider.vsphere_client.tagging.Category.get = MagicMock()
    node_provider.vsphere_client.tagging.Category.get.return_value = mock_category

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
        self.vsphere_client = MagicMock()

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    node_provider.vsphere_client.tagging.Category.CreateSpec = MagicMock()
    node_provider.vsphere_client.tagging.Category.CreateSpec.return_value = (
        "category_spec"
    )
    node_provider.vsphere_client.tagging.Category.create = MagicMock()
    node_provider.vsphere_client.tagging.Category.create.return_value = "category_id_1"

    # test
    category_id = node_provider.create_category()
    # assert
    assert category_id == "category_id_1"


def test_terminate_node():
    def __init__(self, provider_config: dict, cluster_name: str):
        self.vsphere_client = MagicMock()

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
    node_provider.vsphere_client.vcenter.vm.Power.stop = MagicMock()

    # test
    node_provider.terminate_node("vm_2")
    # assert
    node_provider.vsphere_client.vcenter.vm.Power.stop.assert_called_once_with(
        mock_vm2.vm
    )


def test__get_node():
    def __init__(self, provider_config: dict, cluster_name: str):
        self.vsphere_client = MagicMock()

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)

    node_provider.non_terminated_nodes = MagicMock()
    node_provider.vsphere_client.vcenter.VM.list = MagicMock()
    node_provider.vsphere_client.vcenter.VM.list.return_value = ["vm_1"]

    # test
    vm = node_provider._get_node("node_id_1")
    # assert
    assert vm == "vm_1"
    # if no node available
    node_provider.vsphere_client.vcenter.VM.list.return_value = []
    # test
    vm = node_provider._get_node("node_id_1")
    # assert
    assert vm is None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
