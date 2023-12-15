import copy
import time
from threading import RLock
import pytest
import threading
from unittest.mock import MagicMock, patch

from ray.autoscaler._private.vsphere.node_provider import VsphereNodeProvider
from com.vmware.vcenter_client import VM
from com.vmware.vcenter.vm_client import Power as HardPower
from ray.autoscaler._private.vsphere.config import (
    update_vsphere_configs,
    validate_frozen_vm_configs,
)
from ray.autoscaler._private.vsphere.gpu_utils import (
    split_vm_2_gpu_cards_map,
    GPUCard,
    set_gpu_placeholder,
)
from ray.autoscaler._private.vsphere.utils import singleton_client

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
        self.cluster_name = cluster_name
        self.tag_cache = {}
        vsphere_credentials = provider_config["vsphere_config"]["credentials"]
        self.vsphere_credentials = vsphere_credentials
        self.vsphere_config = provider_config["vsphere_config"]
        self.get_pyvmomi_sdk_provider = MagicMock()
        self.get_vsphere_sdk_client = MagicMock()
        self.frozen_vm_scheduler = MagicMock()

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)
    return copy.deepcopy(node_provider)


def test_non_terminated_nodes_returns_no_node():
    """There is no node in vSphere"""
    vnp = mock_vsphere_node_provider()
    vnp.lock = RLock()
    vnp.get_vsphere_sdk_client().vcenter.VM.list.return_value = []

    nodes = vnp.non_terminated_nodes({})  # assert
    assert len(nodes) == 0


def test_non_terminated_nodes_returns_nodes_in_powered_off_creating_state():
    vnp = mock_vsphere_node_provider()
    vnp.lock = RLock()
    vnp.tag_cache_lock = threading.Lock()
    vnp.get_vsphere_sdk_client().vcenter.VM.list.return_value = [
        MagicMock(vm="vm1"),
        MagicMock(vm="vm2"),
    ]
    vnp.get_vsphere_sdk_client().vcenter.vm.Power.get.side_effect = [
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
    vnp.tag_cache_lock = threading.Lock()
    vnp.get_vsphere_sdk_client().vcenter.VM.list.return_value = [
        MagicMock(vm="vm1"),
        MagicMock(vm="vm2"),
    ]
    vnp.get_vsphere_sdk_client().vcenter.vm.Power.get.side_effect = [
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
    vnp.tag_cache_lock = threading.Lock()
    vnp.get_vsphere_sdk_client().vcenter.VM.list.return_value = [
        MagicMock(vm="vm1"),
        MagicMock(vm="vm2"),
    ]
    vnp.get_vsphere_sdk_client().vcenter.vm.Power.get.side_effect = [
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


def test_is_terminated():
    """Should return true if a cached node is not in POWERED_ON state"""
    vnp = mock_vsphere_node_provider()
    vnp.get_pyvmomi_sdk_provider().is_vm_power_on.return_value = False
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


@patch("ray.autoscaler._private.vsphere.node_provider.vim.vm.RelocateSpec")
@patch("ray.autoscaler._private.vsphere.node_provider.vim.vm.InstantCloneSpec")
@patch("ray.autoscaler._private.vsphere.pyvmomi_sdk_provider.WaitForTask")
def test_create_instant_clone_node(mock_wait_task, mock_ic_spec, mock_relo_spec):
    vnp = mock_vsphere_node_provider()
    VM.InstantCloneSpec = MagicMock(return_value="Clone Spec")
    vnp.get_vsphere_sdk_client().vcenter.VM.instant_clone.return_value = "test_id_1"
    vnp.get_vsphere_sdk_client().vcenter.vm.Power.stop.return_value = None
    vnp.set_node_tags = MagicMock(return_value=None)
    vnp.get_vsphere_sdk_client().vcenter.VM.list = MagicMock(
        return_value=[MagicMock(vm="test VM")]
    )
    vnp.connect_nics = MagicMock(return_value=None)
    vnp.get_vsphere_sdk_client().vcenter.vm.hardware.Ethernet.list.return_value = None
    vnp.get_vsphere_sdk_vm_obj = MagicMock(return_value="test VM")

    node_config = {"resource_pool": "rp1", "datastore": "ds1", "resources": {}}
    tags = {"key": "value"}
    gpu_ids_map = None

    mock_ic_spec.return_value = MagicMock()
    mock_relo_spec.return_value = MagicMock()
    vm = vnp.create_instant_clone_node(
        "test-1",
        "test VM",
        node_config,
        tags,
        gpu_ids_map,
    )
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

    vnp.get_vsphere_sdk_client().vcenter.vm.Power.get.return_value = HardPower.Info(
        state=HardPower.State.POWERED_OFF
    )
    vnp.get_vsphere_sdk_client().vcenter.vm.Power.start.return_value = None

    vnp.get_category = MagicMock(return_value=None)
    vnp.create_category = MagicMock(return_value="category_id")
    vnp.get_tag = MagicMock(return_value=None)
    vnp.create_node_tag = MagicMock(return_value="tag_id")
    vnp.attach_tag = MagicMock(return_value=None)
    vnp.delete_vm = MagicMock(return_value=None)
    vnp.create_new_or_fetch_existing_frozen_vms = MagicMock(return_value={"vm": "vm-d"})
    vnp.frozen_vm_scheduler().next_frozen_vm = MagicMock(
        return_value=MagicMock(name=MagicMock("frozen-vm"))
    )
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
    vnp.get_vsphere_sdk_client().tagging.Tag.list_tags_for_category.return_value = [
        "tag_1"
    ]
    vnp.get_vsphere_sdk_client().tagging.Tag.get.return_value = mock_tag
    assert vnp.get_tag("ray-node-name", "test_category_id") == "tag_1"


def test_get_tag_return_none():
    vnp = mock_vsphere_node_provider()
    mock_tag = MagicMock()
    mock_tag.name = "ray-node-name"
    vnp.get_vsphere_sdk_client().tagging.Tag.list_tags_for_category.return_value = [
        "tag_1"
    ]
    vnp.get_vsphere_sdk_client().tagging.Tag.get.return_value = mock_tag
    assert vnp.get_tag("ray-node-name1", "test_category_id") is None


def test_create_node_tag():
    vnp = mock_vsphere_node_provider()
    vnp.get_vsphere_sdk_client().tagging.Tag.CreateSpec.return_value = "tag_spec"
    vnp.get_vsphere_sdk_client().tagging.Tag.create.return_value = "tag_id_1"
    tag_id = vnp.create_node_tag("ray_node_tag", "test_category_id")
    assert tag_id == "tag_id_1"


def test_get_category():
    vnp = mock_vsphere_node_provider()
    vnp.get_vsphere_sdk_client().tagging.Category.list.return_value = ["category_1"]

    mock_category = MagicMock()
    mock_category.name = "ray"
    vnp.get_vsphere_sdk_client().tagging.Category.get.return_value = mock_category
    category = vnp.get_category()
    assert category == "category_1"
    mock_category.name = "default"
    category = vnp.get_category()
    assert category is None


def test_create_category():
    vnp = mock_vsphere_node_provider()

    vnp.get_vsphere_sdk_client().tagging.Category.CreateSpec.return_value = (
        "category_spec"
    )
    vnp.get_vsphere_sdk_client().tagging.Category.create.return_value = "category_id_1"
    category_id = vnp.create_category()
    assert category_id == "category_id_1"


def test_terminate_node():
    vnp = mock_vsphere_node_provider()
    vnp.tag_cache_lock = threading.Lock()
    vnp.get_vsphere_sdk_client().vcenter.vm.Power.stop = MagicMock()
    vnp.tag_cache = {"vm1": ["tag1", "tag2"], "vm2": ["tag1", "tag2"]}
    vnp.terminate_node("vm2")
    vnp.get_vsphere_sdk_client().vcenter.vm.Power.stop.assert_called_once_with("vm2")
    assert len(vnp.tag_cache) == 1


def test_create_new_or_fetch_existing_frozen_vms():
    vnp = mock_vsphere_node_provider()
    vnp.ensure_frozen_vm_status = MagicMock()
    vnp.create_frozen_vm_from_ovf = MagicMock()
    vnp.create_frozen_vm_on_each_host = MagicMock()
    vnp.get_pyvmomi_sdk_provider().get_pyvmomi_obj.return_value = MagicMock(
        vm=[MagicMock(), MagicMock()]
    )
    node_config = {"frozen_vm": {"name": "frozen"}}
    vnp.create_new_or_fetch_existing_frozen_vms(node_config)
    vnp.ensure_frozen_vm_status.assert_called()

    node_config = {"frozen_vm": {"name": "frozen", "resource_pool": "frozen-rp"}}
    vnp.create_new_or_fetch_existing_frozen_vms(node_config)

    node_config = {"frozen_vm": {"name": "frozen", "library_item": "frozen"}}
    vnp.create_new_or_fetch_existing_frozen_vms(node_config)
    vnp.create_frozen_vm_from_ovf.assert_called()

    node_config = {
        "frozen_vm": {
            "name": "frozen",
            "library_item": "frozen",
            "resource_pool": "frozen-rp",
        }
    }
    vnp.create_new_or_fetch_existing_frozen_vms(node_config)
    vnp.create_frozen_vm_on_each_host.assert_called()


def test_update_vsphere_configs():
    input_config = {
        "available_node_types": {
            "ray.head.default": {
                "resources": {},
                "node_config": {"resource_pool": "ray", "datastore": "vsan"},
            },
            "worker": {"resources": {}, "node_config": {}},
        },
        "provider": {"vsphere_config": {}},
        "head_node_type": "ray.head.default",
    }

    with pytest.raises(KeyError):
        update_vsphere_configs(input_config)

    input_config = {
        "provider": {
            "vsphere_config": {
                "frozen_vm": {
                    "name": "frozen",
                    "resource_pool": "frozen-rp",
                    "library_item": "frozen",
                    "cluster": "cluster",
                    "datastore": "vsanDatastore",
                },
                "gpu_config": {"dynamic_pci_passthrough": True},
            },
        },
        "available_node_types": {
            "ray.head.default": {
                "resources": {},
                "node_config": {"resource_pool": "ray", "datastore": "vsan"},
            },
            "worker": {"resources": {}, "node_config": {}},
            "worker1": {"resources": {}, "node_config": {}},
        },
        "head_node_type": "ray.head.default",
    }

    update_vsphere_configs(input_config)
    assert (
        "frozen_vm"
        in input_config["available_node_types"]["ray.head.default"]["node_config"]
    )
    assert "frozen_vm" in input_config["available_node_types"]["worker"]["node_config"]
    assert "frozen_vm" in input_config["available_node_types"]["worker1"]["node_config"]
    assert (
        input_config["available_node_types"]["worker"]["node_config"]["frozen_vm"][
            "name"
        ]
        == "frozen"
    )
    assert (
        "dynamic_pci_passthrough"
        in input_config["available_node_types"]["ray.head.default"]["node_config"][
            "gpu_config"
        ]
    )
    assert (
        "dynamic_pci_passthrough"
        in input_config["available_node_types"]["worker"]["node_config"]["gpu_config"]
    )
    assert (
        "dynamic_pci_passthrough"
        in input_config["available_node_types"]["worker1"]["node_config"]["gpu_config"]
    )
    assert input_config["available_node_types"]["worker"]["node_config"]["gpu_config"][
        "dynamic_pci_passthrough"
    ]


def test_validate_frozen_vm_configs():
    # Test a valid case with OVF deployment
    config = {
        "name": "single-frozen-vm",
        "library_item": "frozen-vm-template",
        "cluster": "vsanCluster",
        "datastore": "vsanDatastore",
    }
    assert validate_frozen_vm_configs(config) is None

    # Test a valid case with existing frozen VM
    config = {"name": "existing-single-frozen-vm"}
    assert validate_frozen_vm_configs(config) is None

    # Test a valid case with OVF deployment and resource pool
    config = {
        "name": "frozen-vm-prefix",
        "library_item": "frozen-vm-template",
        "resource_pool": "frozen-vm-resource-pool",
        "datastore": "vsanDatastore",
    }
    assert validate_frozen_vm_configs(config) is None

    # Test a valid case with existing resource pool
    config = {"resource_pool": "frozen-vm-resource-pool"}
    assert validate_frozen_vm_configs(config) is None

    # Test an invalid case missing everything for OVF deployment
    with pytest.raises(
        ValueError,
        match="'datastore' is not given when trying to deploy the frozen VM from OVF.",
    ):
        config = {
            "library_item": "frozen-vm-template",
        }
        validate_frozen_vm_configs(config)

    # Test an invalid case missing datastore for OVF deployment
    with pytest.raises(
        ValueError,
        match="'datastore' is not given when trying to deploy the frozen VM from OVF.",
    ):
        config = {
            "name": "single-frozen-vm",
            "library_item": "frozen-vm-template",
            "cluster": "vsanCluster",
        }
        validate_frozen_vm_configs(config)

    # Test an invalid case missing cluster and resource pool for OVF deployment
    with pytest.raises(
        ValueError,
        match="both 'cluster' and 'resource_pool' are missing when trying to deploy "
        "the frozen VM from OVF, at least one should be given.",
    ):
        config = {
            "name": "single-frozen-vm",
            "library_item": "frozen-vm-template",
            "datastore": "vsanDatastore",
        }
        validate_frozen_vm_configs(config)

    # Test an invalid case missing name for OVF deployment
    with pytest.raises(
        ValueError, match="'name' must be given when deploying the frozen VM from OVF."
    ):
        config = {
            "library_item": "frozen-vm-template",
            "cluster": "vsanCluster",
            "datastore": "vsanDatastore",
        }
        validate_frozen_vm_configs(config)

    # Test an valid case with redundant data
    config = {
        "name": "single-frozen-vm",
        "library_item": "frozen-vm-template",
        "resource_pool": "frozen-vm-resource-pool",
        "cluster": "vsanCluster",
        "datastore": "vsanDatastore",
    }
    assert validate_frozen_vm_configs(config) is None

    # Another case with redundant data
    config = {
        "name": "single-frozen-vm",
        "resource_pool": "frozen-vm-resource-pool",
        "cluster": "vsanCluster",
        "datastore": "vsanDatastore",
    }
    assert validate_frozen_vm_configs(config) is None


def test_split_vm_2_gpu_cards_map():
    # Test a valid case 1
    vm_2_gpu_cards_map = {
        "frozen-vm-1": [
            GPUCard("0000:3b:00.0", "training-0"),
            GPUCard("0000:3b:00.1", "training-1"),
            GPUCard("0000:3b:00.2", "training-2"),
        ],
        "frozen-vm-2": [
            GPUCard("0000:3b:00.3", "training-3"),
            GPUCard("0000:3b:00.4", "training-4"),
        ],
        "frozen-vm-3": [GPUCard("0000:3b:00.5", "training-5")],
    }
    requested_gpu_num = 1
    expected_result = [
        {"frozen-vm-1": [GPUCard("0000:3b:00.0", "training-0")]},
        {"frozen-vm-1": [GPUCard("0000:3b:00.1", "training-1")]},
        {"frozen-vm-1": [GPUCard("0000:3b:00.2", "training-2")]},
        {"frozen-vm-2": [GPUCard("0000:3b:00.3", "training-3")]},
        {"frozen-vm-2": [GPUCard("0000:3b:00.4", "training-4")]},
        {"frozen-vm-3": [GPUCard("0000:3b:00.5", "training-5")]},
    ]

    result = split_vm_2_gpu_cards_map(vm_2_gpu_cards_map, requested_gpu_num)
    assert result == expected_result

    # Test a valid case 2
    requested_gpu_num = 2
    expected_result = [
        {
            "frozen-vm-1": [
                GPUCard("0000:3b:00.0", "training-0"),
                GPUCard("0000:3b:00.1", "training-1"),
            ]
        },
        {
            "frozen-vm-2": [
                GPUCard("0000:3b:00.3", "training-3"),
                GPUCard("0000:3b:00.4", "training-4"),
            ]
        },
    ]
    result = split_vm_2_gpu_cards_map(vm_2_gpu_cards_map, requested_gpu_num)
    assert result == expected_result


def test_set_placeholder():
    data = [
        {"frozen-vm-1": ["0000:3b:00.0", "0000:3b:00.1"]},
        {"frozen-vm-2": ["0000:3b:00.3", "0000:3b:00.4"]},
    ]
    place_holder_number = 3
    expected_result = [
        {"frozen-vm-1": ["0000:3b:00.0", "0000:3b:00.1"]},
        {"frozen-vm-2": ["0000:3b:00.3", "0000:3b:00.4"]},
        {},
        {},
        {},
    ]

    set_gpu_placeholder(data, place_holder_number)
    assert data == expected_result


def test_singleton():
    @singleton_client
    class SingletonClass:
        def __init__(self, value):
            self.value = value

    singleton1 = SingletonClass(1)
    singleton1.ensure_connect = MagicMock()
    singleton2 = SingletonClass(2)

    assert singleton1 is singleton2
    singleton2.ensure_connect.assert_not_called()
    with patch("time.time", return_value=time.time() + 1000):
        singleton3 = SingletonClass(3)
        singleton3.ensure_connect.assert_called_once()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
