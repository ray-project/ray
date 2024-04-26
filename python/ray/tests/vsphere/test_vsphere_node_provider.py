import copy
import time
import pytest
import threading
from unittest.mock import MagicMock, patch

from ray.autoscaler._private.vsphere.node_provider import VsphereNodeProvider
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
        self.get_vsphere_sdk_provider = MagicMock()
        self.frozen_vm_scheduler = MagicMock()

    with patch.object(VsphereNodeProvider, "__init__", __init__):
        node_provider = VsphereNodeProvider(_PROVIDER_CONFIG, _CLUSTER_NAME)
    return copy.deepcopy(node_provider)


def test_is_terminated():
    """Should return true if a cached node is not in POWERED_ON state"""
    vnp = mock_vsphere_node_provider()
    vnp.get_vsphere_sdk_provider = MagicMock()
    vnp.get_pyvmomi_sdk_provider().is_vm_power_on.return_value = False
    vnp.get_vsphere_sdk_provider().is_vm_creating.return_value = False
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


def test_create_instant_clone_node():
    vnp = mock_vsphere_node_provider()
    vnp.get_pyvmomi_sdk_provider().instance_clone_vm = MagicMock(return_value=None)
    vnp.set_node_tags = MagicMock(return_value=None)
    vnp.get_vsphere_sdk_provider().get_vsphere_sdk_vm_obj = MagicMock(
        return_value="test VM"
    )

    node_config = {"resource_pool": "rp1", "datastore": "ds1", "resources": {}}
    tags = {"key": "value"}
    gpu_ids_map = None
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
    vnp.get_vsphere_sdk_provider().set_node_tags = MagicMock(return_value=None)
    vnp.get_vsphere_sdk_provider().delete_vm_by_name = MagicMock(return_value=None)
    vnp.create_new_or_fetch_existing_frozen_vms = MagicMock(return_value={"vm": "vm-d"})
    vnp.frozen_vm_scheduler().next_frozen_vm = MagicMock(
        return_value=MagicMock(name=MagicMock("frozen-vm"))
    )
    created_nodes_dict = vnp._create_node(
        node_config, {"ray-node-name": "ray-node-1", "ray-node-type": "head"}, 2
    )
    assert len(created_nodes_dict) == 2
    assert vnp.get_vsphere_sdk_provider().set_node_tags.call_count == 2
    vnp.get_vsphere_sdk_provider().delete_vm_by_name.assert_not_called()


def test_terminate_node():
    vnp = mock_vsphere_node_provider()
    vnp.tag_cache_lock = threading.Lock()
    vnp.tag_cache = {"vm1": ["tag1", "tag2"], "vm2": ["tag1", "tag2"]}
    vnp.get_vsphere_sdk_provider().delete_vm_by_id = MagicMock(return_value=None)
    vnp.terminate_node("vm2")
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
