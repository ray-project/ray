import copy
import os
import re
import sys
import tempfile
from threading import RLock
from unittest.mock import MagicMock, patch

import pytest

from ray.autoscaler._private.vsphere.cluster_operator_client import (
    ClusterOperatorClient,
    VMNodeStatus,
)
from ray.autoscaler.tags import (
    NODE_KIND_HEAD,
    NODE_KIND_WORKER,
    STATUS_SETTING_UP,
    STATUS_UNINITIALIZED,
    STATUS_UP_TO_DATE,
    TAG_RAY_LAUNCH_CONFIG,
    TAG_RAY_NODE_KIND,
    TAG_RAY_NODE_NAME,
    TAG_RAY_NODE_STATUS,
    TAG_RAY_USER_NODE_TYPE,
)

_CLUSTER_NAME = "ray-cluster"
_PROVIDER_CONFIG = {
    "vsphere_config": {
        "namespace": "deploy-ray",
        "ca_cert": "",
        "api_server": "10.10.10.10",
    }
}

_CLUSTER_CONFIG = {
    "cluster_name": "ray-cluster",
    "max_workers": 5,
    "upscaling_speed": 1.0,
    "docker": {
        "image": "project-taiga-docker-local.artifactory.eng.vmware.com/\
        development/ray:milestone_2",
        "container_name": "ray_container",
        "pull_before_run": True,
        "run_options": [],
    },
    "idle_timeout_minutes": 5,
    "provider": {
        "type": "vsphere",
        "vsphere_config": {
            "namespace": "deploy-ray",
            "ca_cert": "",
            "api_server": "10.78.116.4",
            "vm_image": "vmi-86621c422637d11ef",
            "storage_class": "wcpe2e-nfs-profile",
        },
    },
    "auth": {
        "ssh_user": "ray",
        "ssh_private_key": os.path.join(tempfile.mkdtemp(), "id_rsa_ray.pem"),
    },
    "available_node_types": {
        "ray.head.default": {
            "resources": {},
            "node_config": {"vm_class": "best-effort-xlarge"},
        },
        "worker": {
            "min_workers": 1,
            "max_workers": 3,
            "resources": {},
            "node_config": {"vm_class": "best-effort-xlarge"},
        },
    },
    "head_node_type": "ray.head.default",
    "file_mounts": {},
    "cluster_synced_files": [],
    "file_mounts_sync_continuously": False,
    "rsync_exclude": [],
    "rsync_filter": [],
    "initialization_commands": [],
    "setup_commands": [],
    "head_setup_commands": {},
    "worker_setup_commands": [],
    "head_start_ray_commands": [],
    "worker_start_ray_commands": [],
}


_CLUSTER_RESPONSE = {
    "metadata": {
        "creationTimestamp": "2024-08-01T07:20:42Z",
        "deletionGracePeriodSeconds": 0,
        "deletionTimestamp": "2024-08-01T07:30:14Z",
        "finalizers": ["vmraycluster.vmray.broadcom.com"],
        "generation": 3,
        "labels": {"vmray.kubernetes.io/head-nounce": "ly2uv"},
    },
    "spec": {
        "api_server": {"location": "10.185.109.130"},
        "autoscaler_desired_workers": {
            "ray-cluster-w-8bx6j4n1": "ray.worker.default",
            "ray-cluster-w-nb9u9xf3": "ray.worker.default",
        },
        "common_node_config": {
            "available_node_types": {
                "ray.head.default": {
                    "max_workers": 0,
                    "min_workers": 0,
                    "resources": {},
                    "vm_class": "best-effort-xlarge",
                },
                "ray.worker.default": {
                    "max_workers": 5,
                    "min_workers": 2,
                    "resources": {"cpu": 4, "memory": 4294967296},
                    "vm_class": "best-effort-xlarge",
                },
            },
            "max_workers": 5,
            "storage_class": "vsan-default-storage-policy",
            "vm_image": "vmi-76e37912125b9ad17",
            "vm_password_salt_hash": "$6$test1234$9/BUZHNkvq.c1miDDMG5cHLmM4V7gbYdGuF0/\
                /3gSIh//DOyi7ypPCs6EAA9b8/tidHottL6UG0tG/RqTgAAi/",
            "vm_user": "ray-vm",
        },
        "enable_tls": True,
        "head_node": {"port": 6254, "node_type": "ray.head.default"},
        "ray_docker_image": "project-taiga-docker-local.artifactory.eng.vmware.com/\
            development/ray:milestone_2.0",
    },
    "status": {
        "cluster_state": "healthy",
        "current_workers": {
            "ray-cluster-w-8bx6j4n1": {"ip": "10.244.1.131", "vm_status": "setting-up"},
            "ray-cluster-w-nb9u9xf3": {"ip": "10.244.1.132", "vm_status": "setting-up"},
        },
        "head_node_status": {
            "ip": "10.244.1.130",
            "ray_status": "running",
            "vm_status": "running",
        },
    },
}


def create_random_pvt_key():
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem_private_key = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    delete_random_pvt_key()
    file = open(_CLUSTER_CONFIG["auth"]["ssh_private_key"], "w")
    file.write(pem_private_key.decode())
    file.close()


def delete_random_pvt_key():
    filename = _CLUSTER_CONFIG["auth"]["ssh_private_key"]
    if os.path.exists(filename):
        os.remove(filename)


def mock_cluster_operator():
    def __init__(self, cluster_name: str, provider_config: dict, cluster_config: dict):
        self.cluster_name = cluster_name
        self.vmraycluster_nounce = None
        self.max_worker_nodes = None
        self.vsphere_config = provider_config["vsphere_config"]
        self.namespace = self.vsphere_config["namespace"]
        os.environ["SERVICE_ACCOUNT_TOKEN"] = "test_account"
        self.k8s_api_client = MagicMock()
        if cluster_config:
            self.max_worker_nodes = cluster_config["max_workers"]
            self.head_setup_commands = cluster_config["head_setup_commands"]
            self.available_node_types = cluster_config["available_node_types"]
            self.head_node_type = cluster_config["head_node_type"]

            # docker configurations.
            self.provider_auth = cluster_config["auth"]
            self.docker = cluster_config["docker"]
            self.docker_config = None

    with patch.object(ClusterOperatorClient, "__init__", __init__):
        operator = ClusterOperatorClient(
            _CLUSTER_NAME, _PROVIDER_CONFIG, _CLUSTER_CONFIG
        )
    return copy.deepcopy(operator)


def test_list_vms():
    """Should return nodes as per tags provided in the tag_filters."""
    operator = mock_cluster_operator()
    operator.lock = RLock()
    head_node_tags = {TAG_RAY_NODE_KIND: NODE_KIND_HEAD}
    head_node_tags[TAG_RAY_LAUNCH_CONFIG] = "launch_hash"
    head_node_tags[TAG_RAY_NODE_NAME] = "ray-{}-head".format(operator.cluster_name)
    head_node_tags[TAG_RAY_NODE_STATUS] = STATUS_UNINITIALIZED
    # Mock cluster response
    operator._get_cluster_response = MagicMock(return_value=_CLUSTER_RESPONSE)
    head_node = f"{operator.cluster_name}-h-1234"
    operator._get_head_name = MagicMock(return_value=head_node)
    # make sure function provides head node
    nodes, tag_cache = operator.list_vms(head_node_tags)
    # make sure max number of worker nodes are set up
    assert operator.max_worker_nodes == 5
    assert len(nodes) == 1
    assert nodes[0] == head_node
    assert head_node in tag_cache.keys()
    assert tag_cache[head_node][TAG_RAY_NODE_NAME] == head_node
    assert tag_cache[head_node][TAG_RAY_NODE_STATUS] == STATUS_UP_TO_DATE
    assert tag_cache[head_node][TAG_RAY_NODE_KIND] == NODE_KIND_HEAD
    assert tag_cache[head_node][TAG_RAY_USER_NODE_TYPE] == "ray.head.default"
    # In case of wrong node kind no node should be returned
    node_tags = head_node_tags.copy()
    node_tags[TAG_RAY_NODE_KIND] = "wrong_node"
    nodes, tag_cache = operator.list_vms(node_tags)
    assert len(nodes) == 0
    assert len(tag_cache) == 0

    # Make sure status updated properly
    cluster_response = _CLUSTER_RESPONSE.copy()
    cluster_response["status"]["head_node_status"][
        "vm_status"
    ] = VMNodeStatus.INITIALIZED.value
    # Mock cluster response
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    # make sure the function provides head node only
    nodes, tag_cache = operator.list_vms(head_node_tags)
    assert nodes[0] == head_node
    assert head_node in tag_cache.keys()
    assert tag_cache[head_node][TAG_RAY_NODE_NAME] == head_node
    assert tag_cache[head_node][TAG_RAY_NODE_STATUS] == STATUS_SETTING_UP

    cluster_response["status"]["head_node_status"][
        "vm_status"
    ] = VMNodeStatus.FAIL.value
    # Mock cluster response
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    # make sure the function provides head node only
    nodes, tag_cache = operator.list_vms(head_node_tags)
    assert nodes[0] == head_node
    assert head_node in tag_cache.keys()
    assert tag_cache[head_node][TAG_RAY_NODE_NAME] == head_node
    assert tag_cache[head_node][TAG_RAY_NODE_STATUS] == STATUS_UNINITIALIZED

    # make sure the function returns only worker nodes
    worker_node_tags = {TAG_RAY_NODE_KIND: NODE_KIND_WORKER}
    cluster_response = _CLUSTER_RESPONSE.copy()
    # Mock cluster response
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    nodes, tag_cache = operator.list_vms(worker_node_tags)
    assert len(nodes) == 2
    for node in nodes:
        assert tag_cache[node][TAG_RAY_NODE_NAME] == node
        assert tag_cache[node][TAG_RAY_NODE_STATUS] == STATUS_UNINITIALIZED
        assert tag_cache[node][TAG_RAY_NODE_KIND] == NODE_KIND_WORKER

    # In case of no tag_filters provided return head node as well as worker nodes
    tag_filters = {}
    cluster_response = _CLUSTER_RESPONSE.copy()
    cluster_response["status"]["head_node_status"][
        "vm_status"
    ] = VMNodeStatus.RUNNING.value
    # Mock cluster response
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    # make sure the function provides head node as well all worker nodes.
    nodes, tag_cache = operator.list_vms(tag_filters)
    assert len(nodes) == 3
    assert head_node in tag_cache.keys()
    assert tag_cache[head_node][TAG_RAY_NODE_NAME] == head_node
    assert tag_cache[head_node][TAG_RAY_NODE_STATUS] == STATUS_UP_TO_DATE
    for node in nodes:
        if node == head_node:
            continue
        assert tag_cache[node][TAG_RAY_NODE_NAME] == node
        assert tag_cache[node][TAG_RAY_NODE_STATUS] == STATUS_UNINITIALIZED
        assert tag_cache[node][TAG_RAY_NODE_KIND] == NODE_KIND_WORKER
    # make sure the function returns nodes which are not yet initialised but are in
    # desired workers list
    cluster_response = _CLUSTER_RESPONSE.copy()
    del cluster_response["status"]["current_workers"]["ray-cluster-w-nb9u9xf3"]
    # Mock cluster response
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    nodes, tag_cache = operator.list_vms(tag_filters)
    desired_worker = "ray-cluster-w-nb9u9xf3"
    assert len(nodes) == 3
    assert tag_cache[desired_worker][TAG_RAY_NODE_NAME] == desired_worker
    assert tag_cache[desired_worker][TAG_RAY_NODE_STATUS] == STATUS_UNINITIALIZED
    assert tag_cache[desired_worker][TAG_RAY_NODE_KIND] == NODE_KIND_WORKER


def test_is_vm_power_on():
    """Should return True only if VM is in Running state"""
    operator = mock_cluster_operator()
    operator.lock = RLock()
    operator._get_node = MagicMock(
        return_value={"vm_status": VMNodeStatus.RUNNING.value}
    )
    assert operator.is_vm_power_on("test_vm") is True

    operator._get_node = MagicMock(
        return_value={"vm_status": VMNodeStatus.INITIALIZED.value}
    )
    assert operator.is_vm_power_on("test_vm") is False


def test_is_vm_creating():
    """Should return True only if VM is in INITIALIZED state"""
    operator = mock_cluster_operator()
    operator.lock = RLock()
    operator._get_node = MagicMock(
        return_value={"vm_status": VMNodeStatus.RUNNING.value}
    )
    assert operator.is_vm_creating("test_vm") is False
    operator._get_node = MagicMock(
        return_value={"vm_status": VMNodeStatus.INITIALIZED.value}
    )
    assert operator.is_vm_creating("test_vm") is True


def test_get_vm_external_ip():
    """Should return IP if it is a valid one otherwise return None"""
    operator = mock_cluster_operator()
    # Check for worker node
    operator._get_head_name = MagicMock(return_value=f"{operator.cluster_name}-h-1234")
    operator.lock = RLock()
    # Check IP is valid one
    operator._get_node = MagicMock(
        return_value={"vm_status": VMNodeStatus.RUNNING.value, "ip": "10.10.10.10"}
    )
    ip = operator.get_vm_external_ip("test_vm")
    assert ip == "10.10.10.10"
    # Check for invalid IP
    operator._get_node = MagicMock(
        return_value={"vm_status": VMNodeStatus.RUNNING.value, "ip": ""}
    )
    ip = operator.get_vm_external_ip("test_vm")
    assert ip is None
    # Check for invalid IP
    operator._get_node = MagicMock(
        return_value={"vm_status": VMNodeStatus.FAIL.value, "ip": ""}
    )
    ip = operator.get_vm_external_ip("test_vm")
    assert ip is None

    # Test head node
    operator._get_vm_service_ingress = MagicMock(return_value=[{"ip": "10.20.10.10"}])
    ip = operator.get_vm_external_ip(f"{operator.cluster_name}-h-1234")
    assert ip == "10.20.10.10"

    operator._get_vm_service_ingress = MagicMock(return_value=[{"ip": ""}])
    ip = operator.get_vm_external_ip(f"{operator.cluster_name}-h-1234")
    assert ip is None

    operator._get_vm_service_ingress = MagicMock(return_value=[{}])
    ip = operator.get_vm_external_ip(f"{operator.cluster_name}-h-1234")
    assert ip is None


def test_delete_node():
    """Should call _patch with updated desired worker nodes"""
    operator = mock_cluster_operator()
    operator.lock = RLock()
    cluster_response = _CLUSTER_RESPONSE.copy()
    # Mock cluster response
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    operator.k8s_api_client.custom_object_api.patch_namespaced_custom_object = (
        MagicMock()
    )
    operator.delete_node("ray-cluster-w-nb9u9xf3")
    custom_api = operator.k8s_api_client.custom_object_api
    custom_api.patch_namespaced_custom_object.assert_called_once()


def test_create_nodes():
    """Should call _patch to create worker nodes"""
    operator = mock_cluster_operator()
    operator.lock = RLock()
    cluster_response = _CLUSTER_RESPONSE.copy()
    operator.k8s_api_client.custom_object_api.patch_namespaced_custom_object = (
        MagicMock()
    )
    # Mock cluster response
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    # Make sure API is not getting called to create a head node as it should be
    # created by the operator.
    tags = {TAG_RAY_NODE_NAME: "head", TAG_RAY_USER_NODE_TYPE: "ray.default.head"}
    operator._patch = MagicMock()
    operator._create_secret = MagicMock()
    operator._create_vmraycluster = MagicMock()
    head_node_name = f"{operator.cluster_name}-h-1234"
    operator._create_node_name = MagicMock(return_value=head_node_name)
    create_random_pvt_key()
    created_nodes = operator.create_nodes(tags, 1, {})
    delete_random_pvt_key()
    assert len(created_nodes) == 1
    assert created_nodes[head_node_name] == head_node_name
    operator._create_secret.assert_called_once()
    operator._create_vmraycluster.assert_called_once()
    # if nodes to be creaed is zero, _patch should not be called
    operator._create_secret = MagicMock()
    operator._create_vmraycluster = MagicMock()
    created_nodes = operator.create_nodes(tags, 0, {})
    assert len(created_nodes) == 0
    operator._create_secret.assert_not_called()
    operator._create_vmraycluster.assert_not_called()
    # Make sure desired number of workers get created
    tags = {TAG_RAY_NODE_NAME: "worker", TAG_RAY_USER_NODE_TYPE: "ray.default.worker"}
    operator.max_worker_nodes = 5
    operator._get_cluster_response = MagicMock(return_value={})
    operator._create_node_name = MagicMock()
    operator.k8s_api_client.custom_object_api.patch_namespaced_custom_object = (
        MagicMock()
    )
    operator._create_node_name.side_effect = ["test-w-1", "test-w-2"]
    created_nodes = operator.create_nodes(tags, 2, {})
    custom_api = operator.k8s_api_client.custom_object_api
    custom_api.patch_namespaced_custom_object.assert_called()
    assert len(created_nodes) == 2
    # Make sure the function creates worker nodes if no desired workers present
    cluster_response = _CLUSTER_RESPONSE.copy()
    cluster_response["spec"]["autoscaler_desired_workers"] = {}
    operator._create_node_name = MagicMock()
    operator._create_node_name.side_effect = [
        "test-w-1",
        "test-w-2",
        "test-w-3",
        "test-w-4",
        "test-w-5",
    ]
    # Mock cluster response
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    operator.k8s_api_client.custom_object_api.patch_namespaced_custom_object = (
        MagicMock()
    )
    created_nodes = operator.create_nodes(tags, 5, {})
    custom_api = operator.k8s_api_client.custom_object_api
    custom_api.patch_namespaced_custom_object.assert_called()
    assert len(created_nodes) == 5
    # Make sure not to overprovision the worker nodes
    cluster_response = _CLUSTER_RESPONSE.copy()
    cluster_response["spec"]["autoscaler_desired_workers"] = {
        "test-1": "ray.default.worker",
        "test-2": "ray.default.worker",
        "test-3": "ray.default.worker",
    }
    operator._create_node_name = MagicMock()
    operator._create_node_name.side_effect = ["test-w-4", "test-w-5", "test-w-6"]
    # Mock cluster response
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    operator.k8s_api_client.custom_object_api.patch_namespaced_custom_object = (
        MagicMock()
    )
    created_nodes = operator.create_nodes(tags, 3, {})
    custom_api = operator.k8s_api_client.custom_object_api
    custom_api.patch_namespaced_custom_object.assert_not_called()
    assert len(created_nodes) == 0


def test__get_node():
    """Should provide a node as per given node_id"""
    operator = mock_cluster_operator()
    operator.lock = RLock()
    cluster_response = _CLUSTER_RESPONSE.copy()
    # Mock cluster response
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    # Get head node
    head_node = f"{operator.cluster_name}-head"
    operator._get_head_name = MagicMock(return_value=head_node)
    node = operator._get_node(head_node)
    assert node["vm_status"] == VMNodeStatus.RUNNING.value
    assert node["ip"] == "10.244.1.130"
    # Get worker node from the current workers
    cluster_response["status"]["current_workers"] = {
        "ray-cluster-w-8bx6j4n1": {"vm_status": "initialized"}
    }
    worker_node = "ray-cluster-w-8bx6j4n1"
    node = operator._get_node(worker_node)
    assert node["vm_status"] == VMNodeStatus.INITIALIZED.value
    # get worker from the desired workers
    cluster_response["spec"]["autoscaler_desired_workers"] = {
        "test-vm1": "ray.default.worker"
    }
    # Mock cluster response
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    worker_node = "test-vm1"
    node = operator._get_node(worker_node)
    assert node["vm_status"] == VMNodeStatus.INITIALIZED.value
    # Should not return any node
    cluster_response = _CLUSTER_RESPONSE.copy()
    # Mock cluster response
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    worker_node = "test-vm4"
    assert operator._get_node(worker_node) == {}


def test_safe_to_scale():
    """Should not scale until current state matches desired state"""
    operator = mock_cluster_operator()
    operator.lock = RLock()
    operator.max_worker_nodes = 5
    cluster_response = _CLUSTER_RESPONSE.copy()
    # Case where not all workers are up and running
    cluster_response["status"]["current_workers"] = {
        "test-vm1": {"vm_status": "running"}
    }
    cluster_response["spec"]["autoscaler_desired_workers"] = {
        "test-vm1": "ray.default.worker",
        "test-vm2": "ray.default.worker",
    }
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    assert operator.safe_to_scale() is False
    # Case where workers are waiting to be deleted
    cluster_response["status"]["current_workers"] = {
        "test-vm1": {"vm_status": "running"},
        "test-vm2": {"vm_status": "running"},
    }
    cluster_response["spec"]["autoscaler_desired_workers"] = {
        "test-vm1": "ray.default.worker"
    }
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    assert operator.safe_to_scale() is False
    # Case where current state is a desired state
    operator._patch = MagicMock()
    cluster_response["spec"]["autoscaler_desired_workers"] = {
        "test-vm1": "ray.default.worker",
        "test-vm2": "ray.default.worker",
    }
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    assert operator.safe_to_scale() is True


def test__create_node_name():
    """Should create node name for head and worker nodes"""
    operator = mock_cluster_operator()
    # test for head node name
    pattern = re.compile("^(ray-cluster-h-[a-z0-9]{8})$")
    node_name = operator._create_node_name("ray-head-node")
    assert pattern.match(node_name) is not None
    # test for worker node name
    pattern = re.compile("^(ray-cluster-w-[a-z0-9]{8})$")
    node_name = operator._create_node_name("ray-worker-node")
    assert pattern.match(node_name) is not None


def test__get_head_name():
    """Should return node name for a head node"""
    operator = mock_cluster_operator()
    operator.vmraycluster_nounce = None
    # If nounce is not set read it from
    # cluster response
    cluster_response = _CLUSTER_RESPONSE.copy()
    nounce = cluster_response["metadata"]["labels"]["vmray.kubernetes.io/head-nounce"]
    # Mock cluster response
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    head_node_name = operator._get_head_name()
    assert head_node_name == f"{operator.cluster_name}-h-{nounce}"


def test__create_vmraycluster():
    """Should create VMRayCluster CRD"""
    operator = mock_cluster_operator()
    operator.vmraycluster_nounce = "1234"
    operator.k8s_api_client.custom_object_api.create_namespaced_custom_object = (
        MagicMock()
    )
    operator._create_vmraycluster()
    custom_api = operator.k8s_api_client.custom_object_api
    custom_api.create_namespaced_custom_object.assert_called_once()


def test__set_tags():
    """Should set tags for a given node"""
    operator = mock_cluster_operator()
    node_id = "test-1"
    node_kind = "head"
    node_user_type = "ray.default.head"
    node_status = "running"
    new_tags = operator._set_tags(
        node_id,
        node_kind,
        node_user_type,
        node_status,
        {
            "ray-node-status": "",
            "ray-node-name": "",
            "ray-node-type": "",
            "ray-user-node-type": "",
            "test": "test",
        },
    )
    assert new_tags[TAG_RAY_NODE_STATUS] == STATUS_UP_TO_DATE
    assert new_tags[TAG_RAY_NODE_NAME] == node_id
    assert new_tags[TAG_RAY_NODE_KIND] == node_kind
    assert new_tags[TAG_RAY_USER_NODE_TYPE] == node_user_type
    assert new_tags["test"] == "test"
    node_status = "initialized"
    new_tags = operator._set_tags(
        node_id,
        node_kind,
        node_user_type,
        node_status,
        {
            "ray-node-status": "",
            "ray-node-name": "",
            "ray-node-type": "",
            "ray-user-node-type": "",
            "test": "test",
        },
    )
    assert new_tags[TAG_RAY_NODE_STATUS] == STATUS_SETTING_UP
    node_status = "any_other_status"
    new_tags = operator._set_tags(
        node_id,
        node_kind,
        node_user_type,
        node_status,
        {
            "ray-node-status": "",
            "ray-node-name": "",
            "ray-node-type": "",
            "ray-user-node-type": "",
            "test": "test",
        },
    )
    assert new_tags[TAG_RAY_NODE_STATUS] == STATUS_UNINITIALIZED


def test__set_max_worker_nodes():
    operator = mock_cluster_operator()
    cluster_response = _CLUSTER_RESPONSE.copy()
    operator._get_cluster_response = MagicMock(return_value=cluster_response)
    operator.max_worker_nodes = None
    operator._set_max_worker_nodes()
    assert operator.max_worker_nodes == 5


def test_get_vm_service_ingress():
    operator = mock_cluster_operator()
    vm_service_response = {
        "status": {"loadBalancer": {"ingress": [{"ip": "10.10.10.10"}]}}
    }
    operator._get_vm_service = MagicMock(return_value=vm_service_response)
    ingress = operator._get_vm_service_ingress()
    assert len(ingress) == 1
    assert ingress[0]["ip"] == "10.10.10.10"
    # Check if status is not available
    vm_service_response = {}
    operator._get_vm_service = MagicMock(return_value=vm_service_response)
    assert operator._get_vm_service_ingress() == []
    # Check if ingress in unavailable
    vm_service_response = {"status": {"loadBalancer": {}}}
    operator._get_vm_service = MagicMock(return_value=vm_service_response)
    assert operator._get_vm_service_ingress() == []


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
