import base64
import ipaddress
import logging
import os
import random
import string
from enum import Enum
from threading import RLock
from typing import Any, Dict, Optional, Tuple

from kubernetes import client, config

from ray.autoscaler.tags import (
    NODE_KIND_HEAD,
    NODE_KIND_WORKER,
    STATUS_SETTING_UP,
    STATUS_UNINITIALIZED,
    STATUS_UP_TO_DATE,
    TAG_RAY_CLUSTER_NAME,
    TAG_RAY_NODE_KIND,
    TAG_RAY_NODE_NAME,
    TAG_RAY_NODE_STATUS,
    TAG_RAY_USER_NODE_TYPE,
)

# Design:

# Each modification the autoscaler wants to make is posted to the API server's desired
# state (e.g. if the autoscaler wants to scale up, it adds VM name to the desired
# worker list it wants to scale, if it wants to scale down it removes the name from
# the list).

# VMRay CRD
VMRAY_CRD_VER = os.getenv("VMRAY_CRD_VER", "v1alpha1")
VMRAY_GROUP = "vmray.broadcom.com"
VMRAYCLUSTER_PLURAL = "vmrayclusters"

# VirtualMachineService CRD
VMSERVICE_CRD_VER = os.getenv("VMSERVICE_CRD_VER", "v1alpha2")
VMSERVICE_GROUP = "vmoperator.vmware.com"
VMSERVICE_PLURAL = "virtualmachineservices"

SERVICE_ACCOUNT_TOKEN = os.getenv("SVC_ACCOUNT_TOKEN", None)

logger = logging.getLogger(__name__)
cur_path = os.path.dirname(__file__)


class VMNodeStatus(Enum):
    INITIALIZED = "initialized"
    RUNNING = "running"
    FAIL = "failure"


class KubernetesHttpApiClient(object):
    def __init__(self, ca_cert: str, api_server: str):
        token = SERVICE_ACCOUNT_TOKEN
        # If SERVICE_ACCOUNT_TOKEN not present, use local
        # ~/.kube/config file. Active context will be used.
        # This is usefull when Ray CLI are used and local autoscaler needs
        # communicate with the k8s API server
        # If the token is present then use that for communication.
        if not token:
            self.client = client.ApiClient(config.load_kube_config())
        else:
            configuration = client.Configuration()
            configuration.api_key["authorization"] = token
            configuration.api_key_prefix["authorization"] = "Bearer"
            configuration.host = f"https://{api_server}"
            if ca_cert:
                configuration.ssl_ca_cert = ca_cert
            else:
                configuration.verify_ssl = False
            self.client = client.ApiClient(configuration)

        # Use customObjectsApi to access custom resources
        self.custom_object_api = client.CustomObjectsApi(self.client)


class ClusterOperatorClient(KubernetesHttpApiClient):
    def __init__(
        self,
        cluster_name: str,
        provider_config: Dict[str, Any],
        cluster_config: Dict[str, Any],
    ):
        self.cluster_name = cluster_name
        self.vmraycluster_nounce = None
        self.max_worker_nodes = None

        self.vsphere_config = provider_config["vsphere_config"]

        self.namespace = self.vsphere_config["namespace"]
        self.k8s_api_client = KubernetesHttpApiClient(
            self.vsphere_config.get("ca_cert"),
            self.vsphere_config.get("api_server"),
        )
        self.lock = RLock()

        if cluster_config:
            self.max_worker_nodes = cluster_config["max_workers"]
            self.head_setup_commands = cluster_config["head_setup_commands"]
            self.available_node_types = cluster_config["available_node_types"]
            self.head_node_type = cluster_config["head_node_type"]

            # docker configurations.
            self.provider_auth = cluster_config["auth"]
            self.docker = cluster_config["docker"]

            # create docker login info secret, if it exists.
            docker_auth_secret_name = self._create_docker_auth_secrets()
            if docker_auth_secret_name:
                self.docker_config = {
                    "auth_secret_name": docker_auth_secret_name,
                }
            else:
                self.docker_config = None
        else:
            self._set_max_worker_nodes()
            self._create_tls_secrets()

    def _create_docker_auth_secrets(self):
        docker_auth_secret_name = self.cluster_name + "-docker-auth"
        docker_auth = self.vsphere_config.get("docker_auth", {})
        username = docker_auth.get("username", None)
        password = docker_auth.get("password", None)
        kp = {}
        if username and password:
            kp["username"] = username
            kp["password"] = password
            registry = docker_auth.get("registry", None)
            if registry:
                kp["registry"] = registry
            self._create_secret(self.namespace, docker_auth_secret_name, kp)
            return docker_auth_secret_name
        return None

    def _create_tls_secrets(self):
        # If token is passed that means its instance of autoscaler
        # running inside the head node, so validate if tls server cert
        # and key are available then create a secret with their
        # value.
        tls_enabled = os.environ.get("RAY_USE_TLS", None) == "1"
        if not SERVICE_ACCOUNT_TOKEN or not tls_enabled:
            return

        tls_cert = None
        tls_key = None

        cert_path = os.environ.get("RAY_TLS_SERVER_CERT", None)
        key_path = os.environ.get("RAY_TLS_SERVER_KEY", None)
        if cert_path:
            with open(cert_path) as f:
                tls_cert = f.read()
        if key_path:
            with open(key_path) as f:
                tls_key = f.read()

        if tls_cert and tls_key:
            kp = {"tls.crt": tls_cert, "tls.key": tls_key}
            self._create_secret(self.namespace, self.cluster_name + "-tls", kp)

    def list_vms(self, tag_filters: Dict[str, str]) -> Tuple[list, dict]:
        """Queries K8s for VMs in the RayCluster and filter them as per
        tags provided in the tag_filters.
        """
        logger.info(f"Getting nodes using tags \n{tag_filters}")
        tag_cache = {}

        filters = tag_filters.copy()
        # Use Ray cluster name to get resources
        if TAG_RAY_CLUSTER_NAME not in tag_filters:
            filters[TAG_RAY_CLUSTER_NAME] = self.cluster_name
        nodes = []
        vmray_cluster_response = self._get_cluster_response()
        if not vmray_cluster_response:
            return nodes, tag_cache

        vmray_cluster_status = vmray_cluster_response.get("status", {})
        if not vmray_cluster_status:
            return nodes, tag_cache

        vmray_cluster_spec = vmray_cluster_response.get("spec", {})

        # Check for a head node
        if NODE_KIND_HEAD in tag_filters.values() or not tag_filters:
            head_node_status = vmray_cluster_status.get("head_node_status", {})
            # head node found
            if head_node_status:
                node_id = self._get_head_name()
                nodes.append(node_id)

                # Setting head node status
                status = head_node_status.get("vm_status", None)
                head_nt = vmray_cluster_spec["head_node"]["node_type"]
                tag_cache[node_id] = self._set_tags(
                    node_id, NODE_KIND_HEAD, head_nt, status, filters
                )
        # Check current worker nodes
        if NODE_KIND_WORKER in tag_filters.values() or not tag_filters:
            current_workers = vmray_cluster_status.get("current_workers", {})
            desired_workers = vmray_cluster_spec.get("autoscaler_desired_workers", {})

            # worker nodes found
            for worker in current_workers.keys():
                nodes.append(worker)
                # setting worker node status
                status = current_workers[worker].get("vm_status", None)
                node_type = desired_workers.get(worker, "")

                tag_cache[worker] = self._set_tags(
                    worker, NODE_KIND_WORKER, node_type, status, filters
                )

            # List VMs from the desired workers' list
            for worker in desired_workers.keys():
                if worker in current_workers.keys():
                    continue
                nodes.append(worker)
                node_type = desired_workers.get(worker, "")
                tag_cache[worker] = self._set_tags(
                    worker, NODE_KIND_WORKER, node_type, STATUS_SETTING_UP, filters
                )

            logger.info(
                f"Non terminated nodes {nodes}, Tags for these are: {tag_cache}"
            )
        return nodes, tag_cache

    def is_vm_power_on(self, node_id: str) -> bool:
        """Check current vm list. If its state is Running then return
        true else false."""
        node = self._get_node(node_id)
        if node:
            return node.get("vm_status", None) == VMNodeStatus.RUNNING.value
        logger.info(f"VM {node_id} not found")
        return False

    def is_vm_creating(self, node_id: str) -> bool:
        """Check current vm list. If its state is INITIALIZED then return
        true else false."""
        node = self._get_node(node_id)
        if node:
            return node.get("vm_status", None) == VMNodeStatus.INITIALIZED.value
        logger.info(f"VM {node_id} is not yet initialized")
        return False

    def set_node_tags(self, tags: Dict[str, str]) -> None:
        """
        Not required
        """
        pass

    def get_vm_external_ip(self, node_id: str) -> Optional[str]:
        """Check current worker list and get the external ip."""
        node = {}
        # For a Ray head node, return external IP of the VMService
        # Ray head node is not accessible directly.
        if node_id == self._get_head_name():
            ingress = self._get_vm_service_ingress()
            for item in ingress:
                if "ip" in item.keys():
                    node = item
                    break
        else:
            worker_node = self._get_node(node_id)
            if (
                worker_node
                and worker_node.get("vm_status", None) == VMNodeStatus.RUNNING.value
            ):
                node = worker_node
        ip = node.get("ip", None)
        # Validate returned IP
        if ip and _is_ipv4(ip):
            return ip
        logger.info(
            f"External IPv4 address: {ip} of VM: {node_id}"
            f"is either invalid or not available"
        )
        return None

    def delete_node(self, node_id: str) -> None:
        """Remove name of the vm from the desired worker list and patch
        the VmRayCluster CR"""
        with self.lock:
            vmray_cluster_response = self._get_cluster_response()
            vmray_cluster_spec = vmray_cluster_response.get("spec", {})

            # Get desired workers
            desired_workers = vmray_cluster_spec.get("autoscaler_desired_workers", {})
            logger.info(f"Current desired workers: {desired_workers}")

            # remove the node from the desired workers list
            if node_id in desired_workers:

                # By default it follow patch application of `merge-patch+json`
                # so we need to remove the node ids by making them null.
                # refs:
                # 1. https://kubernetes.io/docs/tasks/manage-kubernetes-objects/
                # update-api-object-kubectl-patch/#use-a-json-merge-patch-to-update-a-deployment
                # 2. https://github.com/kubernetes-client/python/blob/master/kubernetes/
                # client/api/custom_objects_api.py#L3106
                payload = {"spec": {"autoscaler_desired_workers": {node_id: None}}}

                logger.info(f"Deleting VM {node_id} | payload: {payload}")
                self.k8s_api_client.custom_object_api.patch_namespaced_custom_object(
                    VMRAY_GROUP,
                    VMRAY_CRD_VER,
                    self.namespace,
                    VMRAYCLUSTER_PLURAL,
                    self.cluster_name,
                    payload,
                    async_req=False,
                )
            elif node_id == self._get_head_name():
                # Handle case to delete a head node
                # Delete VMRayCluster which will delete head node
                # as well as associated secrets and other resources.
                self.k8s_api_client.custom_object_api.delete_namespaced_custom_object(
                    VMRAY_GROUP,
                    VMRAY_CRD_VER,
                    self.namespace,
                    VMRAYCLUSTER_PLURAL,
                    self.cluster_name,
                )

    def create_nodes(
        self,
        tags: Dict[str, str],
        to_be_launched_node_count: int,
        node_config: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Ask cluster operator to create worker VMs"""
        logger.info(
            f"Creating {to_be_launched_node_count} nodes with tags: {tags}"
            f"and with config: {node_config}"
        )
        created_nodes_dict = {}
        with self.lock:
            if to_be_launched_node_count > 0:
                new_desired_workers = {}

                new_vm_names = {}
                for _ in range(to_be_launched_node_count):
                    name = self._create_node_name(tags[TAG_RAY_NODE_NAME])
                    new_vm_names[name] = tags[TAG_RAY_USER_NODE_TYPE]

                # Create a head node
                # Autoscaler sends a tag
                # head_node_tags[TAG_RAY_NODE_NAME] = "ray-{}-head".format(
                # config["cluster_name"])
                if "head" in tags[TAG_RAY_NODE_NAME]:
                    # head node will be created as a part of VMRayCluster CR
                    self._create_ssh_secret()
                    self._create_vmraycluster()
                else:
                    # Once VMRayCluster CR is created, update it to create worker
                    # nodes.
                    vmray_cluster_response = self._get_cluster_response()
                    vmray_cluster_spec = vmray_cluster_response.get("spec", {})

                    # get desired workers
                    desired_workers = vmray_cluster_spec.get(
                        "autoscaler_desired_workers", {}
                    )

                    # If workers are present in both the list then it shows stable
                    # state for the cluster.
                    # Append new VM names with existing one
                    if desired_workers:
                        new_desired_workers.update(desired_workers)

                    new_desired_workers.update(new_vm_names)
                    logger.info(f"New desired state will be {new_desired_workers}")
                    if len(new_desired_workers) > self.max_worker_nodes:
                        logger.warning(
                            "Autoscaler attempted to create more than max_workers VMs."
                        )
                        return created_nodes_dict

                    payload = {
                        "spec": {"autoscaler_desired_workers": new_desired_workers}
                    }

                    custom_api = self.k8s_api_client.custom_object_api
                    custom_api.patch_namespaced_custom_object(
                        VMRAY_GROUP,
                        VMRAY_CRD_VER,
                        self.namespace,
                        VMRAYCLUSTER_PLURAL,
                        self.cluster_name,
                        payload,
                        async_req=False,
                    )
                for vm in new_vm_names:
                    created_nodes_dict[vm] = vm
            return created_nodes_dict

    def _get_cluster_response(self):
        response = {}
        try:
            response = (
                self.k8s_api_client.custom_object_api.get_namespaced_custom_object(
                    VMRAY_GROUP,
                    VMRAY_CRD_VER,
                    self.namespace,
                    VMRAYCLUSTER_PLURAL,
                    self.cluster_name,
                )
            )
            return response
        except client.exceptions.ApiException as e:
            # If HTTP 404 received means the cluster is not yet created.
            logger.warning(
                f"Exception while getting {self.cluster_name}. Exception: {str(e)}"
            )
            if e.status == 404:
                logger.warning(f"{self.cluster_name} not available. Creating new one.")
            return response

    def _get_node(self, node_id: str) -> Any:
        vmray_cluster_response = self._get_cluster_response()
        vmray_cluster_status = vmray_cluster_response.get("status", {})
        if not vmray_cluster_status:
            return {}
        head_node_status = vmray_cluster_status.get("head_node_status", {})
        current_workers = vmray_cluster_status.get("current_workers", {})
        # head node is found
        if head_node_status and node_id == self._get_head_name():
            return head_node_status
        # worker nodes found
        for worker in current_workers.keys():
            if worker == node_id:
                return current_workers.get(worker)
        # If worker not found in the current worker then it might be getting created
        # and not yet ready. So check if it is in the desired workers list.
        vmray_cluster_spec = vmray_cluster_response.get("spec", {})
        desired_workers = vmray_cluster_spec.get("autoscaler_desired_workers", {})
        for worker in desired_workers.keys():
            if worker == node_id:
                # set vm_status as VM in the desired workers' list will not
                # have vm_status field.
                node = {"vm_status": VMNodeStatus.INITIALIZED.value}
                return node
        logger.info(f"VM {node_id} not found")
        return {}

    def safe_to_scale(self):
        """
        It is safe to scale as long as total number of workers(desired + current)
        do not exceeds cluster level max_workers.
        This function should handle cases:
        1. If there are workers in the desired_workers list but not in the
        current_workers list that means few workers are not yet up and running.
        2. If there are workers in the current_workers list but not in a
        desired_workers list indicates workers are not yet deleted completely
        and we should wait.
        3. If workers are present in both the list shows stable state for the cluster.
        """
        vmray_cluster_response = self._get_cluster_response()
        vmray_cluster_status = vmray_cluster_response.get("status", {})
        if not vmray_cluster_status:
            return False
        current_workers = vmray_cluster_status.get("current_workers", {})
        vmray_cluster_spec = vmray_cluster_response.get("spec", {})
        desired_workers = vmray_cluster_spec.get("autoscaler_desired_workers", {})
        logger.info(
            f"Checking is it safe to scale:\n"
            f"Current workers: {current_workers.keys()} \n"
            f"Autoscaler desired workers: {desired_workers}"
        )

        # Do not scale until reaches desired state
        if len(desired_workers) != len(current_workers):
            return False

        # Wait until all nodes are in a Running state
        for worker in current_workers.values():
            if worker.get("vm_status", None) != VMNodeStatus.RUNNING.value:
                return False

        return True

    def _create_node_name(self, node_name_tag):
        """Create name for a Ray node"""
        # The nodes are named as follows:
        # <cluster-name>-h-<random alphanumeric string> for the head node
        # <cluster-name>-w-<<random alphanumeric string>> for the worker nodes
        random_str = "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(8)
        )
        if "head" in node_name_tag:
            self.vmraycluster_nounce = random_str
            return f"{self.cluster_name}-h-" + self.vmraycluster_nounce
        return f"{self.cluster_name}-w-" + random_str

    def _get_head_name(self):
        if not self.vmraycluster_nounce:
            vmray_cluster_response = self._get_cluster_response()
            self.vmraycluster_nounce = vmray_cluster_response["metadata"]["labels"][
                "vmray.kubernetes.io/head-nounce"
            ]
        return f"{self.cluster_name}-h-" + self.vmraycluster_nounce

    def _create_vmraycluster(self):

        # Define vmraycluster config structure.
        ray_cluster_config = {}
        ray_cluster_config["apiVersion"] = VMRAY_GROUP + "/" + VMRAY_CRD_VER
        ray_cluster_config["kind"] = "VMRayCluster"
        ray_cluster_config["metadata"] = {}
        ray_cluster_config["spec"] = {}

        # Start reading values from local bootstrap config.
        ray_cluster_config["metadata"]["name"] = self.cluster_name
        ray_cluster_config["metadata"]["labels"] = {
            "vmray.kubernetes.io/head-nounce": self.vmraycluster_nounce,
            "vmray.io/created-by": "ray-cli",
        }
        ray_cluster_config["metadata"]["namespace"] = self.namespace

        ray_cluster_config["spec"]["api_server"] = {}
        ray_cluster_config["spec"]["api_server"]["location"] = self.vsphere_config.get(
            "api_server"
        )
        ray_cluster_config["spec"]["ray_docker_image"] = self.docker["image"]

        # Set head node specific config.
        ray_cluster_config["spec"]["head_node"] = {}
        ray_cluster_config["spec"]["head_node"][
            "head_setup_commands"
        ] = self.head_setup_commands
        ray_cluster_config["spec"]["head_node"][
            "port"
        ] = 6379  # using default GCS port for now.

        ray_cluster_config["spec"]["head_node"]["node_type"] = self.head_node_type

        # Set common node config & available node types.
        ray_cluster_config["spec"]["common_node_config"] = {}
        ray_cluster_config["spec"]["common_node_config"][
            "vm_image"
        ] = self.vsphere_config.get("vm_image")
        ray_cluster_config["spec"]["common_node_config"][
            "storage_class"
        ] = self.vsphere_config.get("storage_class")
        ray_cluster_config["spec"]["common_node_config"][
            "vm_password_salt_hash"
        ] = self.vsphere_config.get("vm_password_salt_hash", "")
        ray_cluster_config["spec"]["common_node_config"][
            "max_workers"
        ] = self.max_worker_nodes
        available_node_types = {}
        for node_type, node_config in self.available_node_types.items():
            available_node_types[node_type] = {}
            available_node_types[node_type]["vm_class"] = node_config[
                "node_config"
            ].get("vm_class", None)
            available_node_types[node_type]["resources"] = node_config.get(
                "resources", {}
            )
            available_node_types[node_type]["min_workers"] = node_config.get(
                "min_workers", 0
            )
            available_node_types[node_type]["max_workers"] = node_config.get(
                "max_workers", 2
            )

        ray_cluster_config["spec"]["common_node_config"][
            "available_node_types"
        ] = available_node_types
        ray_cluster_config["spec"]["common_node_config"][
            "vm_user"
        ] = self.provider_auth["ssh_user"]
        if self.docker_config is not None:
            ray_cluster_config["spec"]["docker_config"] = self.docker_config

        logger.info(f"Creating VmRayCluster \n{ray_cluster_config}")
        self.k8s_api_client.custom_object_api.create_namespaced_custom_object(
            VMRAY_GROUP,
            VMRAY_CRD_VER,
            self.namespace,
            VMRAYCLUSTER_PLURAL,
            ray_cluster_config,
        )

    def _set_tags(self, node_id, node_kind, node_user_type, node_status, tags):
        new_tags = tags.copy()
        if node_status == VMNodeStatus.RUNNING.value:
            new_tags[TAG_RAY_NODE_STATUS] = STATUS_UP_TO_DATE
        elif node_status == VMNodeStatus.INITIALIZED.value:
            new_tags[TAG_RAY_NODE_STATUS] = STATUS_SETTING_UP
        else:
            new_tags[TAG_RAY_NODE_STATUS] = STATUS_UNINITIALIZED

        new_tags[TAG_RAY_NODE_NAME] = node_id
        new_tags[TAG_RAY_NODE_KIND] = node_kind
        new_tags[TAG_RAY_USER_NODE_TYPE] = node_user_type
        return new_tags

    def _set_max_worker_nodes(self):
        vmray_cluster_response = self._get_cluster_response()
        if not self.max_worker_nodes:
            vmray_cluster_spec = vmray_cluster_response.get("spec", {})
            common_node_config = vmray_cluster_spec.get("common_node_config", {})
            # If max_workers is not provided then default to 2
            # ref: https://docs.ray.io/en/latest/cluster/vms/references/
            # ray-cluster-configuration.html#max-workers
            self.max_worker_nodes = common_node_config.get("max_workers", 2)
            logger.info(f"Max worker is set to {self.max_worker_nodes}")

    def _get_vm_service_ingress(self):
        response = self._get_vm_service()
        status = response.get("status", {})
        if not status:
            return []
        ingress = status["loadBalancer"].get("ingress", [])
        logger.info(f"VM service ingress is {ingress}")
        return ingress

    def _get_vm_service(self):
        try:
            return self.k8s_api_client.custom_object_api.get_namespaced_custom_object(
                VMSERVICE_GROUP,
                VMSERVICE_CRD_VER,
                self.namespace,
                VMSERVICE_PLURAL,
                self._get_head_name(),
            )
        except client.exceptions.ApiException as e:
            logger.warning(
                f"Exception while getting vm service in namespace {self.namespace}."
                f"Exception: {str(e)}"
            )
            return {}

    def _create_secret(self, namespace, name, kp):
        data = {}
        for k, v in kp.items():
            # Base64 encode the SSH key
            val = v
            if type(v) is str:
                val = v.encode("utf-8")
            data[k] = base64.b64encode(val).decode("utf-8")

        # Create metadata
        metadata = client.V1ObjectMeta(name=name)

        # Secret object
        secret = client.V1Secret(
            api_version="v1", kind="Secret", metadata=metadata, data=data, type="Opaque"
        )
        instance = client.CoreV1Api(self.k8s_api_client.client)

        # Create the secret in the specified namespace.
        try:
            instance.create_namespaced_secret(namespace=namespace, body=secret)
            logger.info(f"Secret {name} created in namespace {namespace}")
        except client.exceptions.ApiException as e:
            print("Failure while creating Secret [`%s`] : %s\n" % (name, e))

    def _create_ssh_secret(self):
        """Create a K8s SSH secret using a local private SSH key
        specified in the config."""

        # Define secret name
        secret_name = f"{self.cluster_name}-ssh-key"

        pvt_key = self.provider_auth.get("ssh_private_key")
        private_key_path = os.path.expanduser(pvt_key)

        # Read the SSH private key
        with open(private_key_path, "rb") as ssh_key:
            ssh_key_data = ssh_key.read()

        kp = {"ssh-pvt-key": ssh_key_data}
        self._create_secret(self.namespace, secret_name, kp)


def _is_ipv4(ip):
    try:
        ipaddress.IPv4Address(ip)
        return True
    except ipaddress.AddressValueError:
        return False
