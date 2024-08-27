import copy
import logging
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict

from pyVmomi import vim

from ray.autoscaler._private.vsphere.config import (
    bootstrap_vsphere,
    is_dynamic_passthrough,
)
from ray.autoscaler._private.vsphere.gpu_utils import (
    get_gpu_cards_from_vm,
    get_vm_2_gpu_cards_map,
    plug_gpu_cards_to_vm,
    set_gpu_placeholder,
    split_vm_2_gpu_cards_map,
)
from ray.autoscaler._private.vsphere.pyvmomi_sdk_provider import PyvmomiSdkProvider
from ray.autoscaler._private.vsphere.scheduler import SchedulerFactory
from ray.autoscaler._private.vsphere.utils import Constants, now_ts
from ray.autoscaler._private.vsphere.vsphere_sdk_provider import VsphereSdkProvider
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME, TAG_RAY_NODE_NAME

logger = logging.getLogger(__name__)


class VsphereNodeProvider(NodeProvider):
    max_terminate_nodes = 1000

    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        self.frozen_vm_scheduler = None
        self.vsphere_config = provider_config["vsphere_config"]
        self.vsphere_credentials = provider_config["vsphere_config"]["credentials"]

        # The below cache will be a map, whose key is the Ray node and the value will
        # be a list of vSphere tags one that node. The reason for this cache is to
        # avoid calling the vSphere API again when the autoscaler tries to read the tags
        # The cache will be filled when a Ray node is created and tagged.
        self.tag_cache = {}
        self.tag_cache_lock = threading.Lock()

    def get_vsphere_sdk_provider(self):
        return VsphereSdkProvider(
            self.vsphere_credentials["server"],
            self.vsphere_credentials["user"],
            self.vsphere_credentials["password"],
            Constants.SessionType.UNVERIFIED,
        )

    def get_pyvmomi_sdk_provider(self):
        return PyvmomiSdkProvider(
            self.vsphere_credentials["server"],
            self.vsphere_credentials["user"],
            self.vsphere_credentials["password"],
            Constants.SessionType.UNVERIFIED,
        )

    def ensure_frozen_vm_status(self, frozen_vm_name):
        """
        This function will help check if the frozen VM with the specific name is
        existing and in the frozen state. If the frozen VM is existing and off, this
        function will also help to power on the frozen VM and wait until it is frozen.
        """
        self.get_pyvmomi_sdk_provider().power_on_vm(frozen_vm_name)

        # Make sure it is frozen status
        return self.get_pyvmomi_sdk_provider().wait_until_vm_is_frozen(frozen_vm_name)

    @staticmethod
    def bootstrap_config(cluster_config):
        return bootstrap_vsphere(cluster_config)

    def non_terminated_nodes(self, tag_filters):
        nodes, tag_cache = self.get_vsphere_sdk_provider().non_terminated_nodes(
            self.cluster_name, tag_filters
        )
        with self.tag_cache_lock:
            self.tag_cache.update(tag_cache)
        return nodes

    def is_running(self, node_id):
        return self.get_pyvmomi_sdk_provider().is_vm_power_on(node_id)

    def is_terminated(self, node_id):
        if self.get_pyvmomi_sdk_provider().is_vm_power_on(node_id):
            return False
        else:
            # If the node is not powered on but has the creating tag, then it could
            # be under reconfiguration, such as plugging the GPU. In this case we
            # should consider the node is not terminated, it will be turned on later
            return not self.get_vsphere_sdk_provider().is_vm_creating(node_id)

    def node_tags(self, node_id):
        with self.tag_cache_lock:
            return self.tag_cache[node_id]

    def external_ip(self, node_id):
        return self.get_pyvmomi_sdk_provider().get_vm_external_ip(node_id)

    def internal_ip(self, node_id):
        # Currently vSphere VMs do not show an internal IP. So we just return the
        # external IP
        return self.get_pyvmomi_sdk_provider().get_vm_external_ip(node_id)

    def set_node_tags(self, node_id, tags):
        # This method gets called from the Ray and it passes
        # node_id which needs to be vm.vm and not vm.name
        self.get_vsphere_sdk_provider().set_node_tags(node_id, tags)

    def create_node(self, node_config, tags, count) -> Dict[str, Any]:
        """Creates instances.

        Returns dict mapping instance id to VM object for the created
        instances.
        """
        filters = tags.copy()
        if TAG_RAY_CLUSTER_NAME not in tags:
            filters[TAG_RAY_CLUSTER_NAME] = self.cluster_name

        to_be_launched_node_count = count

        logger.info(f"Create {count} node with tags : {filters}")

        created_nodes_dict = {}
        if to_be_launched_node_count > 0:
            created_nodes_dict = self._create_node(
                node_config, filters, to_be_launched_node_count
            )

        return created_nodes_dict

    def create_instant_clone_node(
        self,
        parent_vm_name,
        target_vm_name,
        node_config,
        tags,
        gpu_cards_map,
    ):
        resources = node_config["resources"]
        to_be_plugged_gpu = []
        requested_gpu_num = resources.get("GPU", 0)
        if requested_gpu_num > 0:
            # If the Ray node requires GPU, we will select the frozen VM to do instant
            # clone based on GPU availability
            if not gpu_cards_map:
                raise ValueError(
                    f"No available GPU card to assigned to node {target_vm_name}"
                )

            for vm_name in gpu_cards_map:
                # the gpu_cards_map has helped you to stored which GPUs should bind and
                # which frozen VM should be cloned. There is only one k,v pair in this
                # map
                parent_vm_name = vm_name
                to_be_plugged_gpu = gpu_cards_map[vm_name]
                break

        tags[Constants.VSPHERE_NODE_STATUS] = Constants.VsphereNodeStatus.CREATING.value
        threading.Thread(
            target=self.get_vsphere_sdk_provider().tag_new_vm_instantly,
            args=(target_vm_name, tags),
        ).start()
        self.get_pyvmomi_sdk_provider().instance_clone_vm(
            parent_vm_name,
            target_vm_name,
            node_config.get("resource_pool"),
            node_config.get("datastore"),
        )

        target_vm_id = self.get_pyvmomi_sdk_provider().name_to_id(
            [vim.VirtualMachine], target_vm_name
        )

        if "CPU" in resources:
            # Update number of CPUs
            self.get_vsphere_sdk_provider().update_vm_cpu(
                target_vm_id, resources["CPU"]
            )

        if "Memory" in resources:
            # Update Memory
            self.get_vsphere_sdk_provider().update_vm_memory(
                target_vm_id, resources["Memory"]
            )

        if to_be_plugged_gpu:
            is_dynamic = is_dynamic_passthrough(node_config)
            plug_gpu_cards_to_vm(
                self.get_pyvmomi_sdk_provider(),
                target_vm_name,
                to_be_plugged_gpu,
                is_dynamic,
            )

        return self.get_vsphere_sdk_provider().get_vsphere_sdk_vm_obj(target_vm_id)

    def create_frozen_vm_on_each_host(self, node_config, name, resource_pool_name):
        """
        This function helps to deploy a frozen VM on each ESXi host of the resource pool
        specified in the frozen VM config under the vSphere config section. So that we
        can spread the Ray nodes on different ESXi host at the beginning.
        """
        exception_happened = False
        vm_names = []
        cluster_id = self.get_pyvmomi_sdk_provider().get_cluster_id_of_resource_pool(
            resource_pool_name
        )
        hosts = self.get_vsphere_sdk_provider().list_all_hosts_in_cluster(cluster_id)

        futures_frozen_vms = []
        with ThreadPoolExecutor(max_workers=len(hosts)) as executor:
            for host in hosts:
                node_config_frozen_vm = copy.deepcopy(node_config)
                node_config_frozen_vm["host_id"] = host.host

                frozen_vm_name = "{}-{}-{}".format(name, host.name, now_ts())
                vm_names.append(frozen_vm_name)

                futures_frozen_vms.append(
                    executor.submit(
                        self.create_frozen_vm_from_ovf,
                        node_config_frozen_vm,
                        frozen_vm_name,
                    )
                )

        for future in futures_frozen_vms:
            try:
                future.result()
            except Exception as e:
                logger.error(
                    "Exception occurred while creating frozen VMs {}".format(e)
                )
                exception_happened = True

        # We clean up all the created VMs if any exception occurs.
        if exception_happened:
            with ThreadPoolExecutor(max_workers=len(hosts)) as executor:
                futures = [
                    executor.submit(
                        self.get_vsphere_sdk_provider().delete_vm_by_name, vm_names[i]
                    )
                    for i in range(len(futures_frozen_vms))
                ]
            for future in futures:
                _ = future.result()
            raise RuntimeError("Failed creating frozen VMs, exiting!")

    def create_frozen_vm_from_ovf(self, node_config, vm_name_target):
        resource_pool_id = None
        datastore_name = node_config.get("frozen_vm").get("datastore")
        if not datastore_name:
            raise ValueError(
                "The datastore name must be provided when deploying frozen"
                "VM from OVF"
            )
        datastore_id = self.get_pyvmomi_sdk_provider().name_to_id(
            [vim.Datastore], datastore_name
        )

        if node_config.get("frozen_vm").get("resource_pool"):
            resource_pool_id = (
                self.get_vsphere_sdk_provider().get_resource_pool_id_by_name(
                    node_config.get("frozen_vm").get("resource_pool")
                )
            )
        else:
            cluster_name = node_config.get("frozen_vm").get("cluster")
            if not cluster_name:
                raise ValueError(
                    "The cluster name must be provided when deploying a single frozen"
                    " VM from OVF"
                )

            host_id = self.get_pyvmomi_sdk_provider().get_host_id_of_datastore_cluster(
                datastore_name, cluster_name
            )
            if not host_id:
                raise ValueError("No available host to be assigned")

            logger.info("Found a host {}".format(host_id))
            node_config["host_id"] = host_id
            resource_pool_id = (
                self.get_pyvmomi_sdk_provider().get_resource_pool_id_in_cluster(
                    cluster_name
                )
            )

        vm_name = self.get_vsphere_sdk_provider().deploy_ovf(
            node_config["frozen_vm"]["library_item"],
            vm_name_target,
            resource_pool_id,
            node_config.get("host_id"),
            datastore_id,
        )
        return self.ensure_frozen_vm_status(vm_name)

    def ensure_frozen_vms_status(self, reource_pool_name):
        rp_obj = self.get_pyvmomi_sdk_provider().get_pyvmomi_obj(
            [vim.ResourcePool], reource_pool_name
        )

        vms = rp_obj.vm
        # Known "issue": if there are some other VMs manually created in this resource
        # pool, it will also be handled by wait_until_vm_is_frozen, e.g., be turned on.
        for vm in vms:
            self.ensure_frozen_vm_status(vm.name)

    def create_new_or_fetch_existing_frozen_vms(self, node_config):
        frozen_vm_obj = None
        frozen_vm_config = node_config["frozen_vm"]
        frozen_vm_resource_pool_name = frozen_vm_config.get("resource_pool")
        if frozen_vm_resource_pool_name and not self.frozen_vm_scheduler:
            self.frozen_vm_scheduler = SchedulerFactory.get_scheduler(
                self.get_pyvmomi_sdk_provider(), frozen_vm_resource_pool_name
            )

        # If library_item is present then create new frozen VM(s)
        # The logic under the if block will only be executed during creating the head
        # node. When creating the worker node, the frozen VMs must have been existing.
        # will never need to be deployed from OVF.
        if frozen_vm_config.get("library_item"):
            # If resource_pool config is present then create frozen VMs on each
            # host and put them in the specified resource pool.
            if frozen_vm_resource_pool_name:
                self.create_frozen_vm_on_each_host(
                    node_config,
                    frozen_vm_config.get("name", "frozen-vm"),
                    frozen_vm_resource_pool_name,
                )
            # If resource_pool config is not present then create a frozen VM
            # with name as specified.
            else:
                frozen_vm_obj = self.create_frozen_vm_from_ovf(
                    node_config, frozen_vm_config["name"]
                )

        # If library_item config is not present then select already existing
        # frozen VM.
        else:
            # If resource_pool is present, select a frozen VM out of all those
            # present in the resource pool specified.
            if frozen_vm_resource_pool_name:
                self.ensure_frozen_vms_status(frozen_vm_resource_pool_name)
            # If resource_pool is not present then select the frozen VM with
            # name as specified.
            else:
                frozen_vm_name = frozen_vm_config.get("name", "frozen-vm")
                frozen_vm_obj = self.ensure_frozen_vm_status(frozen_vm_name)

        return frozen_vm_obj

    def _create_node(self, node_config, tags, count):
        created_nodes_dict = {}
        exception_happened = False
        frozen_vm_obj = self.create_new_or_fetch_existing_frozen_vms(node_config)

        # The nodes are named as follows:
        # ray-<cluster-name>-head-<uuid> for the head node
        # ray-<cluster-name>-worker-<uuid> for the worker nodes
        vm_names = [
            "{}-{}".format(tags[TAG_RAY_NODE_NAME], str(uuid.uuid4()))
            for _ in range(count)
        ]

        requested_gpu_num = 0
        if "resources" in node_config:
            resources = node_config["resources"]
            requested_gpu_num = resources.get("GPU", 0)
        vm_2_gpu_cards_map = {}
        gpu_cards_map_array = []

        is_dynamic = is_dynamic_passthrough(node_config)

        if requested_gpu_num > 0:
            # Fetch all available frozen-vm + gpu-cards info into
            # `get_vm_2_gpu_cards_map`
            if "resource_pool" in node_config["frozen_vm"]:
                # This means that we have multiple frozen_vms, and we need to gather
                # the information of the GPUs of each frozen VM's ESXi host.
                vm_2_gpu_cards_map = get_vm_2_gpu_cards_map(
                    self.get_pyvmomi_sdk_provider(),
                    node_config["frozen_vm"]["resource_pool"],
                    requested_gpu_num,
                    is_dynamic,
                )
            else:
                # This means that we have only one frozen VM, we just need to put the
                # information of the only ESXi host's GPU info into the map
                gpu_cards = get_gpu_cards_from_vm(
                    frozen_vm_obj, requested_gpu_num, is_dynamic
                )
                vm_2_gpu_cards_map[frozen_vm_obj.name] = gpu_cards

            # Split `vm_2_gpu_ids_map` for nodes, check the comments inside function
            # split_vm_2_gpu_ids_map to get to know why we need do this.
            gpu_cards_map_array = split_vm_2_gpu_cards_map(
                vm_2_gpu_cards_map, requested_gpu_num
            )
            if len(gpu_cards_map_array) < count:
                logger.warning(
                    f"The GPU card number cannot fulfill {count} Ray nodes, "
                    f"but can fulfill {len(gpu_cards_map_array)} Ray nodes. "
                    f"gpu_cards_map_array: {gpu_cards_map_array}"
                )
                # Avoid invalid index when accessing gpu_cards_map_array[i]
                set_gpu_placeholder(
                    gpu_cards_map_array, count - len(gpu_cards_map_array)
                )
        else:
            # CPU node: Avoid invalid index when accessing gpu_cards_map_array[i]
            set_gpu_placeholder(gpu_cards_map_array, count)

        def get_frozen_vm_name():
            if self.frozen_vm_scheduler:
                return self.frozen_vm_scheduler.next_frozen_vm().name
            else:
                return frozen_vm_obj.name

        with ThreadPoolExecutor(max_workers=count) as executor:
            futures = [
                executor.submit(
                    self.create_instant_clone_node,
                    get_frozen_vm_name(),
                    vm_names[i],
                    node_config,
                    tags,
                    gpu_cards_map_array[i],
                )
                for i in range(count)
            ]
        failed_vms_index = []
        for i in range(count):
            future = futures[i]
            try:
                vm = future.result()
                k = Constants.VSPHERE_NODE_STATUS
                v = Constants.VsphereNodeStatus.CREATED.value
                vsphere_node_created_tag = {k: v}
                # if create succeed, we add a "created" tag
                self.set_node_tags(vm.vm, vsphere_node_created_tag)
                created_nodes_dict[vm.name] = vm
                logger.info(f"VM {vm.name} is created.")
            except Exception as e:
                logger.error(
                    "Exception occurred while creating or tagging VMs {}".format(e)
                )
                exception_happened = True
                failed_vms_index.append(i)
                logger.error(f"Failed creating VM {vm_names[i]}")

        # We clean up the created VMs if any exception occurs to them
        if exception_happened:
            with ThreadPoolExecutor(max_workers=count) as executor:
                futures = [
                    executor.submit(
                        self.get_vsphere_sdk_provider().delete_vm_by_name, vm_names[i]
                    )
                    for i in failed_vms_index
                ]
            for future in futures:
                _ = future.result()

            if len(failed_vms_index) == count:
                raise RuntimeError("Failed creating all VMs, exiting!")

        return created_nodes_dict

    def terminate_node(self, node_id):
        if node_id is None:
            return

        self.get_vsphere_sdk_provider().delete_vm_by_id(node_id)

        with self.tag_cache_lock:
            if node_id in self.tag_cache:
                self.tag_cache.pop(node_id)

    def terminate_nodes(self, node_ids):
        if not node_ids:
            return

        for node_id in node_ids:
            self.terminate_node(node_id)
