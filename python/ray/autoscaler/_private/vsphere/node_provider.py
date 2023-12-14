import copy
import logging
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from threading import RLock
from typing import Any, Dict

import com.vmware.vapi.std.errors_client as ErrorClients
from com.vmware.cis.tagging_client import CategoryModel
from com.vmware.content.library_client import Item
from com.vmware.vapi.std_client import DynamicID
from com.vmware.vcenter.ovf_client import DiskProvisioningType, LibraryItem
from com.vmware.vcenter.vm.hardware_client import Cpu, Memory
from com.vmware.vcenter.vm_client import Power as HardPower
from com.vmware.vcenter_client import VM, Host, ResourcePool
from pyVmomi import vim

from ray.autoscaler._private.cli_logger import cli_logger
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


def is_powered_on_or_creating(power_status, vsphere_node_status):
    return (
        power_status.state == HardPower.State.POWERED_OFF
        and vsphere_node_status == Constants.VsphereNodeStatus.CREATING.value
    ) or (power_status.state == HardPower.State.POWERED_ON)


def vsphere_tag_to_kv_pair(vsphere_tag):
    if ":" in vsphere_tag:
        items = vsphere_tag.split(":")
        if len(items) == 2:
            return items
    return None


def kv_pair_to_vsphere_tag(key, value):
    return "{}:{}".format(key, value)


class VsphereNodeProvider(NodeProvider):
    max_terminate_nodes = 1000

    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        self.frozen_vm_scheduler = None
        vsphere_credentials = provider_config["vsphere_config"]["credentials"]
        self.vsphere_credentials = vsphere_credentials
        self.vsphere_config = provider_config["vsphere_config"]

        # The below cache will be a map, whose key is the Ray node and the value will
        # be a list of vSphere tags one that node. The reason for this cache is to
        # avoid calling the vSphere API again when the autoscaler tries to read the tags
        # The cache will be filled when a Ray node is created and tagged.
        self.tag_cache = {}
        self.tag_cache_lock = threading.Lock()
        self.lock = RLock()

    def get_vsphere_sdk_client(self):
        return VsphereSdkProvider(
            self.vsphere_credentials["server"],
            self.vsphere_credentials["user"],
            self.vsphere_credentials["password"],
            Constants.SessionType.UNVERIFIED,
        ).vsphere_sdk_client

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

    def get_matched_tags(self, tag_filters, dynamic_id):
        """
        tag_filters will be a dict like {"tag_key1": "val1", "tag_key2": "val2"}
        dynamic_id will be the vSphere object id
        This function will list all the attached tags of the vSphere object, convert
        the string formatted tag to k,v formatted. Then compare the attached tags to
        the ones in the filters.
        Return all the matched tags and all the tags the vSphere object has.
        vsphere_tag_to_kv_pair will ignore the tags not convertable to k,v pairs.
        """
        matched_tags = {}
        all_tags = {}

        for tag_id in self.list_vm_tags(dynamic_id):
            vsphere_vm_tag = (
                self.get_vsphere_sdk_client().tagging.Tag.get(tag_id=tag_id).name
            )
            tag_key_value = vsphere_tag_to_kv_pair(vsphere_vm_tag)
            if tag_key_value:
                tag_key, tag_value = tag_key_value[0], tag_key_value[1]

                if tag_key in tag_filters and tag_value == tag_filters[tag_key]:
                    matched_tags[tag_key] = tag_value

                all_tags[tag_key] = tag_value

        return matched_tags, all_tags

    def non_terminated_nodes(self, tag_filters):
        """
        This function is going to find all the running vSphere VMs created by Ray via
        the tag filters, the VMs should either be powered_on or be powered_off but has
        a tag "vsphere-node-status:creating"
        """
        with self.lock:
            nodes = []
            vms = self.get_vsphere_sdk_client().vcenter.VM.list()
            filters = tag_filters.copy()
            if TAG_RAY_CLUSTER_NAME not in tag_filters:
                filters[TAG_RAY_CLUSTER_NAME] = self.cluster_name
            for vm in vms:
                vm_id = vm.vm
                dynamic_id = DynamicID(type=Constants.TYPE_OF_RESOURCE, id=vm.vm)

                matched_tags, all_tags = self.get_matched_tags(filters, dynamic_id)
                # Update the tag cache with latest tags
                with self.tag_cache_lock:
                    self.tag_cache[vm_id] = all_tags

                if len(matched_tags) == len(filters):
                    # All the tags in the filters are matched on this vm
                    power_status = self.get_vsphere_sdk_client().vcenter.vm.Power.get(
                        vm_id
                    )

                    # Return VMs in powered-on and creating state
                    vsphere_node_status = all_tags.get(Constants.VSPHERE_NODE_STATUS)
                    if is_powered_on_or_creating(power_status, vsphere_node_status):
                        nodes.append(vm_id)

            logger.debug(f"Non terminated nodes are {nodes}")
            return nodes

    def is_running(self, node_id):
        return self.get_pyvmomi_sdk_provider().is_vm_power_on(node_id)

    def is_terminated(self, node_id):
        if self.get_pyvmomi_sdk_provider().is_vm_power_on(node_id):
            return False
        else:
            vns = Constants.VSPHERE_NODE_STATUS
            matched_tags, _ = self.get_matched_tags(
                {vns: Constants.VsphereNodeStatus.CREATING.value},
                DynamicID(type=Constants.TYPE_OF_RESOURCE, id=node_id),
            )
            if matched_tags:
                # If the node is not powered on but has the creating tag, then it could
                # be under reconfiguration, such as plugging the GPU. In this case we
                # should consider the node is not terminated, it will be turned on later
                return False
        return True

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
        with self.lock:
            category_id = self.get_category()
            if not category_id:
                category_id = self.create_category()

            for key, value in tags.items():

                tag = kv_pair_to_vsphere_tag(key, value)
                tag_id = self.get_tag(tag, category_id)
                if not tag_id:
                    tag_id = self.create_node_tag(tag, category_id)

                # If a tag with a key is present on the VM, then remove it
                # before updating the key with a new value.
                self.remove_tag_from_vm(key, node_id)

                logger.debug(f"Attaching tag {tag} to {node_id}")
                self.attach_tag(node_id, Constants.TYPE_OF_RESOURCE, tag_id=tag_id)

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

    def attach_tag(self, vm_id, resource_type, tag_id):
        dynamic_id = DynamicID(type=resource_type, id=vm_id)
        try:
            self.get_vsphere_sdk_client().tagging.TagAssociation.attach(
                tag_id, dynamic_id
            )
            logger.debug(f"Tag {tag_id} attached on VM {dynamic_id}")
        except Exception as e:
            logger.warning(f"Check that the tag is attachable to {resource_type}")
            raise e

    # Example: If a tag called node-status:initializing is present on the VM.
    # If we would like to add a new value called finished with the node-status key.
    # We'll need to delete the older tag node-status:initializing first before creating
    # node-status:finished
    def remove_tag_from_vm(self, tag_key_to_remove, vm_id):
        dynamic_id = DynamicID(type=Constants.TYPE_OF_RESOURCE, id=vm_id)

        # List all the tags present on the VM.
        for tag_id in self.list_vm_tags(dynamic_id):
            vsphere_vm_tag = (
                self.get_vsphere_sdk_client().tagging.Tag.get(tag_id=tag_id).name
            )
            tag_key_value = vsphere_tag_to_kv_pair(vsphere_vm_tag)
            tag_key = tag_key_value[0] if tag_key_value else None
            if tag_key == tag_key_to_remove:
                # Remove the tag matching the key passed.
                logger.debug("Removing tag {} from the VM {}".format(tag_key, vm_id))
                self.get_vsphere_sdk_client().tagging.TagAssociation.detach(
                    tag_id, dynamic_id
                )
                break

    def list_vm_tags(self, vm_dynamic_id):
        return self.get_vsphere_sdk_client().tagging.TagAssociation.list_attached_tags(
            vm_dynamic_id
        )

    # This method is used to tag VMs as soon as they show up on vCenter.
    def tag_vm(self, vm_name, tags):
        names = {vm_name}
        start = time.time()
        # In most cases the instant clone VM will show up in several seconds.
        # When the vCenter Server is busy, the time could be longer. We set a 120
        # seconds timeout here. Because it's not used anywhere else, we don't make
        # it as a formal constant.
        while time.time() - start < Constants.CREATING_TAG_TIMEOUT:
            time.sleep(0.5)
            vms = self.get_vsphere_sdk_client().vcenter.VM.list(
                VM.FilterSpec(names=names)
            )

            if len(vms) == 1:
                vm_id = vms[0].vm
                self.set_node_tags(vm_id, tags)
                return
            elif len(vms) > 1:
                # This should never happen
                raise RuntimeError("Duplicated VM with name {} found.".format(vm_name))

        raise RuntimeError("VM {} could not be found.".format(vm_name))

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
        threading.Thread(target=self.tag_vm, args=(target_vm_name, tags)).start()
        self.get_pyvmomi_sdk_provider().instance_clone_vm(
            parent_vm_name,
            target_vm_name,
            node_config.get("resource_pool"),
            node_config.get("datastore"),
        )

        target_vm_id = self.get_pyvmomi_sdk_provider().name_to_id(
            [vim.VirtualMachine], target_vm_name
        )

        vsphere_sdk_vm_obj = self.get_vsphere_sdk_vm_obj(target_vm_id)
        if "CPU" in resources:
            # Update number of CPUs
            update_spec = Cpu.UpdateSpec(count=resources["CPU"])
            logger.debug(
                "vm.hardware.Cpu.update({}, {})".format(target_vm_name, update_spec)
            )
            self.get_vsphere_sdk_client().vcenter.vm.hardware.Cpu.update(
                vsphere_sdk_vm_obj.vm, update_spec
            )

        if "Memory" in resources:
            # Update Memory
            update_spec = Memory.UpdateSpec(size_mib=resources["Memory"])
            logger.debug(
                "vm.hardware.Memory.update({}, {})".format(target_vm_name, update_spec)
            )
            self.get_vsphere_sdk_client().vcenter.vm.hardware.Memory.update(
                vsphere_sdk_vm_obj.vm, update_spec
            )

        if to_be_plugged_gpu:
            is_dynamic = is_dynamic_passthrough(node_config)
            plug_gpu_cards_to_vm(
                self.get_pyvmomi_sdk_provider(),
                target_vm_name,
                to_be_plugged_gpu,
                is_dynamic,
            )

        return vsphere_sdk_vm_obj

    def create_frozen_vm_on_each_host(self, node_config, name, resource_pool_name):
        """
        This function helps to deploy a frozen VM on each ESXi host of the resource pool
        specified in the frozen VM config under the vSphere config section. So that we
        can spread the Ray nodes on different ESXi host at the beginning.
        """
        exception_happened = False
        vm_names = []

        resource_pool_obj = self.get_pyvmomi_sdk_provider().get_pyvmomi_obj(
            [vim.ResourcePool], resource_pool_name
        )

        cluster = resource_pool_obj.parent.parent

        host_filter_spec = Host.FilterSpec(clusters={cluster._moId})
        hosts = self.get_vsphere_sdk_client().vcenter.Host.list(host_filter_spec)

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
                    executor.submit(self.delete_vm, vm_names[i])
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
            rp_filter_spec = ResourcePool.FilterSpec(
                names={node_config["frozen_vm"]["resource_pool"]}
            )
            resource_pool_summaries = (
                self.get_vsphere_sdk_client().vcenter.ResourcePool.list(rp_filter_spec)
            )
            if not resource_pool_summaries:
                raise ValueError(
                    "Resource pool with name '{}' not found".format(rp_filter_spec)
                )
            resource_pool_id = resource_pool_summaries[0].resource_pool
            logger.debug("Resource pool ID: {}".format(resource_pool_id))
        else:
            cluster_name = node_config.get("frozen_vm").get("cluster")
            if not cluster_name:
                raise ValueError(
                    "The cluster name must be provided when deploying a single frozen"
                    " VM from OVF"
                )
            cluster_mo = self.get_pyvmomi_sdk_provider().get_pyvmomi_obj(
                [vim.ClusterComputeResource], cluster_name
            )
            node_config["host_id"] = cluster_mo.host[0]._moId
            resource_pool_id = cluster_mo.resourcePool._moId

        # Find and use the OVF library item defined in the manifest file.
        lib_item = node_config["frozen_vm"]["library_item"]
        find_spec = Item.FindSpec(name=lib_item)
        item_ids = self.get_vsphere_sdk_client().content.library.Item.find(find_spec)

        if len(item_ids) < 1:
            raise ValueError(
                "Content library items with name '{}' not found".format(lib_item),
            )
        if len(item_ids) > 1:
            logger.warning(
                "Unexpected: found multiple content library items with name \
                '{}'".format(
                    lib_item
                )
            )

        lib_item_id = item_ids[0]
        deployment_target = LibraryItem.DeploymentTarget(
            resource_pool_id=resource_pool_id,
            host_id=node_config.get("host_id"),
        )
        ovf_summary = self.get_vsphere_sdk_client().vcenter.ovf.LibraryItem.filter(
            ovf_library_item_id=lib_item_id, target=deployment_target
        )
        logger.info("Found an OVF template: {} to deploy.".format(ovf_summary.name))

        # Build the deployment spec
        deployment_spec = LibraryItem.ResourcePoolDeploymentSpec(
            name=vm_name_target,
            annotation=ovf_summary.annotation,
            accept_all_eula=True,
            network_mappings=None,
            storage_mappings=None,
            storage_provisioning=DiskProvisioningType.thin,
            storage_profile_id=None,
            locale=None,
            flags=None,
            additional_parameters=None,
            default_datastore_id=datastore_id,
        )

        # Deploy the ovf template
        result = self.get_vsphere_sdk_client().vcenter.ovf.LibraryItem.deploy(
            lib_item_id,
            deployment_target,
            deployment_spec,
            client_token=str(uuid.uuid4()),
        )

        logger.debug("result: {}".format(result))
        # The type and ID of the target deployment is available in the
        # deployment result.
        if len(result.error.errors) > 0:
            for error in result.error.errors:
                logger.error("OVF error: {}".format(error))

            raise ValueError(
                "OVF deployment failed for VM {}, reason: {}".format(
                    vm_name_target, result
                )
            )

        logger.info(
            'Deployment successful. VM Name: "{}", ID: "{}"'.format(
                vm_name_target, result.resource_id.id
            )
        )
        error = result.error
        if error is not None:
            for warning in error.warnings:
                logger.warning("OVF warning: {}".format(warning.message))

        vm_id = result.resource_id.id
        vm = self.get_vsphere_sdk_vm_obj(vm_id)

        return self.ensure_frozen_vm_status(vm.name)

    def delete_vm(self, vm_name):
        vms = self.get_vsphere_sdk_client().vcenter.VM.list(
            VM.FilterSpec(names={vm_name})
        )

        if len(vms) > 0:
            vm_id = vms[0].vm

            status = self.get_vsphere_sdk_client().vcenter.vm.Power.get(vm_id)

            if status.state != HardPower.State.POWERED_OFF:
                self.get_vsphere_sdk_client().vcenter.vm.Power.stop(vm_id)

            logger.info("Deleting VM {}".format(vm_name))
            self.get_vsphere_sdk_client().vcenter.VM.delete(vm_id)

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
                frozen_vm_obj = self.frozen_vm_scheduler.next_frozen_vm()

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
                frozen_vm_obj = self.frozen_vm_scheduler.next_frozen_vm()
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
                    executor.submit(self.delete_vm, vm_names[i])
                    for i in failed_vms_index
                ]
            for future in futures:
                _ = future.result()

            if len(failed_vms_index) == count:
                raise RuntimeError("Failed creating all VMs, exiting!")

        return created_nodes_dict

    def get_tag(self, tag_name, category_id):
        for id in self.get_vsphere_sdk_client().tagging.Tag.list_tags_for_category(
            category_id
        ):
            if tag_name == self.get_vsphere_sdk_client().tagging.Tag.get(id).name:
                return id
        return None

    def create_node_tag(self, ray_node_tag, category_id):
        logger.debug(f"Creating {ray_node_tag} tag")
        tag_spec = self.get_vsphere_sdk_client().tagging.Tag.CreateSpec(
            ray_node_tag, "Ray node tag", category_id
        )
        tag_id = None
        try:
            tag_id = self.get_vsphere_sdk_client().tagging.Tag.create(tag_spec)
        except ErrorClients.Unauthorized as e:
            cli_logger.abort(f"Unauthorized to create the tag. Exception: {e}")
        except Exception as e:
            cli_logger.abort(e)

        logger.debug(f"Tag {tag_id} created")
        return tag_id

    def get_category(self):
        for id in self.get_vsphere_sdk_client().tagging.Category.list():
            if (
                self.get_vsphere_sdk_client().tagging.Category.get(id).name
                == Constants.NODE_CATEGORY
            ):
                return id
        return None

    def create_category(self):
        # Create RAY_NODE category. This category is associated with VMs and supports
        # multiple tags e.g. "Ray-Head-Node, Ray-Worker-Node-1 etc."
        cli_logger.info(f"Creating {Constants.NODE_CATEGORY} category")
        category_spec = self.get_vsphere_sdk_client().tagging.Category.CreateSpec(
            name=Constants.NODE_CATEGORY,
            description="Identifies Ray head node and worker nodes",
            cardinality=CategoryModel.Cardinality.MULTIPLE,
            associable_types=set(),
        )
        category_id = None

        try:
            category_id = self.get_vsphere_sdk_client().tagging.Category.create(
                category_spec
            )
        except ErrorClients.Unauthorized as e:
            cli_logger.abort(f"Unauthorized to create the category. Exception: {e}")
        except Exception as e:
            cli_logger.abort(e)

        cli_logger.info(f"Category {category_id} created")

        return category_id

    def terminate_node(self, node_id):
        if node_id is None:
            return

        status = self.get_vsphere_sdk_client().vcenter.vm.Power.get(node_id)

        if status.state != HardPower.State.POWERED_OFF:
            self.get_vsphere_sdk_client().vcenter.vm.Power.stop(node_id)
            logger.debug("vm.Power.stop({})".format(node_id))

        self.get_vsphere_sdk_client().vcenter.VM.delete(node_id)
        logger.info("Deleted vm {}".format(node_id))
        with self.tag_cache_lock:
            if node_id in self.tag_cache:
                self.tag_cache.pop(node_id)

    def terminate_nodes(self, node_ids):
        if not node_ids:
            return

        for node_id in node_ids:
            self.terminate_node(node_id)

    def get_vsphere_sdk_vm_obj(self, vm_id):
        """Get the vm object by vSphere SDK with the vm id, such as 'vm-12'"""
        vms = self.get_vsphere_sdk_client().vcenter.VM.list(VM.FilterSpec(vms={vm_id}))
        if len(vms) == 0:
            logger.warning("VM with name ({}) not found by vSphere sdk".format(vm_id))
            return None
        return vms[0]
