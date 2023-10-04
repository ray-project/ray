import copy
import ipaddress
import logging
import threading
import time
import uuid
from collections import defaultdict
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
from pyVim.task import WaitForTask
from pyVmomi import vim

from ray.autoscaler._private.cli_logger import cli_logger
from ray.autoscaler._private.vsphere.config import bootstrap_vsphere
from ray.autoscaler._private.vsphere.sdk_provider import VmwSdkProviderFactory
from ray.autoscaler._private.vsphere.utils import Constants
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
        self.cache_stopped_nodes = provider_config.get("cache_stopped_nodes", True)
        vsphere_credentials = provider_config["vsphere_config"]["credentials"]
        self.vsphere_credentials = vsphere_credentials
        self.vsphere_config = provider_config["vsphere_config"]

        self.vsphere_sdk_provider = VmwSdkProviderFactory(
            vsphere_credentials["server"],
            vsphere_credentials["user"],
            vsphere_credentials["password"],
            VmwSdkProviderFactory.ClientType.AUTOMATION_SDK,
        ).sdk_provider
        self.vsphere_sdk_client = self.vsphere_sdk_provider.vsphere_sdk_client
        self.pyvmomi_sdk_provider = VmwSdkProviderFactory(
            vsphere_credentials["server"],
            vsphere_credentials["user"],
            vsphere_credentials["password"],
            VmwSdkProviderFactory.ClientType.PYVMOMI_SDK,
        ).sdk_provider

        # Tags that we believe to actually be on VM.
        self.tag_cache = {}
        # Tags that we will soon upload.
        self.tag_cache_pending = defaultdict(dict)
        self.tag_cache_lock = threading.Lock()
        self.lock = RLock()

        # Cache of node objects from the last nodes() call. This avoids
        # excessive DescribeInstances requests.
        self.cached_nodes: Dict[str, VM] = {}

    def check_frozen_vm_status(self, frozen_vm_name):
        """
        This function will help check if the frozen VM with the specific name is
        existing and in the frozen state. If the frozen VM is existing and off, this
        function will also help to power on the frozen VM and wait until it is frozen.
        """
        vm = self.pyvmomi_sdk_provider.get_pyvmomi_obj(
            [vim.VirtualMachine], frozen_vm_name
        )
        if vm is None:
            raise ValueError(
                "The frozen VM {} doesn't exist on vSphere, please contact the VI "
                "admin".format(frozen_vm_name)
            )
        logger.info(f"Found frozen VM with name: {vm._moId}")

        if vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOff:
            logger.debug(f"Frozen VM {vm._moId} is off. Powering it ON")
            WaitForTask(vm.PowerOnVM_Task())

        # Make sure it is frozen status
        self.wait_until_vm_is_frozen(vm)

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

        for tag_id in self.vsphere_sdk_client.tagging.TagAssociation.list_attached_tags(
            dynamic_id
        ):
            vsphere_vm_tag = self.vsphere_sdk_client.tagging.Tag.get(tag_id=tag_id).name
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
            vms = self.vsphere_sdk_client.vcenter.VM.list()
            filters = tag_filters.copy()
            if TAG_RAY_CLUSTER_NAME not in tag_filters:
                filters[TAG_RAY_CLUSTER_NAME] = self.cluster_name
            for vm in vms:
                vm_id = vm.vm
                dynamic_id = DynamicID(type=Constants.TYPE_OF_RESOURCE, id=vm.vm)

                matched_tags, all_tags = self.get_matched_tags(filters, dynamic_id)
                # Update the tag cache with latest tags
                self.tag_cache[vm_id] = all_tags

                if len(matched_tags) == len(filters):
                    # All the tags in the filters are matched on this vm
                    power_status = self.vsphere_sdk_client.vcenter.vm.Power.get(vm_id)

                    # Return VMs in powered-on and creating state
                    vsphere_node_status = all_tags.get(Constants.VSPHERE_NODE_STATUS)
                    if is_powered_on_or_creating(power_status, vsphere_node_status):
                        nodes.append(vm_id)
                        # refresh cached_nodes with latest information e.g external ip
                        self.cached_nodes[vm_id] = vm

            logger.debug(f"Non terminated nodes are {nodes}")
            return nodes

    def is_running(self, node_id):
        node = self._get_cached_node(node_id)
        return node.power_state in {HardPower.State.POWERED_ON}

    def is_terminated(self, node_id):
        node = self._get_cached_node(node_id)
        return node.power_state not in {HardPower.State.POWERED_ON}

    def node_tags(self, node_id):
        with self.tag_cache_lock:
            d1 = self.tag_cache[node_id]
            return dict(d1)

    def external_ip(self, node_id):
        # Return the external IP of the VM

        vm = self.vsphere_sdk_client.vcenter.vm.guest.Identity.get(node_id)
        try:
            _ = ipaddress.IPv4Address(vm.ip_address)
            logger.debug("Fetch IP {} for VM {}".format(vm.ip_address, vm))
        except ipaddress.AddressValueError:
            # vSphere SDK could return IPv6 address when the VM is just booted. We
            # just return None in this case because the Ray doesn't support IPv6
            # address yet When the next time external_ip is called, we could return
            # the IPv4 address
            return None
        return vm.ip_address

    def internal_ip(self, node_id):
        # Currently vSphere VMs do not show an internal IP. So we just return the
        # external IP
        return self.external_ip(node_id)

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
        reused_nodes_dict = {}
        reuse_nodes = list()
        reuse_node_ids = []
        number_of_reused_nodes = 0
        # Try to reuse previously stopped nodes with compatible configs
        if self.cache_stopped_nodes:
            vms = self.vsphere_sdk_client.vcenter.VM.list(
                VM.FilterSpec(
                    power_states={
                        HardPower.State.POWERED_OFF,
                        HardPower.State.SUSPENDED,
                    },
                    clusters={self.cluster_name},
                )
            )

            # Select POWERED_OFF or SUSENDED vms which has ray-node-type,
            # ray-launch-config, ray-user-node-type tags
            for vm in vms:
                if number_of_reused_nodes >= count:
                    break

                vm_id = vm.name
                dynamic_id = DynamicID(type=Constants.TYPE_OF_RESOURCE, id=vm.vm)
                matched_tags, _ = self.get_matched_tags(filters, dynamic_id)
                if len(matched_tags) == len(filters):
                    reuse_nodes.append(vm)
                    reused_nodes_dict[vm_id] = vm
                    # Tag needs vm.vm and not vm.name as id
                    reuse_node_ids.append(vm.vm)
                    number_of_reused_nodes += 1

            if reuse_nodes:
                logger.info(
                    f"Reusing nodes {reuse_node_ids}. "
                    "To disable reuse, set `cache_stopped_nodes: False` "
                    "under `provider` in the cluster configuration."
                )
                for node_id in reuse_node_ids:
                    logger.debug("Powering on VM with id {}".format(node_id))
                    self.vsphere_sdk_client.vcenter.vm.Power.start(node_id)
                to_be_launched_node_count -= len(reuse_node_ids)

        created_nodes_dict = {}
        if to_be_launched_node_count > 0:
            created_nodes_dict = self._create_node(
                node_config, filters, to_be_launched_node_count
            )

        all_created_nodes = reused_nodes_dict
        all_created_nodes.update(created_nodes_dict)

        # Set tags on the nodes that were reused
        for node_id in reuse_node_ids:
            self.set_node_tags(node_id, filters)

        return all_created_nodes

    def get_vm(self, node_id):
        """
        Return the VM summary object
        Note: The method assumes that there is only one vm with the mentioned name.
        """
        vm = self._get_cached_node(node_id)
        logger.debug(f"VM {node_id} found")

        return vm

    def attach_tag(self, vm_id, resource_type, tag_id):
        dynamic_id = DynamicID(type=resource_type, id=vm_id)
        try:
            self.vsphere_sdk_client.tagging.TagAssociation.attach(tag_id, dynamic_id)
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
        for tag_id in self.vsphere_sdk_client.tagging.TagAssociation.list_attached_tags(
            dynamic_id
        ):
            vsphere_vm_tag = self.vsphere_sdk_client.tagging.Tag.get(tag_id=tag_id).name
            tag_key_value = vsphere_tag_to_kv_pair(vsphere_vm_tag)
            tag_key = tag_key_value[0] if tag_key_value else None
            if tag_key == tag_key_to_remove:
                # Remove the tag matching the key passed.
                logger.debug("Removing tag {} from the VM {}".format(tag_key, vm_id))
                self.vsphere_sdk_client.tagging.TagAssociation.detach(
                    tag_id, dynamic_id
                )
                break

    def get_frozen_vm_obj(self):
        vm = self.pyvmomi_sdk_provider.get_pyvmomi_obj(
            [vim.VirtualMachine], self.frozen_vm_name
        )
        return vm

    def choose_frozen_vm_obj(self):
        vm = self.scheduler_factory.get_scheduler().choose_frozen_vm()
        return vm

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
            vms = self.vsphere_sdk_client.vcenter.VM.list(VM.FilterSpec(names=names))

            if len(vms) == 1:
                vm_id = vms[0].vm
                self.set_node_tags(vm_id, tags)
                return
            elif len(vms) > 1:
                # This should never happen
                raise RuntimeError("Duplicated VM with name {} found.".format(vm_name))

        raise RuntimeError("VM {} could not be found.".format(vm_name))

    def create_instant_clone_node(self, source_vm, vm_name_target, node_config, tags):
        # If resource pool is not provided in the config yaml, then the resource pool
        # of the frozen VM will also be the resource pool of the new VM.
        resource_pool = (
            self.pyvmomi_sdk_provider.get_pyvmomi_obj(
                [vim.ResourcePool], node_config["resource_pool"]
            )
            if "resource_pool" in node_config and node_config["resource_pool"]
            else None
        )
        # If datastore is not provided in the config yaml, then the datastore
        # of the frozen VM will also be the resource pool of the new VM.
        datastore = (
            self.pyvmomi_sdk_provider.get_pyvmomi_obj(
                [vim.Datastore], node_config["datastore"]
            )
            if "datastore" in node_config and node_config["datastore"]
            else None
        )
        resources = node_config["resources"]
        vm_relocate_spec = vim.vm.RelocateSpec(
            pool=resource_pool,
            datastore=datastore,
        )
        instant_clone_spec = vim.vm.InstantCloneSpec(
            name=vm_name_target, location=vm_relocate_spec
        )

        logger.debug("source_vm={}".format(source_vm))

        # If there is only one frozen VM then the caller of create_instant_clone_node
        # will pass a frozen VM obj has the source_vm. If there is a resource pool of
        # frozen VMs, then the source_vm passed by the caller will be None. That is why
        # we need to call self.choose_frozen_vm_obj to get a frozen VM obj.
        parent_vm = source_vm if source_vm else self.choose_frozen_vm_obj()

        logger.debug("parent_vm={}".format(parent_vm))

        tags[Constants.VSPHERE_NODE_STATUS] = Constants.VsphereNodeStatus.CREATING.value
        threading.Thread(target=self.tag_vm, args=(vm_name_target, tags)).start()
        WaitForTask(parent_vm.InstantClone_Task(spec=instant_clone_spec))

        cloned_vm = self.pyvmomi_sdk_provider.get_pyvmomi_obj(
            [vim.VirtualMachine], vm_name_target
        )

        # Get VM ID
        vm_id = cloned_vm._moId

        vm = self.get_vm(vm_id)

        if "CPU" in resources:
            # Update number of CPUs
            update_spec = Cpu.UpdateSpec(count=resources["CPU"])
            logger.debug("vm.hardware.Cpu.update({}, {})".format(vm_id, update_spec))
            self.vsphere_sdk_client.vcenter.vm.hardware.Cpu.update(vm_id, update_spec)

        if "Memory" in resources:
            # Update Memory
            update_spec = Memory.UpdateSpec(size_mib=resources["Memory"])

            logger.debug("vm.hardware.Memory.update({}, {})".format(vm_id, update_spec))
            self.vsphere_sdk_client.vcenter.vm.hardware.Memory.update(
                vm_id, update_spec
            )

        return vm

    def create_frozen_vm_on_each_host(self, node_config, name, wait_until_frozen=False):
        """
        This function helps to deploy a frozen VM on each ESXi host of the resource pool
        specified in the frozen VM config under the vSphere config section. So that we
        can spread the Ray nodes on different ESXi host at the beginning.
        """
        exception_happened = False
        vm_names = []

        res_pool = self.pyvmomi_sdk_provider.get_pyvmomi_obj(
            [vim.ResourcePool], node_config["frozen_vm"]["resource_pool"]
        )
        # In vSphere, for any user-created resource pool, the cluster object is the
        # grandparent of the object.
        cluster = res_pool.parent.parent

        host_filter_spec = Host.FilterSpec(clusters={cluster._moId})
        hosts = self.vsphere_sdk_client.vcenter.Host.list(host_filter_spec)

        futures_frozen_vms = []
        with ThreadPoolExecutor(max_workers=len(hosts)) as executor:
            for host in hosts:
                node_config_frozen_vm = copy.deepcopy(node_config)
                node_config_frozen_vm["host_id"] = host.host

                frozen_vm_name = "{}-{}".format(name, host.name)
                vm_names.append(frozen_vm_name)

                futures_frozen_vms.append(
                    executor.submit(
                        self.create_frozen_vm_from_ovf,
                        node_config_frozen_vm,
                        frozen_vm_name,
                        wait_until_frozen,
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

    def create_frozen_vm_from_ovf(
        self, node_config, vm_name_target, wait_until_frozen=False
    ):
        resource_pool_id = None
        datastore_name = node_config.get("frozen_vm").get("datastore")
        if not datastore_name:
            raise ValueError(
                "The datastore name must be provided when deploying frozen"
                "VM from OVF"
            )
        datastore_mo = self.pyvmomi_sdk_provider.get_pyvmomi_obj(
            [vim.Datastore], datastore_name
        )
        if not datastore_mo:
            raise ValueError(
                f"Cannot find the vSphere datastore by name {datastore_name}"
            )
        datastore_id = datastore_mo._moId
        if node_config.get("frozen_vm").get("resource_pool"):
            rp_filter_spec = ResourcePool.FilterSpec(
                names={node_config["frozen_vm"]["resource_pool"]}
            )
            resource_pool_summaries = self.vsphere_sdk_client.vcenter.ResourcePool.list(
                rp_filter_spec
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
            cluster_mo = self.pyvmomi_sdk_provider.get_pyvmomi_obj(
                [vim.ClusterComputeResource], cluster_name
            )
            if not cluster_mo:
                raise ValueError(
                    f"Cannot find the vSphere cluster by name {cluster_name}"
                )
            node_config["host_id"] = cluster_mo.host[0]._moId
            resource_pool_id = cluster_mo.resourcePool._moId

        # Find and use the OVF library item defined in the manifest file.
        lib_item = node_config["frozen_vm"]["library_item"]
        find_spec = Item.FindSpec(name=lib_item)
        item_ids = self.vsphere_sdk_client.content.library.Item.find(find_spec)

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
        ovf_summary = self.vsphere_sdk_client.vcenter.ovf.LibraryItem.filter(
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
        result = self.vsphere_sdk_client.vcenter.ovf.LibraryItem.deploy(
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
                logger.error("OVF error: {}".format(result))

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
        self.vm_id = result.resource_id.id
        error = result.error
        if error is not None:
            for warning in error.warnings:
                logger.warning("OVF warning: {}".format(warning.message))

        vm_id = result.resource_id.id

        status = self.vsphere_sdk_client.vcenter.vm.Power.get(vm_id)
        if status.state != HardPower.State.POWERED_ON:
            self.vsphere_sdk_client.vcenter.vm.Power.start(vm_id)
            logger.info("vm.Power.start({})".format(vm_id))

        # Get the created vm object
        vm = self.get_vm(result.resource_id.id)
        vm_mo = self.pyvmomi_sdk_provider.get_pyvmomi_obj([vim.VirtualMachine], vm.name)
        if wait_until_frozen:
            self.wait_until_vm_is_frozen(vm_mo)

        return vm_mo

    def delete_vm(self, vm_name):
        vms = self.vsphere_sdk_client.vcenter.VM.list(VM.FilterSpec(names={vm_name}))

        if len(vms) > 0:
            vm_id = vms[0].vm

            status = self.vsphere_sdk_client.vcenter.vm.Power.get(vm_id)

            if status.state != HardPower.State.POWERED_OFF:
                self.vsphere_sdk_client.vcenter.vm.Power.stop(vm_id)

            logger.info("Deleting VM {}".format(vm_id))
            self.vsphere_sdk_client.vcenter.VM.delete(vm_id)

    def wait_until_vm_is_frozen(self, vm):
        """The function waits until a VM goes into the frozen state."""

        start = time.time()

        while time.time() - start < Constants.VM_FREEZE_TIMEOUT:
            time.sleep(Constants.VM_FREEZE_SLEEP_TIME)
            if vm.runtime.instantCloneFrozen:
                logger.info("VM {} went into frozen state successfully.".format(vm))
                return

        raise RuntimeError("VM {} didn't go into frozen state".format(vm))

    def initialize_frozen_vm_scheduler(self, frozen_vm_config):
        self.frozen_vm_resource_pool_name = frozen_vm_config["resource_pool"]
        self.policy_name = (
            frozen_vm_config["schedule_policy"]
            if "schedule_policy" in self.vsphere_config
            else ""
        )
        self.frozen_vms_resource_pool = self.pyvmomi_sdk_provider.get_pyvmomi_obj(
            [vim.ResourcePool], self.frozen_vm_resource_pool_name
        )

        if self.frozen_vms_resource_pool is None:
            raise RuntimeError(
                f"Resource Pool {self.frozen_vm_resource_pool_name} could not be found."
            )

        # Make all frozen vms on resource pool are power on and frozen
        self.check_frozen_vms_status(self.frozen_vms_resource_pool)

        from ray.autoscaler._private.vsphere.scheduler import SchedulerFactory

        self.scheduler_factory = SchedulerFactory(
            self.frozen_vms_resource_pool, self.policy_name
        )

    def check_frozen_vms_status(self, resource_pool):
        vms = resource_pool.vm
        for vm in vms:
            self.check_frozen_vm_status(vm.name)

    def create_new_or_fetch_existing_frozen_vms(self, node_config):
        frozen_vm_obj = None
        frozen_vm_config = node_config["frozen_vm"]

        # If library_item is present then create new frozen VM(s)
        # The logic under the if block will only be executed during creating the head
        # node. When creating the worker node, the frozen VMs must have been existing.
        # will never need to be deployed from OVF.
        if frozen_vm_config.get("library_item"):
            # If resource_pool config is present then create frozen VMs on each
            # host and put them in the specified resource pool.
            if frozen_vm_config.get("resource_pool"):
                self.create_frozen_vm_on_each_host(
                    node_config, frozen_vm_config.get("name", "frozen-vm"), True
                )
                self.initialize_frozen_vm_scheduler(frozen_vm_config)
                frozen_vm_obj = None

            # If resource_pool config is not present then create a frozen VM
            # with name as specified.
            else:
                frozen_vm_obj = self.create_frozen_vm_from_ovf(
                    node_config, frozen_vm_config["name"], True
                )

        # If library_item config is not present then select already existing
        # frozen VM.
        else:
            # If resource_pool is present, select a frozen VM out of all those
            # present in the resource pool specified.
            if frozen_vm_config.get("resource_pool"):
                self.initialize_frozen_vm_scheduler(frozen_vm_config)
                frozen_vm_obj = None
            # If resource_pool is not present then select the frozen VM with
            # name as specified.
            else:
                self.frozen_vm_name = frozen_vm_config.get("name", "frozen-vm")
                self.check_frozen_vm_status(self.frozen_vm_name)
                frozen_vm_obj = self.get_frozen_vm_obj()

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

        with ThreadPoolExecutor(max_workers=count) as executor:
            futures = [
                executor.submit(
                    self.create_instant_clone_node,
                    frozen_vm_obj,
                    vm_names[i],
                    node_config,
                    tags,
                )
                for i in range(count)
            ]
        for future in futures:
            try:
                vm = future.result()
                k = Constants.VSPHERE_NODE_STATUS
                v = Constants.VsphereNodeStatus.CREATED.value
                vsphere_node_created_tag = {k: v}
                # if create succeed, we add a "created" tag
                self.set_node_tags(vm.vm, vsphere_node_created_tag)
                created_nodes_dict[vm.name] = vm
            except Exception as e:
                logger.error(
                    "Exception occurred while creating or tagging VMs {}".format(e)
                )
                exception_happened = True

        # We clean up all the created VMs if any exception occurs.
        if exception_happened:
            with ThreadPoolExecutor(max_workers=count) as executor:
                futures = [
                    executor.submit(self.delete_vm, vm_names[i]) for i in range(count)
                ]
            for future in futures:
                _ = future.result()
            raise RuntimeError("Failed creating VMs, exiting!")

        return created_nodes_dict

    def get_tag(self, tag_name, category_id):
        for id in self.vsphere_sdk_client.tagging.Tag.list_tags_for_category(
            category_id
        ):
            if tag_name == self.vsphere_sdk_client.tagging.Tag.get(id).name:
                return id
        return None

    def create_node_tag(self, ray_node_tag, category_id):
        logger.debug(f"Creating {ray_node_tag} tag")
        tag_spec = self.vsphere_sdk_client.tagging.Tag.CreateSpec(
            ray_node_tag, "Ray node tag", category_id
        )
        tag_id = None
        try:
            tag_id = self.vsphere_sdk_client.tagging.Tag.create(tag_spec)
        except ErrorClients.Unauthorized as e:
            cli_logger.abort(f"Unathorised to create the tag. Exception: {e}")
        except Exception as e:
            cli_logger.abort(e)

        logger.debug(f"Tag {tag_id} created")
        return tag_id

    def get_category(self):
        for id in self.vsphere_sdk_client.tagging.Category.list():
            if (
                self.vsphere_sdk_client.tagging.Category.get(id).name
                == Constants.NODE_CATEGORY
            ):
                return id
        return None

    def create_category(self):
        # Create RAY_NODE category. This category is associated with VMs and supports
        # multiple tags e.g. "Ray-Head-Node, Ray-Worker-Node-1 etc."
        cli_logger.info(f"Creating {Constants.NODE_CATEGORY} category")
        category_spec = self.vsphere_sdk_client.tagging.Category.CreateSpec(
            name=Constants.NODE_CATEGORY,
            description="Identifies Ray head node and worker nodes",
            cardinality=CategoryModel.Cardinality.MULTIPLE,
            associable_types=set(),
        )
        category_id = None

        try:
            category_id = self.vsphere_sdk_client.tagging.Category.create(category_spec)
        except ErrorClients.Unauthorized as e:
            cli_logger.abort(f"Unathorised to create the category. Exception: {e}")
        except Exception as e:
            cli_logger.abort(e)

        cli_logger.info(f"Category {category_id} created")

        return category_id

    def terminate_node(self, node_id):
        if node_id is None:
            return

        status = self.vsphere_sdk_client.vcenter.vm.Power.get(node_id)

        if status.state != HardPower.State.POWERED_OFF:
            self.vsphere_sdk_client.vcenter.vm.Power.stop(node_id)
            logger.debug("vm.Power.stop({})".format(node_id))

        self.vsphere_sdk_client.vcenter.VM.delete(node_id)
        logger.info("Deleted vm {}".format(node_id))

        # Pop node_id from cached_nodes and tag_cache only if not present
        if node_id in self.cached_nodes:
            self.cached_nodes.pop(node_id)

        if node_id in self.tag_cache:
            self.tag_cache.pop(node_id)

    def terminate_nodes(self, node_ids):
        if not node_ids:
            return

        for node_id in node_ids:
            self.terminate_node(node_id)

    def _get_node(self, node_id):
        """Get the node object from vSphere."""
        vms = self.vsphere_sdk_client.vcenter.VM.list(VM.FilterSpec(vms={node_id}))
        if len(vms) == 0:
            logger.warning("VM with name ({}) not found".format(node_id))
            return None
        return vms[0]

    def _get_cached_node(self, node_id):
        """Return node info from cache if possible, otherwise fetch it from vSphere."""
        if node_id in self.cached_nodes:
            return self.cached_nodes[node_id]

        return self._get_node(node_id)
