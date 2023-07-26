import copy
import logging
import os
import threading
import time
import uuid
from collections import defaultdict
from threading import RLock, Thread
from typing import Any, Dict, List

import com.vmware.vapi.std.errors_client as ErrorClients
import yaml
from com.vmware.cis.tagging_client import CategoryModel
from com.vmware.content.library_client import Item
from com.vmware.vapi.std_client import DynamicID
from com.vmware.vcenter.guest_client import (
    CloudConfiguration,
    CloudinitConfiguration,
    ConfigurationSpec,
    CustomizationSpec,
    GlobalDNSSettings,
)
from com.vmware.vcenter.ovf_client import LibraryItem
from com.vmware.vcenter.vm.hardware_client import Cpu, Ethernet, Memory
from com.vmware.vcenter.vm_client import Power as HardPower
from com.vmware.vcenter_client import VM, Datastore, Network, ResourcePool
from pyVim.task import WaitForTask
from pyVmomi import vim

from ray.autoscaler._private.cli_logger import cli_logger
from ray.autoscaler._private.vsphere.config import (
    PUBLIC_KEY_PATH,
    USER_DATA_FILE_PATH,
    bootstrap_vsphere,
)
from ray.autoscaler._private.vsphere.utils import Constants, VmwSdkClient
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME, TAG_RAY_NODE_NAME

logger = logging.getLogger(__name__)


def to_vsphere_format(tags):
    """Convert the Ray node name tag to the vSphere-specific 'Name' tag."""

    if TAG_RAY_NODE_NAME in tags:
        tags["Name"] = tags[TAG_RAY_NODE_NAME]
        del tags[TAG_RAY_NODE_NAME]
    return tags


def from_vsphere_format(tags):
    """Convert the vSphere-specific 'Name' tag to the Ray node name tag."""

    if "Name" in tags:
        tags[TAG_RAY_NODE_NAME] = tags["Name"]
        del tags["Name"]
    return tags


class VsphereNodeProvider(NodeProvider):
    max_terminate_nodes = 1000

    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        self.cache_stopped_nodes = provider_config.get("cache_stopped_nodes", True)
        vsphere_credentials = provider_config["vsphere_config"]["credentials"]
        self.vsphere_credentials = vsphere_credentials

        self.vsphere_automation_sdk_client = VmwSdkClient(
            vsphere_credentials["server"],
            vsphere_credentials["user"],
            vsphere_credentials["password"],
            VmwSdkClient.SessionType.UNVERIFIED,
            VmwSdkClient.ClientType.AUTOMATION_SDK,
        ).get_client()

        self.vsphere_pyvmomi_sdk_client = VmwSdkClient(
            vsphere_credentials["server"],
            vsphere_credentials["user"],
            vsphere_credentials["password"],
            VmwSdkClient.SessionType.UNVERIFIED,
            VmwSdkClient.ClientType.PYVMOMI_SDK,
        ).get_client()

        # Tags that we believe to actually be on VM.
        self.tag_cache = {}
        # Tags that we will soon upload.
        self.tag_cache_pending = defaultdict(dict)
        self.tag_cache_lock = threading.Lock()
        self.lock = RLock()

        # Cache of node objects from the last nodes() call. This avoids
        # excessive DescribeInstances requests.
        self.cached_nodes: Dict[str, VM] = {}

    class ThreadWithReturnValue(Thread):
        """This class can be used to get return values from the the thread that was launched"""

        def __init__(
            self,
            group=None,
            target=None,
            name=None,
            args=(),
            kwargs=None,
            *,
            daemon=None,
        ):
            self.args = args
            self._return = None
            self.thread_exception = None

            Thread.__init__(self, group, target, name, args, kwargs, daemon=daemon)

        def run(self):
            if self._target is not None:
                try:
                    self._return = self._target(*self._args, **self._kwargs)
                except Exception as e:
                    self.thread_exception = e

        def join(self):
            Thread.join(self)

            if self.thread_exception:
                raise self.thread_exception

            return self._return

    class TagVMThread(ThreadWithReturnValue):
        """This class is used to tag VMs as soon as they show up on vCenter.
        If an user executes ray down when the OVFs are still getting deployed, we should still be
        able to identify the VMs in creating state. Hence, we tag the VMs as soon as they show up
        on the vCenter. With this approach we can use the tags to find out VMs both in deploying and
        deployed states.
        """

        def __init__(self, vm_name, tags, outer_obj):
            self.vm_name = vm_name
            self.tags = tags
            self.running = True
            self.thread_exception = None
            self.outer_obj = outer_obj
            self._return = None
            Thread.__init__(self)

        def run(self):

            try:
                names = set([self.vm_name])
                vms = []

                start = time.time()
                timeout = 120

                while time.time() - start < timeout and self.running:
                    vms = self.outer_obj.vsphere_automation_sdk_client.vcenter.VM.list(
                        VM.FilterSpec(names=names)
                    )

                    if len(vms) > 0:
                        vm_id = vms[0].vm
                        self.outer_obj.set_node_tags(vm_id, self.tags)
                        return

                raise RuntimeError("VM {} could not be found.".format(self.vm_name))

            except Exception as e:
                self.thread_exception = e

        def join(self):
            return super().join()

        def terminate(self):
            self.running = False

    class FreezeVMThread(ThreadWithReturnValue):
        def __init__(self, target_name, node_config, outer_obj):
            self.node_config = copy.deepcopy(node_config)
            freeze_vm = node_config["freeze_vm"]
            self.node_config["library_item"] = freeze_vm["library_item"]
            self.node_config["resource_pool"] = freeze_vm["resource_pool"]
            self.node_config["resources"] = freeze_vm["resources"]
            self.node_config["networks"] = freeze_vm["networks"]
            self.node_config["keep_nics_disconnected"] = True
            self.node_config["datastore"] = freeze_vm["datastore"]
            self.outer_obj = outer_obj
            self.target_name = target_name
            self.thread_exception = None
            super().__init__(args=(target_name, node_config, outer_obj))

        def run(self):
            try:
                vm_name = self.target_name
                vms = self.outer_obj.vsphere_automation_sdk_client.vcenter.VM.list(
                    VM.FilterSpec(names=set([vm_name]))
                )

                # Frozen VM already present so no need to create again
                if len(vms) > 0:
                    cli_logger.info("Frozen VM already exists. Not creating again.")

                    vm_id = vms[0].vm
                    status = self.vsphere_automation_sdk_client.vcenter.vm.Power.get(
                        vm_id
                    )

                    if status.state != HardPower.State.POWERED_ON:
                        cli_logger.info("Frozen VM is off. Powering it ON")
                        self.vsphere_automation_sdk_client.vcenter.vm.Power.start(vm_id)
                        cli_logger.info("vm.Power.start({})".format(vm_id))
                    return

                cli_logger.info("Creating frozen VM")
                self._return = self.outer_obj.create_ovf_node(self.node_config, vm_name)
            except Exception as e:
                cli_logger.info("Failed creating frozen VM {}".format(self.target_name))
                self.thread_exception = e

        def join(self):
            return super().join()

    @staticmethod
    def bootstrap_config(cluster_config):
        return bootstrap_vsphere(cluster_config)

    def non_terminated_nodes(self, tag_filters):
        """For vSphere, non_terminated nodes are:
        1. Nodes in creating state.
        2. Nodes already created and powered on.
        """
        with self.lock:
            nodes = []
            cli_logger.info("Getting non terminated nodes...")
            vms = self.vsphere_automation_sdk_client.vcenter.VM.list()
            cli_logger.info(f"Got {len(vms)} non terminated nodes.")
            filters = tag_filters.copy()
            if TAG_RAY_CLUSTER_NAME not in tag_filters:
                filters[TAG_RAY_CLUSTER_NAME] = self.cluster_name

            for vm in vms:
                vm_id = vm.vm
                yn_id = DynamicID(type=Constants.TYPE_OF_RESOURCE, id=vm.vm)

                matched_tags = {}
                all_tags = {}
                # If the VM has a tag from tag_filter then select it
                for (
                    tag_id
                ) in self.vsphere_automation_sdk_client.tagging.TagAssociation.list_attached_tags(
                    yn_id
                ):
                    vsphere_vm_tag = self.vsphere_automation_sdk_client.tagging.Tag.get(
                        tag_id=tag_id
                    ).name
                    if ":" in vsphere_vm_tag:
                        tag_key_value = vsphere_vm_tag.split(":")
                        tag_key = tag_key_value[0]
                        tag_value = tag_key_value[1]
                        if tag_key in filters and tag_value == filters[tag_key]:
                            matched_tags[tag_key] = tag_value

                        all_tags[tag_key] = tag_value

                # Update the tag cache with latest tags
                self.tag_cache[vm_id] = all_tags

                if len(matched_tags) == len(filters):

                    power_status = (
                        self.vsphere_automation_sdk_client.vcenter.vm.Power.get(vm_id)
                    )

                    # Return VMs in powered-on and creating state
                    if (
                        power_status.state == HardPower.State.POWERED_OFF
                        and all_tags[Constants.VSPHERE_NODE_STATUS]
                        == Constants.VsphereNodeStatus.CREATING.value
                        or power_status.state == HardPower.State.POWERED_ON
                    ):
                        nodes.append(vm_id)
                        # refresh cached_nodes with latest information e.g external ip
                        if vm_id in self.cached_nodes:
                            del self.cached_nodes[vm_id]
                            self.cached_nodes[vm_id] = vm

            cli_logger.info(f"Nodes are {nodes}")
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

        vm = self.vsphere_automation_sdk_client.vcenter.vm.guest.Identity.get(node_id)

        cli_logger.info("Fetch IP {} for VM {}".format(vm.ip_address, vm))

        return vm.ip_address

    def internal_ip(self, node_id):
        # TODO: Currently vSphere VMs do not show an internal IP. Check IP configuration
        # to get internal IP too. Temporary fix is to just work with external IPs
        return self.external_ip(node_id)

    def set_node_tags(self, node_id, tags):
        """This method gets called from the Ray and it passes
        node_id which needs to be vm.vm and not vm.name
        """
        cli_logger.info("Setting tags for vm {}".format(node_id))

        with self.lock:
            category_id = self.get_category()
            if not category_id:
                category_id = self.create_category()

            for key, value in tags.items():

                tag = key + ":" + value
                tag_id = self.get_tag(tag, category_id)
                if not tag_id:
                    tag_id = self.create_node_tag(tag, category_id)

                # If a tag with a key is present on the VM, then remove it
                # before updating the key with a new value.
                self.remove_tag_from_vm(key, node_id)

                cli_logger.info(f"Attaching tag {tag} to {node_id}")
                self.attach_tag(node_id, Constants.TYPE_OF_RESOURCE, tag_id=tag_id)

    def create_node(self, node_config, tags, count) -> Dict[str, Any]:
        """Creates instances.

        Returns dict mapping instance id to VM object for the created
        instances.
        """
        filters = tags.copy()
        if TAG_RAY_CLUSTER_NAME not in tags:
            filters[TAG_RAY_CLUSTER_NAME] = self.cluster_name

        counter = count

        cli_logger.info(f"Create Node tags : {filters}")
        reused_nodes_dict = {}
        reuse_nodes = list()
        reuse_node_ids = []
        number_of_reused_nodes = 0
        # Try to reuse previously stopped nodes with compatible configs
        if self.cache_stopped_nodes:
            vms = self.vsphere_automation_sdk_client.vcenter.VM.list(
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
                if number_of_reused_nodes >= counter:
                    break

                vm_id = vm.name
                yn_id = DynamicID(type=Constants.TYPE_OF_RESOURCE, id=vm.vm)
                filter_matched_count = 0
                for (
                    tag_id
                ) in self.vsphere_automation_sdk_client.tagging.TagAssociation.list_attached_tags(
                    yn_id
                ):
                    vsphere_vm_tag = self.vsphere_automation_sdk_client.tagging.Tag.get(
                        tag_id=tag_id
                    ).name

                    if ":" in vsphere_vm_tag:
                        tag_key_value = vsphere_vm_tag.split(":")
                        tag_key = tag_key_value[0]
                        tag_value = tag_key_value[1]
                        if tag_key in tags and tag_value == filters[tag_key]:
                            filter_matched_count += 1
                if filter_matched_count == len(filters):
                    reuse_nodes.append(vm)
                    reused_nodes_dict[vm_id] = vm
                    # Tag needs vm.vm and not vm.name as id
                    reuse_node_ids.append(vm.vm)
                    number_of_reused_nodes += 1

            if reuse_nodes:
                cli_logger.info(
                    "Reusing nodes {}. "
                    "To disable reuse, set `cache_stopped_nodes: False` "
                    "under `provider` in the cluster configuration.",
                    cli_logger.render_list(reuse_node_ids),
                )
                for node_id in reuse_node_ids:
                    cli_logger.info("Powering on VM")
                    self.vsphere_automation_sdk_client.vcenter.vm.Power.start(node_id)
                counter -= len(reuse_node_ids)

        created_nodes_dict = {}
        if counter:
            created_nodes_dict = self._create_node(node_config, filters, counter)

        all_created_nodes = reused_nodes_dict
        all_created_nodes.update(created_nodes_dict)

        # Set tags on the nodes that were reused
        for node_id in reuse_node_ids:
            self.set_node_tags(node_id, filters)

        return all_created_nodes

    @staticmethod
    def _merge_tag_specs(
        tag_specs: List[Dict[str, Any]], user_tag_specs: List[Dict[str, Any]]
    ) -> None:
        """
        Merges user-provided node config tag specifications into a base
        list of node provider tag specifications. The base list of
        node provider tag specs is modified in-place.

        This allows users to add tags and override values of existing
        tags with their own, and only applies to the resource type
        "instance". All other resource types are appended to the list of
        tag specs.

        Args:
            tag_specs (List[Dict[str, Any]]): base node provider tag specs
            user_tag_specs (List[Dict[str, Any]]): user's node config tag specs
        """

        for user_tag_spec in user_tag_specs:
            if user_tag_spec["ResourceType"] == "instance":
                for user_tag in user_tag_spec["Tags"]:
                    exists = False
                    for tag in tag_specs[0]["Tags"]:
                        if user_tag["Key"] == tag["Key"]:
                            exists = True
                            tag["Value"] = user_tag["Value"]
                            break
                    if not exists:
                        tag_specs[0]["Tags"] += [user_tag]
            else:
                tag_specs += [user_tag_spec]

    def get_vm(self, node_id):
        """
        Return the VM summary object
        Note: The method assumes that there is only one vm with the mentioned name.
        """
        vm = self._get_cached_node(node_id)
        cli_logger.info(f"VM {node_id} found")

        return vm

    def attach_tag(self, vm_id, resource_type, tag_id):
        dyn_id = DynamicID(type=resource_type, id=vm_id)
        try:
            self.vsphere_automation_sdk_client.tagging.TagAssociation.attach(
                tag_id, dyn_id
            )
            cli_logger.info("Tag attached")
        except Exception as e:
            cli_logger.info(
                "Check that the tag is associable to {}".format(resource_type)
            )
            raise e

    def set_cloudinit_userdata(self, vm_id):
        logger.info("Setting cloudinit userdata for vm {}".format(vm_id))

        metadata = '{"cloud_name": "vSphere"}'

        # Read the public key that was generated previously.
        with open(PUBLIC_KEY_PATH, "r") as file:
            public_key = file.read().rstrip("\n")

        # This file contains the userdata with default values.
        # We want to add the key that we generate with create_key_pair
        # function into authorized_keys section

        f = open(USER_DATA_FILE_PATH, "r")
        data = yaml.load(f, Loader=yaml.FullLoader)
        for _, v in data.items():
            for user in v:
                if isinstance(user, dict):
                    user["ssh_authorized_keys"] = [public_key]
        f.close()

        modified_userdata = yaml.dump(data, default_flow_style=False)

        # The userdata needs to be prefixed with #cloud-config.
        # Without it, the cloudinit spec would get applied but
        # it wouldn't add the userdata on the VM.

        modified_userdata = "#cloud-config\n" + modified_userdata
        logger.info("Successfully modified the userdata file for vm {}".format(vm_id))

        # Create cloud-init spec and apply
        cloudinit_config = CloudinitConfiguration(
            metadata=metadata, userdata=modified_userdata
        )
        cloud_config = CloudConfiguration(
            cloudinit=cloudinit_config, type=CloudConfiguration.Type("CLOUDINIT")
        )
        config_spec = ConfigurationSpec(cloud_config=cloud_config)
        global_dns_settings = GlobalDNSSettings()
        adapter_mapping_list = []
        customization_spec = CustomizationSpec(
            configuration_spec=config_spec,
            global_dns_settings=global_dns_settings,
            interfaces=adapter_mapping_list,
        )

        # create customization specification by CustomizationSpecs service
        specs_svc = self.vsphere_automation_sdk_client.vcenter.guest.CustomizationSpecs
        spec_name = str(uuid.uuid4())
        spec_desc = (
            "This is a customization specification which includes"
            "raw cloud-init configuration data"
        )
        create_spec = specs_svc.CreateSpec(
            name=spec_name, description=spec_desc, spec=customization_spec
        )
        specs_svc.create(spec=create_spec)

        vmcust_svc = self.vsphere_automation_sdk_client.vcenter.vm.guest.Customization
        set_spec = vmcust_svc.SetSpec(name=spec_name, spec=None)
        vmcust_svc.set(vm=vm_id, spec=set_spec)

        logger.info("Successfully added cloudinit config for vm {}".format(vm_id))

    def create_nic(self, network_config, vm_id, start_connected=False):
        # Find the network
        network_filter_spec = Network.FilterSpec(names=set([network_config["name"]]))
        network_id = self.vsphere_automation_sdk_client.vcenter.Network.list(
            network_filter_spec
        )[0].network

        # Create a NIC to connect to the network found above.
        backing = Ethernet.BackingSpec(
            type=Ethernet.BackingType.__dict__[network_config["backing_type"]],
            network=network_id,
        )
        eth_create_spec = Ethernet.CreateSpec(
            type=Ethernet.EmulationType.__dict__[network_config["adapter_type"]],
            backing=backing,
            start_connected=start_connected,
            allow_guest_control=True,
        )

        cli_logger.info("Creating NIC for VM {}".format(vm_id))

        self.vsphere_automation_sdk_client.vcenter.vm.hardware.Ethernet.create(
            vm_id, eth_create_spec
        )

    def create_ovf_node(self, node_config, vm_name_target):
        # Find and use the resource pool defined in the manifest file.
        rp_filter_spec = ResourcePool.FilterSpec(
            names=set([node_config["resource_pool"]])
        )
        resource_pool_summaries = (
            self.vsphere_automation_sdk_client.vcenter.ResourcePool.list(rp_filter_spec)
        )
        if not resource_pool_summaries:
            raise ValueError(
                "Resource pool with name '{}' not found".format(rp_filter_spec)
            )

        resource_pool_id = resource_pool_summaries[0].resource_pool

        cli_logger.print("Resource pool ID: {}".format(resource_pool_id))

        # Find and use the OVF library item defined in the manifest file.
        find_spec = Item.FindSpec(name=node_config["library_item"])
        item_ids = self.vsphere_automation_sdk_client.content.library.Item.find(
            find_spec
        )

        lib_item_id = item_ids[0]

        deployment_target = LibraryItem.DeploymentTarget(
            resource_pool_id=resource_pool_id
        )
        ovf_summary = self.vsphere_automation_sdk_client.vcenter.ovf.LibraryItem.filter(
            ovf_library_item_id=lib_item_id, target=deployment_target
        )
        cli_logger.print(
            "Found an OVF template: {} to deploy.".format(ovf_summary.name)
        )

        datastore_id = None
        if "datastore" in node_config and node_config["datastore"]:
            datastore_filter_spec = Datastore.FilterSpec(
                names=set([node_config["datastore"]])
            )
            datastore_id = self.vsphere_automation_sdk_client.vcenter.Datastore.list(
                datastore_filter_spec
            )[0].datastore

        # Build the deployment spec
        deployment_spec = LibraryItem.ResourcePoolDeploymentSpec(
            name=vm_name_target,
            annotation=ovf_summary.annotation,
            accept_all_eula=True,
            network_mappings=None,
            storage_mappings=None,
            storage_provisioning=None,
            storage_profile_id=None,
            locale=None,
            flags=None,
            additional_parameters=None,
            default_datastore_id=datastore_id,
        )

        # Deploy the ovf template
        result = self.vsphere_automation_sdk_client.vcenter.ovf.LibraryItem.deploy(
            lib_item_id,
            deployment_target,
            deployment_spec,
            client_token=str(uuid.uuid4()),
        )

        # The type and ID of the target deployment is available in the deployment result.
        if result.succeeded:
            cli_logger.print(
                'Deployment successful. VM Name: "{}", ID: "{}"'.format(
                    vm_name_target, result.resource_id.id
                )
            )
            self.vm_id = result.resource_id.id
            error = result.error
            if error is not None:
                for warning in error.warnings:
                    cli_logger.print("OVF warning: {}".format(warning.message))

        else:
            cli_logger.print("Deployment failed.")
            for error in result.error.errors:
                cli_logger.print("OVF error: {}".format(result))

            raise ValueError(
                "OVF deployment failed for VM {}, reason: {}".format(vm_name_target),
                result,
            )

        vm_id = result.resource_id.id

        if "CPU" in node_config["resources"]:
            # Update number of CPUs
            update_spec = Cpu.UpdateSpec(count=node_config["resources"]["CPU"])

            cli_logger.info("vm.hardware.Cpu.update({}, {})".format(vm_id, update_spec))
            self.vsphere_automation_sdk_client.vcenter.vm.hardware.Cpu.update(
                vm_id, update_spec
            )

        if "Memory" in node_config["resources"]:
            # Update Memory
            update_spec = Memory.UpdateSpec(size_mib=node_config["resources"]["Memory"])

            cli_logger.info(
                "vm.hardware.Memory.update({}, {})".format(vm_id, update_spec)
            )
            self.vsphere_automation_sdk_client.vcenter.vm.hardware.Memory.update(
                vm_id, update_spec
            )

        # Inject a new user with public key into the VM
        self.set_cloudinit_userdata(vm_id)

        status = self.vsphere_automation_sdk_client.vcenter.vm.Power.get(vm_id)
        if status.state != HardPower.State.POWERED_ON:
            cli_logger.info("Powering on VM")
            self.vsphere_automation_sdk_client.vcenter.vm.Power.start(vm_id)
            cli_logger.info("vm.Power.start({})".format(vm_id))

        if "networks" in node_config and node_config["networks"]:
            for network in node_config["networks"]:
                # For frozen VM, keep the NICs in disconnected state on power on
                if (
                    "keep_nics_disconnected" in node_config
                    and node_config["keep_nics_disconnected"] == True
                ):
                    self.create_nic(network, vm_id, False)
                else:
                    self.create_nic(network, vm_id, True)

        # Get the created vm object
        vm = self.get_vm(result.resource_id.id)

        return vm

    def remove_tag_from_vm(self, tag_key_to_remove, vm_id):
        """Example: If a tag called node-status:initializing is present on the VM.
        If we would like to add a new value called finished with the node-status key.
        We'll need to delete the older tag node-status:initializing first before creating
        node-status:finished
        """
        yn_id = DynamicID(type=Constants.TYPE_OF_RESOURCE, id=vm_id)

        # List all the tags present on the VM.
        for (
            tag_id
        ) in self.vsphere_automation_sdk_client.tagging.TagAssociation.list_attached_tags(
            yn_id
        ):
            vsphere_vm_tag = self.vsphere_automation_sdk_client.tagging.Tag.get(
                tag_id=tag_id
            ).name
            if ":" in vsphere_vm_tag:
                tag_key_value = vsphere_vm_tag.split(":")
                tag_key = tag_key_value[0]
                if tag_key == tag_key_to_remove:
                    # Remove the tag matching the key passed.
                    cli_logger.info(
                        "Removing tag {} from the VM {}".format(tag_key, vm_id)
                    )
                    self.vsphere_automation_sdk_client.tagging.TagAssociation.detach(
                        tag_id, yn_id
                    )
                    break

    def wait_until_power_on(self, vm_id, timeout, interval):
        start = time.time()
        status = self.vsphere_automation_sdk_client.vcenter.vm.Power.get(vm_id)
        while (
            status != HardPower.Info(state=HardPower.State.POWERED_ON)
            and time.time() - start < timeout
        ):
            cli_logger.info("waiting for power on of frozen VM")
            time.sleep(interval)

    def find_vm_to_clone_from(self, tags):

        # Frozen VM should have unique name
        # We can fetch the frozen VM name from the tag present on the head node or we can
        # build it as below. We prefer the below approach over fetching the tag from head
        # node as this is faster.
        frozen_vm_target_name = tags[TAG_RAY_NODE_NAME].split("-")
        frozen_vm_target_name = Constants.FROZEN_VM_FORMAT.format(
            frozen_vm_target_name[0], frozen_vm_target_name[1]
        )

        vms = self.vsphere_automation_sdk_client.vcenter.VM.list(
            VM.FilterSpec(names=set([frozen_vm_target_name]))
        )

        cli_logger.info("VM to clone from {}".format(vms))

        return vms[0] if len(vms) > 0 else None

    def get_pyvmomi_obj(self, vimtype, name):
        obj = None

        # TODO: Find a better way to solve pyvmomi timeout issues
        self.vsphere_pyvmomi_sdk_client = VmwSdkClient(
            self.vsphere_credentials["server"],
            self.vsphere_credentials["user"],
            self.vsphere_credentials["password"],
            VmwSdkClient.SessionType.UNVERIFIED,
            VmwSdkClient.ClientType.PYVMOMI_SDK,
        ).get_client()

        container = self.vsphere_pyvmomi_sdk_client.viewManager.CreateContainerView(
            self.vsphere_pyvmomi_sdk_client.rootFolder, vimtype, True
        )

        for c in container.view:
            if name:
                if c.name == name:
                    obj = c
                    break
            else:
                obj = c
                break

        return obj

    def disconnect_nics(self, vm_id):
        nics = self.vsphere_automation_sdk_client.vcenter.vm.hardware.Ethernet.list(
            vm_id
        )

        for nic in nics:
            eth_update_spec = Ethernet.UpdateSpec(start_connected=False)
            self.vsphere_automation_sdk_client.vcenter.vm.hardware.Ethernet.update(
                vm_id, nic.nic, eth_update_spec
            )
            cli_logger.info("Disconnecting NIC {} from VM {}".format(nic.name, vm_id))
            self.vsphere_automation_sdk_client.vcenter.vm.hardware.Ethernet.disconnect(
                vm_id, nic
            )

    def connect_nics(self, vm_id):
        nics = self.vsphere_automation_sdk_client.vcenter.vm.hardware.Ethernet.list(
            vm_id
        )

        for nic in nics:
            eth_update_spec = Ethernet.UpdateSpec(start_connected=True)
            self.vsphere_automation_sdk_client.vcenter.vm.hardware.Ethernet.update(
                vm_id, nic.nic, eth_update_spec
            )
            cli_logger.info("Connecting NIC {} to VM {}".format(nic, vm_id))
            self.vsphere_automation_sdk_client.vcenter.vm.hardware.Ethernet.connect(
                vm_id, nic.nic
            )

    def create_instant_clone_node(self, source_vm, vm_name_target):
        vm_relocate_spec = vim.vm.RelocateSpec()
        instant_clone_spec = vim.vm.InstantCloneSpec()
        instant_clone_spec.name = vm_name_target
        instant_clone_spec.location = vm_relocate_spec
        instant_clone_spec.config = None

        parent_vm = self.get_pyvmomi_obj([vim.VirtualMachine], source_vm.name)

        WaitForTask(parent_vm.InstantClone_Task(spec=instant_clone_spec))

        clone = self.get_pyvmomi_obj([vim.VirtualMachine], vm_name_target)

        # Get VM ID
        vm_id = str(clone).split(":")[1][:-1]

        vm = self.get_vm(vm_id)

        # The OVF file for frozen VM has the NIC in disconnected state because
        # If we keep NIC connected upon power on for frozen VM then the VMs cloned from this VM
        # for a transient moment will show wrong IP before refresing their network.

        # Hence, we keep NIC disconnected in OVF of frozen VM so connect the NIC after powering on
        # the cloned VMs
        self.connect_nics(vm_id)

        return vm

    def delete_vm(self, vm_name):
        vms = self.vsphere_automation_sdk_client.vcenter.VM.list(
            VM.FilterSpec(names=set([vm_name]))
        )

        if len(vms) > 0:
            vm_id = vms[0].vm

            status = self.vsphere_automation_sdk_client.vcenter.vm.Power.get(vm_id)

            if status.state != HardPower.State.POWERED_OFF:
                self.vsphere_automation_sdk_client.vcenter.vm.Power.stop(vm_id)

            cli_logger.info("Deleting VM {}".format(vm_id))
            self.vsphere_automation_sdk_client.vcenter.VM.delete(vm_id)

    def _create_node(self, node_config, tags, count):
        created_nodes_dict = {}

        # create a frozen VM if not already present
        frozen_vm_thread = None
        frozen_vm_target_name = None

        # Threads for creating and tagging VMs
        create_node_threads = []
        tag_node_threads = []

        # freeze_vm config is only present on the head node config. Hence, any code related
        # to freeze VM executes during the creation of the head node on the bootstrap
        # machine
        if "freeze_vm" in node_config:
            # Frozen VM should have unique name
            frozen_vm_target_name = tags[TAG_RAY_NODE_NAME].split("-")
            frozen_vm_target_name = Constants.FROZEN_VM_FORMAT.format(
                frozen_vm_target_name[0], frozen_vm_target_name[1]
            )

            frozen_vm_thread = self.FreezeVMThread(
                frozen_vm_target_name, node_config, self
            )
            frozen_vm_thread.start()

        for _ in range(count):

            # The nodes are named as follows:
            # ray-<cluster-name>-head-<uuid> for the head node
            # ray-<cluster-name>-worker-<uuid> for the worker nodes
            vm_name = tags[TAG_RAY_NODE_NAME] + "-" + str(uuid.uuid4())

            vm = None

            try:
                vm = self.find_vm_to_clone_from(tags)

            except ValueError:
                cli_logger.info(
                    "clone_from config not present so creating VM from OVF."
                )

            # The frozen VM name is stored as a tag on the head node
            if frozen_vm_target_name is not None:
                tags[Constants.RAY_HEAD_FROZEN_VM_TAG] = frozen_vm_target_name

            # Set vsphere-node-status tag to creating for the node.
            tags[
                Constants.VSPHERE_NODE_STATUS
            ] = Constants.VsphereNodeStatus.CREATING.value

            tag_node_thread = self.TagVMThread(vm_name, tags, self)
            tag_node_threads.append(tag_node_thread)

            # Clone from existing worker if we got a valid VM from clone_from config.
            if "clone" in node_config and node_config["clone"] == True and vm != None:
                create_node_threads.append(
                    self.ThreadWithReturnValue(
                        target=self.create_instant_clone_node, args=(vm, vm_name)
                    )
                )
            else:
                create_node_threads.append(
                    self.ThreadWithReturnValue(
                        target=self.create_ovf_node, args=(node_config, vm_name)
                    )
                )

        for i in range(0, len(create_node_threads)):
            # Setting daemon=True allows the main thread to continue execution and terminate
            # if any of the child threads fail
            create_node_threads[i].start()

            tag_node_threads[i].start()

        exception_occurred = False
        for i in range(0, len(create_node_threads)):
            try:
                vm = create_node_threads[i].join()
                tag_node_threads[i].join()
                # Now that the VM got created, set the vsphere-node-status tag to created
                vsphere_node_created_tag = {
                    Constants.VSPHERE_NODE_STATUS: Constants.VsphereNodeStatus.CREATED.value
                }
                self.set_node_tags(vm.vm, vsphere_node_created_tag)

                created_nodes_dict[vm_name] = vm

            except Exception as e:
                # If any failure happens, terminate tag node threads
                # and set exception_occurred = Ture, which will be used later
                # to clean resources
                cli_logger.error(
                    "Exception occurred while creating or tagging VMs {}".format(e)
                )
                exception_occurred = True
                tag_node_threads[i].terminate()

        try:
            if frozen_vm_thread is not None:
                frozen_vm_thread.join()
        except Exception as e:
            cli_logger.error("Exception occurred while creating Frozen VM {}".format(e))
            exception_occurred = True

        if exception_occurred:
            for i in range(0, len(create_node_threads)):
                cli_logger.error("Exception occurred. Deleting VMs!")
                self.delete_vm(create_node_threads[i].args[1])

            if frozen_vm_target_name:
                cli_logger.error("Exception occurred. Deleting frozen VM!")
                self.delete_vm(frozen_vm_target_name)

            raise RuntimeError("Failed creating VMs, exiting!")

        return created_nodes_dict

    def get_tag(self, tag_name, category_id):
        for id in self.vsphere_automation_sdk_client.tagging.Tag.list_tags_for_category(
            category_id
        ):
            if tag_name == self.vsphere_automation_sdk_client.tagging.Tag.get(id).name:
                return id
        return None

    def create_node_tag(self, ray_node_tag, category_id):
        cli_logger.info(f"Creating {ray_node_tag} tag")
        tag_spec = self.vsphere_automation_sdk_client.tagging.Tag.CreateSpec(
            ray_node_tag, "Ray node tag", category_id
        )
        try:
            tag_id = self.vsphere_automation_sdk_client.tagging.Tag.create(tag_spec)
        except ErrorClients.Unauthorized as e:
            cli_logger.abort(f"Unathorised to create the tag. Exception: {e}")
        except Exception as e:
            cli_logger.abort(e)

        cli_logger.info(f"Tag {tag_id} created")
        return tag_id

    def get_category(self):
        for id in self.vsphere_automation_sdk_client.tagging.Category.list():
            if (
                self.vsphere_automation_sdk_client.tagging.Category.get(id).name
                == Constants.NODE_CATEGORY
            ):
                return id
        return None

    def create_category(self):
        # Create RAY_NODE category. This category is associated with VMs and supports multiple tags e.g. "Ray-Head-Node, Ray-Worker-Node-1 etc."
        cli_logger.info(f"Creating {Constants.NODE_CATEGORY} category")
        category_spec = self.vsphere_automation_sdk_client.tagging.Category.CreateSpec(
            name=Constants.NODE_CATEGORY,
            description="Identifies Ray head node and worker nodes",
            cardinality=CategoryModel.Cardinality.MULTIPLE,
            associable_types=set(),
        )
        category_id = None

        try:
            category_id = self.vsphere_automation_sdk_client.tagging.Category.create(
                category_spec
            )
        except ErrorClients.Unauthorized as e:
            cli_logger.abort(f"Unathorised to create the category. Exception: {e}")
        except Exception as e:
            cli_logger.abort(e)

        cli_logger.info(f"Category {category_id} created")

        return category_id

    def terminate_node(self, node_id):
        if node_id is None:
            return

        status = self.vsphere_automation_sdk_client.vcenter.vm.Power.get(node_id)

        if status.state != HardPower.State.POWERED_OFF:
            self.vsphere_automation_sdk_client.vcenter.vm.Power.stop(node_id)
            cli_logger.info("vm.Power.stop({})".format(node_id))

        self.vsphere_automation_sdk_client.vcenter.VM.delete(node_id)
        cli_logger.info("Deleted vm {}".format(node_id))

        # Pop node_id from cached_nodes and tag_cache only if not present
        if node_id in self.cached_nodes:
            self.cached_nodes.pop(node_id)

        if node_id in self.tag_cache:
            self.tag_cache.pop(node_id)

    def terminate_nodes(self, node_ids):
        if not node_ids:
            return

        frozen_vm_name = None
        for node_id in node_ids:

            # Get frozen VM name from the head node tag
            vm_obj = self.get_vm(node_id)

            if "head" in vm_obj.name:
                head_node_tags = self.node_tags(node_id)

                if Constants.RAY_HEAD_FROZEN_VM_TAG in head_node_tags:
                    frozen_vm_name = head_node_tags[Constants.RAY_HEAD_FROZEN_VM_TAG]

            self.terminate_node(node_id)

        # If all the nodes got deleted it means the user has executed ray down command so
        # delete the frozen VM as well.

        # Frozen VM will not be present if the workers were created from OVF files.
        if len(self.non_terminated_nodes({})) == 0 and frozen_vm_name:
            # Get the Frozen VM by name
            vms = self.vsphere_automation_sdk_client.vcenter.VM.list(
                VM.FilterSpec(names=set([frozen_vm_name]))
            )
            self.terminate_node(vms[0].vm)

    def _get_node(self, node_id):
        """Refresh and get info for this node, updating the cache."""
        self.non_terminated_nodes({})  # Side effect: updates cache

        vms = self.vsphere_automation_sdk_client.vcenter.VM.list(
            VM.FilterSpec(vms=set([node_id]))
        )
        if len(vms) == 0:
            cli_logger.error("VM with name ({}) not found".format(node_id))
            return None
        return vms[0]

    def _get_cached_node(self, node_id):
        """Return node info from cache if possible, otherwise fetches it."""
        if node_id in self.cached_nodes:
            return self.cached_nodes[node_id]

        return self._get_node(node_id)
