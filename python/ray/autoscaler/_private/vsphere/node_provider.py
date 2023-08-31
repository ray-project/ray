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
import yaml
from com.vmware.cis.tagging_client import CategoryModel
from com.vmware.vapi.std_client import DynamicID
from com.vmware.vcenter.guest_client import (
    CloudConfiguration,
    CloudinitConfiguration,
    ConfigurationSpec,
    CustomizationSpec,
    GlobalDNSSettings,
)
from com.vmware.vcenter.vm.hardware_client import Cpu, Ethernet, Memory
from com.vmware.vcenter.vm_client import Power as HardPower
from com.vmware.vcenter_client import VM
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

        self.vsphere_sdk_client = VmwSdkClient(
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

    def check_frozen_vm_existence(self):
        vms = self.vsphere_sdk_client.vcenter.VM.list(
            VM.FilterSpec(names={self.frozen_vm_name})
        )

        if len(vms) == 1:
            cli_logger.info(
                "Found the frozen VM with name: {}".format(self.frozen_vm_name)
            )

            vm_id = vms[0].vm
            status = self.vsphere_sdk_client.vcenter.vm.Power.get(vm_id)
            if status.state != HardPower.State.POWERED_ON:
                cli_logger.info("Inject user data into frozen vm by cloud init")
                self.set_cloudinit_userdata(vm_id)
                cli_logger.info("Frozen VM is off. Powering it ON")
                self.vsphere_sdk_client.vcenter.vm.Power.start(vm_id)
                cli_logger.info("vm.Power.start({})".format(vm_id))
        elif len(vms) > 1:
            # This should never happen but we need to code defensively
            raise ValueError(
                "Unexpected: there are more than one VMs with name {}".format(
                    self.frozen_vm_name
                )
            )
        else:
            raise ValueError(
                "The frozen VM {} doesn't exist on vSphere, please contact the VI "
                "admin".format(self.frozen_vm_name)
            )

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
            cli_logger.info("Getting non terminated nodes...")
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

        vm = self.vsphere_sdk_client.vcenter.vm.guest.Identity.get(node_id)
        try:
            _ = ipaddress.IPv4Address(vm.ip_address)
            cli_logger.info("Fetch IP {} for VM {}".format(vm.ip_address, vm))
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

        cli_logger.info("Setting tags for vm {}".format(node_id))
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

        to_be_launched_node_count = count

        cli_logger.info(f"Create Node tags : {filters}")
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
                cli_logger.info(
                    "Reusing nodes {}. "
                    "To disable reuse, set `cache_stopped_nodes: False` "
                    "under `provider` in the cluster configuration.",
                    cli_logger.render_list(reuse_node_ids),
                )
                for node_id in reuse_node_ids:
                    cli_logger.info("Powering on VM with id {}".format(node_id))
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
        cli_logger.info(f"VM {node_id} found")

        return vm

    def attach_tag(self, vm_id, resource_type, tag_id):
        dynamic_id = DynamicID(type=resource_type, id=vm_id)
        try:
            self.vsphere_sdk_client.tagging.TagAssociation.attach(tag_id, dynamic_id)
            cli_logger.info(f"Tag {tag_id} attached on VM {dynamic_id}")
        except Exception as e:
            cli_logger.error(f"Check that the tag is attachable to {resource_type}")
            raise e

    def set_cloudinit_userdata(self, vm_id):
        """
        This function will only be called when the frozen VM was at "off" state at
        start. Then we are able to inject cloudinit user data into the VM. For example,
        The SSH keys for the Ray nodes to communicate with each other.
        """
        logger.info("Setting cloudinit userdata for vm {}".format(vm_id))

        metadata = '{"cloud_name": "vSphere"}'

        # Read the public key that was generated previously.
        with open(PUBLIC_KEY_PATH, "r") as file:
            public_key = file.read().rstrip("\n")

        # This file contains the userdata with default values.
        # We want to add the key that we generate with create_key_pair
        # function into authorized_keys section

        with open(USER_DATA_FILE_PATH, "r") as user_data_file:
            data = yaml.load(user_data_file, Loader=yaml.FullLoader)
            for _, v in data.items():
                for user in v:
                    if isinstance(user, dict):
                        user["ssh_authorized_keys"] = [public_key]
            modified_userdata = yaml.dump(data, default_flow_style=False)

        # The userdata needs to be prefixed with #cloud-config.
        # Without it, the cloudinit spec would get applied but
        # it wouldn't add the userdata on the VM.
        # For information about the cloud init doc of vSphere VM, check below pages:
        # https://kb.vmware.com/s/article/82250
        # https://cloudinit.readthedocs.io/en/latest/explanation/format.html

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
        specs_svc = self.vsphere_sdk_client.vcenter.guest.CustomizationSpecs
        spec_name = str(uuid.uuid4())
        spec_desc = (
            "This is a customization specification which includes"
            "raw cloud-init configuration data"
        )
        create_spec = specs_svc.CreateSpec(
            name=spec_name, description=spec_desc, spec=customization_spec
        )
        specs_svc.create(spec=create_spec)

        vmcust_svc = self.vsphere_sdk_client.vcenter.vm.guest.Customization
        set_spec = vmcust_svc.SetSpec(name=spec_name, spec=None)
        vmcust_svc.set(vm=vm_id, spec=set_spec)

        logger.info("Successfully added cloudinit config for vm {}".format(vm_id))

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
                cli_logger.info("Removing tag {} from the VM {}".format(tag_key, vm_id))
                self.vsphere_sdk_client.tagging.TagAssociation.detach(
                    tag_id, dynamic_id
                )
                break

    def get_frozen_vm_obj(self):
        frozen_vm_target_name = self.frozen_vm_name
        vms = self.vsphere_sdk_client.vcenter.VM.list(
            VM.FilterSpec(names={frozen_vm_target_name})
        )

        cli_logger.info("VM to clone from {}".format(vms))

        return vms[0] if len(vms) > 0 else None

    def get_pyvmomi_obj(self, vimtype, name):
        """
        This function finds the vSphere object by the object name and the object type.
        The object type can be "VM", "Host", "Datastore", etc.
        The object name is a unique name under the vCenter server.
        To check all such object information, you can go to the managed object board
        page of your vCenter Server, such as: https://<your_vc_ip/mob
        """
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
        if not obj:
            raise RuntimeError(
                f"Unexpected: cannot find vSphere object {vimtype} with name: {name}"
            )
        return obj

    def connect_nics(self, vm_id):
        nics = self.vsphere_sdk_client.vcenter.vm.hardware.Ethernet.list(vm_id)

        for nic in nics:
            eth_update_spec = Ethernet.UpdateSpec(start_connected=True)
            self.vsphere_sdk_client.vcenter.vm.hardware.Ethernet.update(
                vm_id, nic.nic, eth_update_spec
            )
            cli_logger.info("Connecting NIC {} to VM {}".format(nic, vm_id))
            self.vsphere_sdk_client.vcenter.vm.hardware.Ethernet.connect(vm_id, nic.nic)

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
            self.get_pyvmomi_obj([vim.ResourcePool], node_config["resource_pool"])
            if "resource_pool" in node_config
            else None
        )
        # If datastore is not provided in the config yaml, then the datastore
        # of the frozen VM will also be the resource pool of the new VM.
        datastore = (
            self.get_pyvmomi_obj([vim.Datastore], node_config["datastore"])
            if "datastore" in node_config
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

        parent_vm = self.get_pyvmomi_obj([vim.VirtualMachine], source_vm.name)

        tags[Constants.VSPHERE_NODE_STATUS] = Constants.VsphereNodeStatus.CREATING.value
        threading.Thread(target=self.tag_vm, args=(vm_name_target, tags)).start()
        # We need to wait the task, to make sure connect nic can succeed
        WaitForTask(parent_vm.InstantClone_Task(spec=instant_clone_spec))

        cloned_vm = self.get_pyvmomi_obj([vim.VirtualMachine], vm_name_target)

        # Get VM ID
        vm_id = cloned_vm._moId

        vm = self.get_vm(vm_id)

        # The frozen VM should have the NIC in disconnected state because If we keep
        # NIC connected upon power on for frozen VM then the VMs cloned from this VM
        # for a transient moment will show wrong IP before refreshing their network.
        # Hence, we keep NIC disconnected for frozen VM and connect the NIC after
        # powering on the cloned VMs
        self.connect_nics(vm_id)

        if "CPU" in resources:
            # Update number of CPUs
            update_spec = Cpu.UpdateSpec(count=resources["CPU"])
            cli_logger.info("vm.hardware.Cpu.update({}, {})".format(vm_id, update_spec))
            self.vsphere_sdk_client.vcenter.vm.hardware.Cpu.update(vm_id, update_spec)

        if "Memory" in resources:
            # Update Memory
            update_spec = Memory.UpdateSpec(size_mib=resources["Memory"])

            cli_logger.info(
                "vm.hardware.Memory.update({}, {})".format(vm_id, update_spec)
            )
            self.vsphere_sdk_client.vcenter.vm.hardware.Memory.update(
                vm_id, update_spec
            )

        return vm

    def delete_vm(self, vm_name):
        vms = self.vsphere_sdk_client.vcenter.VM.list(VM.FilterSpec(names={vm_name}))

        if len(vms) > 0:
            vm_id = vms[0].vm

            status = self.vsphere_sdk_client.vcenter.vm.Power.get(vm_id)

            if status.state != HardPower.State.POWERED_OFF:
                self.vsphere_sdk_client.vcenter.vm.Power.stop(vm_id)

            cli_logger.info("Deleting VM {}".format(vm_id))
            self.vsphere_sdk_client.vcenter.VM.delete(vm_id)

    def _create_node(self, node_config, tags, count):
        created_nodes_dict = {}
        exception_happened = False

        if "frozen_vm_name" in node_config:
            # This function either returns nothing or raise exception
            self.frozen_vm_name = node_config["frozen_vm_name"]
            self.check_frozen_vm_existence()
        frozen_vm_obj = self.get_frozen_vm_obj()

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
                cli_logger.error(
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
        cli_logger.info(f"Creating {ray_node_tag} tag")
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

        cli_logger.info(f"Tag {tag_id} created")
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
            cli_logger.info("vm.Power.stop({})".format(node_id))

        self.vsphere_sdk_client.vcenter.VM.delete(node_id)
        cli_logger.info("Deleted vm {}".format(node_id))

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
            cli_logger.error("VM with name ({}) not found".format(node_id))
            return None
        return vms[0]

    def _get_cached_node(self, node_id):
        """Return node info from cache if possible, otherwise fetch it from vSphere."""
        if node_id in self.cached_nodes:
            return self.cached_nodes[node_id]

        return self._get_node(node_id)
