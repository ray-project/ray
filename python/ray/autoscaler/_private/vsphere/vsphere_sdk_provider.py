import logging
import time
import uuid
from threading import RLock

import com.vmware.vapi.std.errors_client as ErrorClients
import requests
from com.vmware.cis.tagging_client import CategoryModel
from com.vmware.content.library_client import Item
from com.vmware.vapi.std.errors_client import Unauthenticated
from com.vmware.vapi.std_client import DynamicID
from com.vmware.vcenter.ovf_client import DiskProvisioningType, LibraryItem
from com.vmware.vcenter.vm.hardware_client import Cpu, Memory
from com.vmware.vcenter.vm_client import Power as HardPower
from com.vmware.vcenter_client import VM, Host, ResourcePool
from vmware.vapi.vsphere.client import create_vsphere_client

from ray.autoscaler._private.vsphere.utils import Constants, singleton_client
from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME

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


def get_unverified_session():
    """
    vCenter provisioned internally have SSH certificates
    expired so we use unverified session. Find out what
    could be done for production.

    Get a requests session with cert verification disabled.
    Also disable the insecure warnings message.
    Note this is not recommended in production code.
    @return: a requests session with verification disabled.
    """
    session = requests.session()
    session.verify = False
    requests.packages.urllib3.disable_warnings()
    return session


@singleton_client
class VsphereSdkProvider:
    def __init__(self, server, user, password, session_type: Constants.SessionType):
        self.server = server
        self.user = user
        self.password = password
        self.session_type = session_type
        self.vsphere_sdk_client = self.get_client()
        self.lock = RLock()

    def get_client(self):
        session = None
        if self.session_type == Constants.SessionType.UNVERIFIED:
            session = get_unverified_session()
        else:
            # TODO: support verified context
            pass
        return create_vsphere_client(
            server=self.server,
            username=self.user,
            password=self.password,
            session=session,
        )

    def ensure_connect(self):
        try:
            # List the clusters to check the connectivity
            _ = self.vsphere_sdk_client.vcenter.Cluster.list()
        except Unauthenticated:
            self.vsphere_sdk_client = self.get_client()
        except Exception as e:
            raise RuntimeError(f"failed to ensure the connect, exception: {e}")

    def get_vsphere_sdk_vm_obj(self, vm_id):
        """
        This function will get the vm object by vSphere SDK with the vm id
        """
        vms = self.vsphere_sdk_client.vcenter.VM.list(VM.FilterSpec(vms={vm_id}))
        if len(vms) == 0:
            logger.warning("VM with name ({}) not found by vSphere sdk".format(vm_id))
            return None
        return vms[0]

    def delete_vm_by_id(self, vm_id):
        """
        This function will delete the vm object by vSphere SDK with the vm id
        """
        status = self.vsphere_sdk_client.vcenter.vm.Power.get(vm_id)

        if status.state != HardPower.State.POWERED_OFF:
            self.vsphere_sdk_client.vcenter.vm.Power.stop(vm_id)

        logger.info("Deleting VM {}".format(vm_id))
        self.vsphere_sdk_client.vcenter.VM.delete(vm_id)

    def delete_vm_by_name(self, vm_name):
        """
        This function will delete the vm object by vSphere SDK with the vm name
        """
        vms = self.vsphere_sdk_client.vcenter.VM.list(VM.FilterSpec(names={vm_name}))

        if len(vms) > 0:
            logger.info("Deleting VM {}".format(vm_name))
            self.delete_vm_by_id(vms[0].vm)

    def list_all_hosts_in_cluster(self, cluster_id):
        """
        This function will list all host objects in cluster with this cluster id
        """
        host_filter_spec = Host.FilterSpec(clusters={cluster_id})
        return self.vsphere_sdk_client.vcenter.Host.list(host_filter_spec)

    def get_resource_pool_id_by_name(self, rp_name):
        """
        This function will get the resource pool id by vSphere SDK with the
        resource pool name
        """
        rp_filter_spec = ResourcePool.FilterSpec(names={rp_name})
        resource_pool_summaries = self.vsphere_sdk_client.vcenter.ResourcePool.list(
            rp_filter_spec
        )
        if not resource_pool_summaries:
            raise ValueError(
                "Resource pool with name '{}' not found".format(rp_filter_spec)
            )
        logger.debug(
            "Resource pool ID: {}".format(resource_pool_summaries[0].resource_pool)
        )
        return resource_pool_summaries[0].resource_pool

    def non_terminated_nodes(self, cluster_name, tag_filters):
        """
        This function is going to find all the running vSphere VMs created by Ray via
        the tag filters, the VMs should either be powered_on or be powered_off but has
        a tag "vsphere-node-status:creating"
        """
        with self.lock:
            nodes = []
            vms = self.vsphere_sdk_client.vcenter.VM.list()
            filters = tag_filters.copy()
            tag_cache = {}
            if TAG_RAY_CLUSTER_NAME not in tag_filters:
                filters[TAG_RAY_CLUSTER_NAME] = cluster_name
            for vm in vms:
                vm_id = vm.vm
                dynamic_id = DynamicID(type=Constants.TYPE_OF_RESOURCE, id=vm_id)

                matched_tags, all_tags = self.get_matched_tags(filters, dynamic_id)
                # Update the tag cache with latest tags
                tag_cache[vm_id] = all_tags

                if len(matched_tags) == len(filters):
                    # All the tags in the filters are matched on this vm
                    power_status = self.vsphere_sdk_client.vcenter.vm.Power.get(vm_id)

                    # Return VMs in powered-on and creating state
                    vsphere_node_status = all_tags.get(Constants.VSPHERE_NODE_STATUS)
                    if is_powered_on_or_creating(power_status, vsphere_node_status):
                        nodes.append(vm_id)

            logger.debug(f"Non terminated nodes are {nodes}")
            return nodes, tag_cache

    def is_vm_creating(self, vm_id):
        """
        This function will check if this vm is creating status
        """
        vns = Constants.VSPHERE_NODE_STATUS
        matched_tags, _ = self.get_matched_tags(
            {vns: Constants.VsphereNodeStatus.CREATING.value},
            DynamicID(type=Constants.TYPE_OF_RESOURCE, id=vm_id),
        )
        if matched_tags:
            return True
        return False

    def list_vm_tags(self, vm_id):
        """
        This function will list all the attached tags of vm
        """
        return self.vsphere_sdk_client.tagging.TagAssociation.list_attached_tags(vm_id)

    def get_matched_tags(self, tag_filters, vm_id):
        """
        This function will list all the attached tags of the vSphere object, convert
        the string formatted tag to k,v formatted. Then compare the attached tags to
        the ones in the filters.

        tag_filters will be a dict like {"tag_key1": "val1", "tag_key2": "val2"}
        vm_id will be the vSphere vm object id

        Return all the matched tags and all the tags the vSphere object has.
        vsphere_tag_to_kv_pair will ignore the tags not convertable to k,v pairs.
        """
        matched_tags = {}
        all_tags = {}

        for tag_id in self.list_vm_tags(vm_id):
            vsphere_vm_tag = self.vsphere_sdk_client.tagging.Tag.get(tag_id=tag_id).name
            tag_key_value = vsphere_tag_to_kv_pair(vsphere_vm_tag)
            if tag_key_value:
                tag_key, tag_value = tag_key_value[0], tag_key_value[1]

                if tag_key in tag_filters and tag_value == tag_filters[tag_key]:
                    matched_tags[tag_key] = tag_value

                all_tags[tag_key] = tag_value

        return matched_tags, all_tags

    def remove_tag_from_vm(self, tag_key_to_remove, vm_id):
        """
        This function will remove all tags of vm.
        Example: If a tag called node-status:initializing is present on the VM.
        If we would like to add a new value called finished with the node-status
        key.We'll need to delete the older tag node-status:initializing first
        before creating
        node-status:finished
        """
        dynamic_id = DynamicID(type=Constants.TYPE_OF_RESOURCE, id=vm_id)

        # List all the tags present on the VM.
        for tag_id in self.list_vm_tags(dynamic_id):
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

    def get_tag_id_by_name(self, tag_name, category_id):
        """
        This function is used to get tag id
        """
        for id in self.vsphere_sdk_client.tagging.Tag.list_tags_for_category(
            category_id
        ):
            if tag_name == self.vsphere_sdk_client.tagging.Tag.get(id).name:
                return id
        return None

    def get_category(self):
        """
        This function is used to get RAY_NODE category
        """
        for id in self.vsphere_sdk_client.tagging.Category.list():
            if (
                self.vsphere_sdk_client.tagging.Category.get(id).name
                == Constants.NODE_CATEGORY
            ):
                return id
        return None

    def create_category(self):
        """
        This function is used to create RAY_NODE category.
        This category is associated with VMs and supports
        multiple tags e.g. "Ray-Head-Node, Ray-Worker-Node-1 etc."
        """
        logger.info(f"Creating {Constants.NODE_CATEGORY} category")
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
            logger.critical(f"Unauthorized to create the category. Exception: {e}")
            raise e
        except Exception as e:
            logger.critical(e)
            raise e

        logger.info(f"Category {category_id} created")

        return category_id

    def create_node_tag(self, ray_node_tag, category_id):
        """
        This function is used to create tag "ray_node_tag" under category "category_id"
        """
        logger.debug(f"Creating {ray_node_tag} tag")
        tag_spec = self.vsphere_sdk_client.tagging.Tag.CreateSpec(
            ray_node_tag, "Ray node tag", category_id
        )
        tag_id = None
        try:
            tag_id = self.vsphere_sdk_client.tagging.Tag.create(tag_spec)
        except ErrorClients.Unauthorized as e:
            logger.critical(f"Unauthorized to create the tag. Exception: {e}")
            raise e
        except Exception as e:
            logger.critical(e)
            raise e

        logger.debug(f"Tag {tag_id} created")
        return tag_id

    def attach_tag(self, vm_id, resource_type, tag_id):
        """
        This function is used to attach tag to vm
        """
        dynamic_id = DynamicID(type=resource_type, id=vm_id)
        try:
            self.vsphere_sdk_client.tagging.TagAssociation.attach(tag_id, dynamic_id)
            logger.debug(f"Tag {tag_id} attached on VM {dynamic_id}")
        except Exception as e:
            logger.warning(f"Check that the tag is attachable to {resource_type}")
            raise e

    def set_node_tags(self, vm_id, tags):
        """
        This function is used to create category if category is not exists,
        crate tag if tag is not exists, and update the latest tag to VM.
        """
        with self.lock:
            category_id = self.get_category()
            if not category_id:
                category_id = self.create_category()

            for key, value in tags.items():
                tag = kv_pair_to_vsphere_tag(key, value)
                tag_id = self.get_tag_id_by_name(tag, category_id)
                if not tag_id:
                    tag_id = self.create_node_tag(tag, category_id)

                # If a tag with a key is present on the VM, then remove it
                # before updating the key with a new value.
                self.remove_tag_from_vm(key, vm_id)

                logger.debug(f"Attaching tag {tag} to {vm_id}")
                self.attach_tag(vm_id, Constants.TYPE_OF_RESOURCE, tag_id=tag_id)

    # This method is used to
    def tag_new_vm_instantly(self, vm_name, tags):
        """
        This function is used to do tag VMs as soon as VM show up on vCenter.
        """
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

    def update_vm_cpu(self, vm_id, cpu_count):
        """
        This function helps to Update CPUs Number of VM
        """
        update_spec = Cpu.UpdateSpec(count=cpu_count)
        logger.debug("vm.hardware.Cpu.update({}, {})".format(vm_id, update_spec))
        self.vsphere_sdk_client.vcenter.vm.hardware.Cpu.update(vm_id, update_spec)

    def update_vm_memory(self, vm_id, memory):
        """
        This function helps to Update Memory of VM
        """
        update_spec = Memory.UpdateSpec(size_mib=memory)
        logger.debug("vm.hardware.Memory.update({}, {})".format(vm_id, update_spec))
        self.vsphere_sdk_client.vcenter.vm.hardware.Memory.update(vm_id, update_spec)

    def deploy_ovf(
        self, lib_item, vm_name_target, resource_pool_id, host_id, datastore_id
    ):
        """
        This function is used to deploy vm from OVF
        """
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
            host_id=host_id,
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
        return vm.name
