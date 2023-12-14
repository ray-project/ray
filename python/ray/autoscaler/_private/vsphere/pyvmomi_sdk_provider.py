import atexit
import logging
import ssl
import time
from enum import Enum

from pyVim.connect import Disconnect, SmartStubAdapter, VimSessionOrientedStub
from pyVim.task import WaitForTask
from pyVmomi import vim, vmodl

from ray.autoscaler._private.vsphere.utils import Constants, is_ipv4, singleton_client

logger = logging.getLogger(__name__)


class ObjectType(Enum):
    # Enum for Object Type
    ResourcePool = "ResourcePool"
    VirtualMachine = "VirtualMachine"
    Datastore = "Datastore"
    ClusterComputeResource = "ClusterComputeResource"


class KeyType(Enum):
    # Enum for Key Type, name or object id
    Name = "Name"
    ObjectID = "ObjectID"


def get_object_type(vimtype):
    if vimtype == [vim.ResourcePool]:
        return ObjectType.ResourcePool
    elif vimtype == [vim.VirtualMachine]:
        return ObjectType.VirtualMachine
    elif vimtype == [vim.Datastore]:
        return ObjectType.Datastore
    elif vimtype == [vim.ClusterComputeResource]:
        return ObjectType.ClusterComputeResource
    else:
        raise ValueError("Invalid Object Type")


def check_obj_validness(obj):
    if not obj:
        return False
    try:
        # check the validness of the cached vmomi obj
        _ = obj.name
        return True
    except vmodl.fault.ManagedObjectNotFound:
        return False
    except Exception as e:
        logger.error(f"Got an exception during check the pyvmomi obj validness: {e}")
        return False


@singleton_client
class PyvmomiSdkProvider:
    def __init__(
        self,
        server,
        user,
        password,
        session_type: Constants.SessionType,
        port: int = 443,
    ):
        # Instance variables
        self.server = server
        self.user = user
        self.password = password
        self.session_type = session_type
        self.port = port

        # Instance parameters
        self.timeout = 0
        self.cached = {
            KeyType.Name: {
                ObjectType.ResourcePool: {},
                ObjectType.VirtualMachine: {},
                ObjectType.Datastore: {},
                ObjectType.ClusterComputeResource: {},
            },
            KeyType.ObjectID: {
                ObjectType.ResourcePool: {},
                ObjectType.VirtualMachine: {},
                ObjectType.Datastore: {},
                ObjectType.ClusterComputeResource: {},
            },
        }

        # Connect using a session oriented connection
        # Ref. https://github.com/vmware/pyvmomi/issues/347
        self.pyvmomi_sdk_client = self.get_client()
        if not self.pyvmomi_sdk_client:
            raise ValueError("Could not connect to the specified host")
        atexit.register(Disconnect, self.pyvmomi_sdk_client)

    def get_client(self):
        if self.session_type == Constants.SessionType.UNVERIFIED:
            context_obj = ssl._create_unverified_context()
        else:
            # TODO: support verified context
            pass
        credentials = VimSessionOrientedStub.makeUserLoginMethod(
            self.user, self.password
        )
        smart_stub = SmartStubAdapter(
            host=self.server,
            port=self.port,
            sslContext=context_obj,
            connectionPoolTimeout=self.timeout,
        )
        session_stub = VimSessionOrientedStub(smart_stub, credentials)
        return vim.ServiceInstance("ServiceInstance", session_stub)

    def get_obj_from_cache(self, vimtype, name, obj_id):
        object_type = get_object_type(vimtype)
        if name:
            object_cache = self.cached[KeyType.Name][object_type]
            obj = object_cache.get(name)
            if check_obj_validness(obj):
                if obj.name != name:
                    # example: If someone has changed the VM name on the vSphere side,
                    # then create another VM with the same name. Then this cache item
                    # will be dirty because it still points to the previous VM obj.
                    object_cache.pop(name)
                    object_cache[obj.name] = obj
                    return None
                return obj
            if obj:
                object_cache.pop(name)
        elif obj_id:
            object_cache = self.cached[KeyType.ObjectID][object_type]
            obj = object_cache.get(obj_id)
            if check_obj_validness(obj):
                return obj
            if obj:
                object_cache.pop(obj_id)
        return None

    def put_obj_in_cache(self, vimtype, obj):
        object_type = get_object_type(vimtype)
        self.cached[KeyType.Name][object_type][obj.name] = obj
        self.cached[KeyType.ObjectID][object_type][obj._moId] = obj

    def get_pyvmomi_obj(self, vimtype, name=None, obj_id=None):
        """
        This function will return the vSphere object.
        The argument for `vimtype` can be "vim.VM", "vim.Host", "vim.Datastore", etc.
        Then either the name or the object id need to be provided.
        To check all such object information, you can go to the managed object board
        page of your vCenter Server, such as: https://<your_vc_ip/mob
        """
        if not name and not obj_id:
            # Raise runtime error because this is not user fault
            raise RuntimeError("Either name or obj id must be provided")
        if self.pyvmomi_sdk_client is None:
            raise RuntimeError("Must init pyvmomi_sdk_client first")

        cached_obj = self.get_obj_from_cache(vimtype, name, obj_id)
        if cached_obj:
            return cached_obj

        container = self.pyvmomi_sdk_client.content.viewManager.CreateContainerView(
            self.pyvmomi_sdk_client.content.rootFolder, vimtype, True
        )
        obj = None
        # If both name and moid are provided we will prioritize name.
        if name:
            for candidate in container.view:
                if candidate.name == name:
                    obj = candidate
        elif obj_id:
            for candidate in container.view:
                if obj_id in str(candidate):
                    obj = candidate
        if obj:
            self.put_obj_in_cache(vimtype, obj)
            return obj
        raise ValueError(
            f"Cannot find the object with type {vimtype} on vSphere with"
            f"name={name} and obj_id={obj_id}"
        )

    def ensure_connect(self):
        try:
            _ = self.pyvmomi_sdk_client.RetrieveContent()
        except vim.fault.NotAuthenticated:
            self.pyvmomi_sdk_client = self.get_client()
            self.cached.clear()
        except Exception as e:
            self.cached.clear()
            raise RuntimeError(f"failed to ensure the connect, exception: {e}")

    def name_to_id(self, vimtype, name):
        obj = self.get_pyvmomi_obj(vimtype, name)
        return obj._moId

    def power_on_vm(self, vm_name):
        vm = self.get_pyvmomi_obj([vim.VirtualMachine], vm_name)
        if vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOff:
            logger.debug(f"Frozen VM {vm._moId} is off. Powering it ON")
            WaitForTask(vm.PowerOnVM_Task())
            logger.debug(f"VM {vm_name} is power on. Done.")

    def power_off_vm(self, vm_name):
        vm_obj = self.get_pyvmomi_obj([vim.VirtualMachine], vm_name)
        logger.debug(f"power_off_vm: {vm_name}...")
        time.sleep(10)
        logger.debug(f"power_off_vm: powerState={vm_obj.runtime.powerState}...")
        if vm_obj.runtime.powerState == vim.VirtualMachinePowerState.poweredOn:
            logger.debug(f"Power off VM {vm_name}...")
            WaitForTask(vm_obj.PowerOffVM_Task())
            logger.debug(f"VM {vm_name} is power off. Done.")

    def is_vm_power_on(self, vm_id):
        vm = self.get_pyvmomi_obj([vim.VirtualMachine], obj_id=vm_id)
        return vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOn

    def is_vm_power_off(self, vm_id):
        vm = self.get_pyvmomi_obj([vim.VirtualMachine], obj_id=vm_id)
        return vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOff

    def wait_until_vm_is_frozen(self, vm_name):
        """The function waits until a VM goes into the frozen state."""

        vm = self.get_pyvmomi_obj([vim.VirtualMachine], vm_name)
        start = time.time()

        while time.time() - start < Constants.VM_FREEZE_TIMEOUT:
            time.sleep(Constants.VM_FREEZE_SLEEP_TIME)
            if vm.runtime.instantCloneFrozen:
                logger.info(
                    "VM {} went into frozen state successfully.".format(vm.name)
                )
                return vm

        raise RuntimeError("VM {} didn't go into frozen state".format(vm.name))

    def get_vm_external_ip(self, vm_id):
        # Return the external IP of the VM
        # Fetch vSphere VM object
        vm = self.get_pyvmomi_obj([vim.VirtualMachine], obj_id=vm_id)
        if vm.guest.net:
            for ipaddr in vm.guest.net[0].ipAddress:
                if is_ipv4(ipaddr):
                    logger.debug("Fetch IP {} for VM {}".format(ipaddr, vm.name))
                    return ipaddr
        else:
            logger.warning(f"Net of VM {vm.name} is not ready")
        logger.warning(f"External IPv4 address of VM {vm.name} is not available")
        return None

    def instance_clone_vm(
        self,
        source_vm_name,
        target_vm_name,
        target_resource_pool_name,
        target_datastore_name,
    ):
        # If resource pool is not provided, then the resource pool
        # of the source VM will also be the resource pool of the target VM.
        resource_pool = (
            self.get_pyvmomi_obj([vim.ResourcePool], target_resource_pool_name)
            if target_resource_pool_name
            else None
        )
        # If datastore is not provided, then the datastore
        # of the source VM will also be the resource pool of the target VM.
        datastore = (
            self.get_pyvmomi_obj([vim.Datastore], target_datastore_name)
            if target_datastore_name
            else None
        )
        vm_relocate_spec = vim.vm.RelocateSpec(
            pool=resource_pool,
            datastore=datastore,
        )
        instant_clone_spec = vim.vm.InstantCloneSpec(
            name=target_vm_name, location=vm_relocate_spec
        )
        source_vm = self.get_pyvmomi_obj([vim.VirtualMachine], source_vm_name)
        WaitForTask(source_vm.InstantClone_Task(spec=instant_clone_spec))
        logger.info(f"Clone VM {target_vm_name} from Frozen-VM {source_vm_name}")
