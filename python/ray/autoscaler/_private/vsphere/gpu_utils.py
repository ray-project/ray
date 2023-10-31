import logging

from pyVim.task import WaitForTask
from pyVmomi import vim

from ray.autoscaler._private.vsphere.sdk_provider import ClientType, get_sdk_provider

logger = logging.getLogger(__name__)


def get_gpu_id_if_available(host, gpu):
    """
    This function checks if a GPU is available on an ESXi host, if yes,
    returns the GPU ID of the GPU. Otherwise, returns None.
    """
    bindings = host.config.assignableHardwareBinding
    # No VM bind to any GPU card on this host
    if not bindings:
        return gpu.pciId

    for hardware in bindings:
        # There is a VM bind to this GPU card
        if gpu.pciId in hardware.instanceId and hardware.vm:
            logger.warning(f"GPU {gpu.pciId} is used by VM {hardware.vm.name}")
            return None
    # No VM bind to this GPU card
    return gpu.pciId


def get_idle_gpu_ids(host, gpus, desired_gpu_number):
    """
    This function takes the number of desired GPU and all the GPU cards of a host.
    This function will select the unused GPU cards' GPU IDs and put them into a list.
    If the length of the list > the number of the desired GPU, returns the list,
    otherwise returns an empty list to indicate that this host cannot fulfill the GPU
    requirement.
    """
    gpu_ids = []

    for gpu in gpus:
        # Find one available GPU card on this host
        gpu_id = get_gpu_id_if_available(host, gpu)
        if gpu_id:
            gpu_ids.append(gpu_id)

    if len(gpu_ids) < desired_gpu_number:
        logger.warning(
            f"No enough unused GPU cards on host {host.name}, "
            f"expected number {desired_gpu_number}, only {len(gpu_ids)}, "
            f"gpu_ids {gpu_ids}"
        )
        return []

    # Find no GPU card on this host
    return gpu_ids


def get_supported_gpus(host):
    """
    This function returns all the supported GPUs on this host,
    currently "supported" means Nvidia GPU.
    """
    gpus = []
    # This host has no GPU card, return empty array
    if host.config.graphicsInfo is None:
        return gpus
    # Currently, only support nvidia GPU
    for gpu in host.config.graphicsInfo:
        if "nvidia" in gpu.vendorName.lower():
            gpus.append(gpu)

    return gpus


def get_vm_2_gpu_ids_map(pool_name, desired_gpu_number):
    """
    This function returns "vm, gpu_ids" map, the key represents the VM
    and the value lists represents the available GPUs this VM can bind.
    With this map, we can find which frozen VM we can do instant clone to create the
    Ray nodes.
    """
    result = {}
    pyvmomi_sdk_provider = get_sdk_provider(ClientType.PYVMOMI_SDK)
    pool = pyvmomi_sdk_provider.get_pyvmomi_obj([vim.ResourcePool], pool_name)
    if not pool.vm:
        logger.error(f"No frozen-vm in pool {pool.name}")
        return result

    for vm in pool.vm:
        host = vm.runtime.host

        # Get all gpu cards from this host
        gpus = get_supported_gpus(host)
        if len(gpus) < desired_gpu_number:
            # This is for debug purpose
            # To get all supported GPU cards' GPU IDs
            gpu_ids = []
            for gpu in gpus:
                gpu_ids.append(gpu.pciId)
            logger.warning(
                f"No enough supported GPU cards on host {host.name}, "
                f"expected number {desired_gpu_number}, only {len(gpus)}, "
                f"gpu_ids {gpu_ids}"
            )
            continue

        # Get all available gpu cards to see if it can fulfill the number
        gpu_ids = get_idle_gpu_ids(host, gpus, desired_gpu_number)
        if gpu_ids:
            logger.info(f"Got Frozen VM {vm.name}, Host {host.name}, GPU ids {gpu_ids}")
            result[vm.name] = gpu_ids

    if not result:
        logger.error(f"No enough unused GPU cards for any VMs of pool {pool.name}")
    return result


def split_vm_2_gpu_ids_map(vm_2_gpu_ids_map, requested_gpu_num, node_number):
    """
    This function split the `vm, all_gpu_ids` map into array of
    "vm, gpu_ids_with_requested_gpu_num" map. The purpose to split the gpu list is for
    creating more VMs on one ESXi host, each VM can bind a certain number of GPUs.

    Parameters:
        vm_2_gpu_ids_map: It is `vm, all_gpu_ids` map, and you can get it by call
                          function `get_vm_2_gpu_ids_map`.
        requested_gpu_num: The number of GPU cards is requested by each ray node.
        node_number: The number of ray nodes

    Returns:
        Array of "vm, gpu_ids_with_requested_gpu_num" map.
        Each element of this array will be used in one ray node.

    Example:
        We have 3 hosts, `host1`, `host2`, and `host3`
        Each host has 1 frozen vm, `frozen-vm-1`, `frozen-vm-2`, and `frozen-vm-3`.
        `host1` has 3 GPU cards, `0000:3b:00.0`, `0000:3b:00.1`,`0000:3b:00.2`
        `host2` has 2 GPU cards, `0000:3b:00.3`,`0000:3b:00.4`
        `host3` has 1 GPU card, `0000:3b:00.5`
        And we provison a ray cluster with 3 nodes, each node need 1 GPU card

        In this case,  vm_2_gpu_ids_map is like this:
        {
            'frozen-vm-1': ['0000:3b:00.0', '0000:3b:00.1', '0000:3b:00.2'],
            'frozen-vm-2': ['0000:3b:00.3', '0000:3b:00.4'],
            'frozen-vm-3': ['0000:3b:00.5'],
        }
        requested_gpu_num is 1, and node_number is 3.

        After call the bove with this funtion, it returns this array:
        [
            { 'frozen-vm-1' : ['0000:3b:00.0'] },
            { 'frozen-vm-1' : ['0000:3b:00.1'] },
            { 'frozen-vm-1' : ['0000:3b:00.2'] },
            { 'frozen-vm-2' : ['0000:3b:00.3'] },
            { 'frozen-vm-2' : ['0000:3b:00.4'] },
            { 'frozen-vm-3' : ['0000:3b:00.5'] },
        ]

        Each element of this array could be used in 1 ray node with exactly
        `requested_gpu_num` GPU, no more, no less.
    """
    gpu_ids_map_array = []
    for vm_name in vm_2_gpu_ids_map:
        gpu_ids = vm_2_gpu_ids_map[vm_name]
        i = 0
        j = requested_gpu_num
        while j <= len(gpu_ids):
            gpu_ids_maps = {vm_name: gpu_ids[i:j]}
            gpu_ids_map_array.append(gpu_ids_maps)
            i = j
            j = i + requested_gpu_num

    # When there are not enough gpu cards for all ray nodes
    if len(gpu_ids_map_array) < node_number:
        logger.error(
            f"No enough available GPU cards to assigned to nodes "
            f"expected enough for {node_number} nodes, "
            f"only enough for {len(gpu_ids_map_array)} nodes, "
            f"gpu_ids_map_array {gpu_ids_map_array}"
        )
        return []

    return gpu_ids_map_array


def get_gpu_ids_from_vm(vm, desired_gpu_number):
    """
    This function will be called when there is only one single frozen VM.
    It returns gpu_ids if enough GPUs are available for this VM,
    Or returns an empty list.
    """
    gpus = get_supported_gpus(vm.runtime.host)
    if len(gpus) < desired_gpu_number:
        # Below code under this if is for logging purpose
        # To get all supported GPU cards' GPU IDs
        gpu_ids = []
        for gpu in gpus:
            gpu_ids.append(gpu.pciId)
        logger.warning(
            f"No enough supported GPU cards "
            f"for VM {vm.name} on host {vm.runtime.host.name}, "
            f"expected number {desired_gpu_number}, only {len(gpus)}, "
            f"gpu_ids {gpu_ids}"
        )
        return []

    gpu_ids = get_idle_gpu_ids(vm.runtime.host, gpus, desired_gpu_number)
    if gpu_ids:
        logger.info(
            f"Got Frozen VM {vm.name}, Host {vm.runtime.host.name}, GPU ids {gpu_ids}"
        )
    else:
        logger.warning(
            f"No enough unused GPU cards "
            f"for VM {vm.name} on host {vm.runtime.host.name}"
        )
    return gpu_ids


def add_gpus_to_vm(vm_name: str, gpu_ids: list):
    """
    This function helps to add a list of gpu to a VM by PCI passthrough. Steps:
    1. Power off the VM if it is not at the off state.
    2. Construct a reconfigure spec and reconfigure the VM.
    3. Power on the VM.
    """
    pyvmomi_sdk_provider = get_sdk_provider(ClientType.PYVMOMI_SDK)
    vm_obj = pyvmomi_sdk_provider.get_pyvmomi_obj([vim.VirtualMachine], vm_name)
    # The VM is supposed to be at powered on status after instant clone.
    # We need to power it off.
    if vm_obj.runtime.powerState == vim.VirtualMachinePowerState.poweredOn:
        logger.debug(f"Power off VM {vm_name}...")
        WaitForTask(vm_obj.PowerOffVM_Task())
        logger.debug(f"VM {vm_name} is power off. Done.")

    config_spec = vim.vm.ConfigSpec()

    # The below 2 advanced configs are needed for a VM to have a passthru PCI device
    config_spec.extraConfig = [
        vim.option.OptionValue(key="pciPassthru.64bitMMIOSizeGB", value="64"),
        vim.option.OptionValue(key="pciPassthru.use64bitMMIO", value="TRUE"),
    ]

    # PCI passthru device requires the memory to be hard reserved.
    config_spec.memoryReservationLockedToMax = True
    # https://kb.vmware.com/s/article/89638
    # Due to above known issue, we need to disable the cpu hot add for Ray nodes.
    # This will have no impact on our feature.
    config_spec.cpuHotAddEnabled = False

    # add the GPUs into the reconfigure spec.
    config_spec.deviceChange = []

    # get the VM's plugable PCI devices
    pci_passthroughs = vm_obj.environmentBrowser.QueryConfigTarget(
        host=None
    ).pciPassthrough

    # The key is the id, such as '0000:3b:00.0'
    # The value is an instance of struct vim.vm.PciPassthroughInfo, please google it.
    id_to_pci_passthru_info = {item.pciDevice.id: item for item in pci_passthroughs}

    # The reason for this magic number -100 is following this page
    # https://gist.github.com/wiggin15/319b5e828c42af3aed40
    # The explanation can be found here:
    # https://vdc-download.vmware.com/vmwb-repository/dcr-public/
    # 790263bc-bd30-48f1-af12-ed36055d718b/e5f17bfc-ecba-40bf-a04f-376bbb11e811/
    # vim.vm.device.VirtualDevice.html
    key = -100
    for gpu_id in gpu_ids:
        logger.info(f"Plugin GPU card {gpu_id} into VM {vm_name}")
        pci_passthru_info = id_to_pci_passthru_info[gpu_id]
        backing = vim.VirtualPCIPassthroughDeviceBackingInfo(
            # This hex trick is what we must do to construct a backing info.
            # https://gist.github.com/wiggin15/319b5e828c42af3aed40
            # Otherwise the VM cannot be powered on.
            deviceId=hex(pci_passthru_info.pciDevice.deviceId % 2**16).lstrip("0x"),
            id=gpu_id,
            systemId=pci_passthru_info.systemId,
            vendorId=pci_passthru_info.pciDevice.vendorId,
            deviceName=pci_passthru_info.pciDevice.deviceName,
        )

        gpu = vim.VirtualPCIPassthrough(key=key, backing=backing)

        device_change = vim.vm.device.VirtualDeviceSpec(operation="add", device=gpu)

        config_spec.deviceChange.append(device_change)
        key += 1

    WaitForTask(vm_obj.ReconfigVM_Task(spec=config_spec))
    logger.debug(f"Power on VM {vm_name}...")
    WaitForTask(vm_obj.PowerOnVM_Task())
    logger.debug(f"VM {vm_name} is power on. Done.")
