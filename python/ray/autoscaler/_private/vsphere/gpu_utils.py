import logging

from pyVim.task import WaitForTask
from pyVmomi import vim

logger = logging.getLogger(__name__)


class GPUCard:
    def __init__(self, pci_id, custom_label=""):
        self.pciId = pci_id
        self.customLabel = custom_label

    def __str__(self):
        return "pciId: %s, customLabel: %s" % (self.pciId, self.customLabel)

    def __repr__(self):
        return "pciId: %s, customLabel: %s" % (self.pciId, self.customLabel)

    def __eq__(self, other):
        return self.pciId == other.pciId and self.customLabel == other.customLabel


def is_gpu_available(host, gpu_card):
    """
    This function checks if a GPU is available on an ESXi host
    """
    bindings = host.config.assignableHardwareBinding
    # No VM bind to any GPU card on this host
    if not bindings:
        return True

    for hardware in bindings:
        # There is a VM bind to this GPU card
        pci_id = gpu_card.pciId
        if pci_id in hardware.instanceId and hardware.vm:
            logger.warning(f"GPU {pci_id} is used by VM {hardware.vm.name}")
            return False
    # No VM bind to this GPU card
    return True


def get_idle_gpu_cards(host, gpu_cards, desired_gpu_number):
    """
    This function takes the number of desired GPU and all the GPU cards of a host.
    This function will select the unused GPU cards and put them into a list.
    If the length of the list > the number of the desired GPU, returns the list,
    otherwise returns an empty list to indicate that this host cannot fulfill the GPU
    requirement.
    """
    gpu_idle_cards = []

    for gpu_card in gpu_cards:
        # Find one available GPU card on this host
        if is_gpu_available(host, gpu_card):
            gpu_idle_cards.append(gpu_card)

    if len(gpu_idle_cards) < desired_gpu_number:
        logger.warning(
            f"No enough unused GPU cards on host {host.name}, "
            f"expected number {desired_gpu_number}, only {len(gpu_idle_cards)}, "
            f"gpu_cards {gpu_idle_cards}"
        )
        return []

    return gpu_idle_cards


def get_supported_gpus(host, is_dynamic_pci_passthrough):
    """
    This function returns all the supported GPUs on this host,
    currently "supported" means Nvidia GPU.
    """
    gpu_cards = []
    # This host has no GPU card, return empty array
    if host.config.graphicsInfo is None:
        return gpu_cards
    # Currently, only support nvidia GPU
    for graphics_info in host.config.graphicsInfo:
        if "nvidia" in graphics_info.vendorName.lower():
            # When dynamic passthrough is enabled, if Hareware Label is
            # set, save the info, avoid to choose wrong GPU card
            # For example, 2 host, each host has 1 GPU card with same pciId.
            # If the two GPU cards have same Hareware Label, no problem.
            # But if the two GPU cards have different Hareware Label,
            # they are all visiable to all VMs. Need this Hareware Label
            # info to choose one which is on VM's host
            if (
                is_dynamic_pci_passthrough
                and host.config.assignableHardwareConfig.attributeOverride
            ):
                for attr in host.config.assignableHardwareConfig.attributeOverride:
                    if graphics_info.pciId in attr.instanceId:
                        gpu_card = GPUCard(graphics_info.pciId, attr.value)
                        gpu_cards.append(gpu_card)
                        break
            else:
                gpu_card = GPUCard(graphics_info.pciId)
                gpu_cards.append(gpu_card)

    return gpu_cards


def get_vm_2_gpu_cards_map(
    pyvmomi_sdk_provider, pool_name, desired_gpu_number, is_dynamic_pci_passthrough
):
    """
    This function returns "vm, gpu_cards" map, the key represents the VM
    and the value lists represents the available GPUs this VM can bind.
    With this map, we can find which frozen VM we can do instant clone to create the
    Ray nodes.
    """
    result = {}
    pool = pyvmomi_sdk_provider.get_pyvmomi_obj([vim.ResourcePool], pool_name)
    if not pool.vm:
        logger.error(f"No frozen-vm in pool {pool.name}")
        return result

    for vm in pool.vm:
        host = vm.runtime.host

        # Get all gpu cards from this host
        gpu_cards = get_supported_gpus(host, is_dynamic_pci_passthrough)
        if len(gpu_cards) < desired_gpu_number:
            # This is for debug purpose
            logger.warning(
                f"No enough supported GPU cards on host {host.name}, "
                f"expected number {desired_gpu_number}, only {len(gpu_cards)}, "
                f"gpu_cards {gpu_cards}"
            )
            continue

        # Get all available gpu cards to see if it can fulfill the number
        gpu_idle_cards = get_idle_gpu_cards(
            host,
            gpu_cards,
            desired_gpu_number,
        )
        if gpu_idle_cards:
            logger.info(
                f"Got Frozen VM {vm.name}, Host {host.name}, GPU Cards {gpu_idle_cards}"
            )
            result[vm.name] = gpu_idle_cards

    if not result:
        logger.error(f"No enough unused GPU cards for any VMs of pool {pool.name}")
    return result


def split_vm_2_gpu_cards_map(vm_2_gpu_cards_map, requested_gpu_num):
    """
    This function split the `vm, all_gpu_cards` map into array of
    "vm, gpu_cards_with_requested_gpu_num" map. The purpose to split the gpu list is for
    avioding GPU contention when creating multiple VMs on one ESXi host.

    Parameters:
        vm_2_gpu_cards_map: It is `vm, all_gpu_cards` map, and you can get it by call
                          function `get_vm_2_gpu_cards_map`.
        requested_gpu_num: The number of GPU cards is requested by each ray node.

    Returns:
        Array of "vm, gpu_cards_with_requested_gpu_num" map.
        Each element of this array will be used in one ray node.

    Example:
        We have 3 hosts, `host1`, `host2`, and `host3`
        Each host has 1 frozen vm, `frozen-vm-1`, `frozen-vm-2`, and `frozen-vm-3`.
        Dynamic passthrough is enabled.
        pciId: 0000:3b:00.0, customLabel:
        `host1` has 3 GPU cards, with pciId/customLabel:
            `0000:3b:00.0/training-0`,
            `0000:3b:00.1/training-1`,
            `0000:3b:00.2/training-2`
        `host2` has 2 GPU cards, with pciId/customLabel:
            `0000:3b:00.3/training-3`,
            `0000:3b:00.4/training-4`
        `host3` has 1 GPU card, with pciId/customLabel:
            `0000:3b:00.5/training-5`
        And we provision a ray cluster with 3 nodes, each node need 1 GPU card

        In this case,  vm_2_gpu_cards_map is like this:
        {
            'frozen-vm-1': [
                pciId: 0000:3b:00.0, customLabel: training-0,
                pciId: 0000:3b:00.1, customLabel: training-1,
                pciId: 0000:3b:00.2, customLabel: training-2,
            ],
            'frozen-vm-2': [
                pciId: 0000:3b:00.3, customLabel: training-3,
                pciId: 0000:3b:00.4, customLabel: training-4,
            ],
            'frozen-vm-3': [ pciId: 0000:3b:00.5, customLabel: training-5 ],
        }
        requested_gpu_num is 1.

        After call the above with this funtion, it returns this array:
        [
            { 'frozen-vm-1' : [ pciId: 0000:3b:00.0, customLabel: training-0 ] },
            { 'frozen-vm-1' : [ pciId: 0000:3b:00.1, customLabel: training-1 ] },
            { 'frozen-vm-1' : [ pciId: 0000:3b:00.2, customLabel: training-2 ] },
            { 'frozen-vm-2' : [ pciId: 0000:3b:00.3, customLabel: training-3 ] },
            { 'frozen-vm-2' : [ pciId: 0000:3b:00.4, customLabel: training-4 ] },
            { 'frozen-vm-3' : [ pciId: 0000:3b:00.5, customLabel: training-5 ] },
        ]

        Each element of this array could be used in 1 ray node with exactly
        `requested_gpu_num` GPU, no more, no less.
    """
    gpu_cards_map_array = []
    for vm_name in vm_2_gpu_cards_map:
        gpu_cards = vm_2_gpu_cards_map[vm_name]
        i = 0
        j = requested_gpu_num
        while j <= len(gpu_cards):
            gpu_cards_map = {vm_name: gpu_cards[i:j]}
            gpu_cards_map_array.append(gpu_cards_map)
            i = j
            j = i + requested_gpu_num

    return gpu_cards_map_array


def get_gpu_cards_from_vm(vm, desired_gpu_number, is_dynamic_pci_passthrough):
    """
    This function will be called when there is only one single frozen VM.
    It returns gpu_cards if enough GPUs are available for this VM,
    Or returns an empty list.
    """
    gpu_cards = get_supported_gpus(vm.runtime.host, is_dynamic_pci_passthrough)
    if len(gpu_cards) < desired_gpu_number:
        # Below code under this if is for logging purpose
        logger.warning(
            f"No enough supported GPU cards "
            f"for VM {vm.name} on host {vm.runtime.host.name}, "
            f"expected number {desired_gpu_number}, only {len(gpu_cards)}, "
            f"gpu_cards {gpu_cards}"
        )
        return []

    gpu_idle_cards = get_idle_gpu_cards(vm.runtime.host, gpu_cards, desired_gpu_number)
    if gpu_idle_cards:
        logger.info(
            f"Got Frozen VM {vm.name}, Host {vm.runtime.host.name}, "
            f"GPU Cards {gpu_idle_cards}"
        )
    else:
        logger.warning(
            f"No enough unused GPU cards "
            f"for VM {vm.name} on host {vm.runtime.host.name}"
        )
    return gpu_idle_cards


def plug_gpu_cards_to_vm(
    pyvmomi_sdk_provider, vm_name: str, gpu_cards: list, is_dynamic_pci_passthrough
):
    """
    This function helps to add a list of gpu to a VM by PCI passthrough. Steps:
    1. Power off the VM if it is not at the off state.
    2. Construct a reconfigure spec and reconfigure the VM.
    3. Power on the VM.
    """

    # The VM is supposed to be at powered on status after instant clone.
    # We need to power it off.
    pyvmomi_sdk_provider.power_off_vm(vm_name)

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
    vm_obj = pyvmomi_sdk_provider.get_pyvmomi_obj([vim.VirtualMachine], vm_name)
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
    for gpu_card in gpu_cards:
        pci_id = gpu_card.pciId
        custom_label = gpu_card.customLabel
        pci_passthru_info = id_to_pci_passthru_info[pci_id]
        device_id = pci_passthru_info.pciDevice.deviceId
        vendor_id = pci_passthru_info.pciDevice.vendorId

        backing = None

        if is_dynamic_pci_passthrough:
            logger.info(
                f"Plugin GPU card - Id {pci_id} deviceId {device_id} "
                f"vendorId {vendor_id} customLabel {custom_label} into VM {vm_name}"
            )
            allowed_device = vim.VirtualPCIPassthroughAllowedDevice(
                vendorId=vendor_id,
                deviceId=device_id,
            )
            backing = vim.VirtualPCIPassthroughDynamicBackingInfo(
                allowedDevice=[allowed_device],
                customLabel=custom_label,
                assignedId=str(device_id),
            )
        else:
            logger.info(f"Plugin GPU card {pci_id} into VM {vm_name}")
            backing = vim.VirtualPCIPassthroughDeviceBackingInfo(
                # This hex trick is what we must do to construct a backing info.
                # https://gist.github.com/wiggin15/319b5e828c42af3aed40
                # Otherwise the VM cannot be powered on.
                deviceId=hex(pci_passthru_info.pciDevice.deviceId % 2**16).lstrip(
                    "0x"
                ),
                id=pci_id,
                systemId=pci_passthru_info.systemId,
                vendorId=pci_passthru_info.pciDevice.vendorId,
                deviceName=pci_passthru_info.pciDevice.deviceName,
            )

        gpu = vim.VirtualPCIPassthrough(key=key, backing=backing)
        device_change = vim.vm.device.VirtualDeviceSpec(operation="add", device=gpu)
        config_spec.deviceChange.append(device_change)
        key += 1

    WaitForTask(vm_obj.ReconfigVM_Task(spec=config_spec))
    pyvmomi_sdk_provider.power_on_vm(vm_name)


def set_gpu_placeholder(array_obj, place_holder_number):
    for i in range(place_holder_number):
        array_obj.append({})
