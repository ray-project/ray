import logging

from pyVim.task import WaitForTask
from pyVmomi import vim

logger = logging.getLogger(__name__)


class ACCCard:
    def __init__(self, pci_id, custom_label=""):
        self.pciId = pci_id
        self.customLabel = custom_label

    def __str__(self):
        return "pciId: %s, customLabel: %s" % (self.pciId, self.customLabel)

    def __repr__(self):
        return "pciId: %s, customLabel: %s" % (self.pciId, self.customLabel)

    def __eq__(self, other):
        return self.pciId == other.pciId and self.customLabel == other.customLabel


def is_acc_available(host, acc_card):
    """
    This function checks if a ACC is available on an ESXi host
    """
    bindings = host.config.assignableHardwareBinding
    # No VM bind to any ACC card on this host
    if not bindings:
        return True

    for hardware in bindings:
        # There is a VM bind to this ACC card
        pci_id = acc_card.pciId
        if pci_id in hardware.instanceId and hardware.vm:
            logger.warning(f"ACC {pci_id} is used by VM {hardware.vm.name}")
            return False
    # No VM bind to this ACC card
    return True


def get_idle_acc_cards(host, acc_cards, desired_acc_number):
    """
    This function takes the number of desired ACC and all the ACC cards of a host.
    This function will select the unused ACC cards and put them into a list.
    If the length of the list > the number of the desired ACC, returns the list,
    otherwise returns an empty list to indicate that this host cannot fulfill the ACC
    requirement.
    """
    acc_idle_cards = []

    for acc_card in acc_cards:
        # Find one available ACC card on this host
        if is_acc_available(host, acc_card):
            acc_idle_cards.append(acc_card)

    if len(acc_idle_cards) < desired_acc_number:
        logger.warning(
            f"No enough unused ACC cards on host {host.name}, "
            f"expected number {desired_acc_number}, only {len(acc_idle_cards)}, "
            f"acc_cards {acc_idle_cards}"
        )
        return []

    return acc_idle_cards


def get_supported_accs(host, is_dynamic_pci_passthrough):
    """
    This function returns all the supported ACCs on this host,
    currently "supported" means Nvidia ACC.
    """
    acc_cards = []
    # This host has no ACC card, return empty array
    if host.config.graphicsInfo is None:
        return acc_cards
    # Currently, only support nvidia ACC
    for graphics_info in host.config.graphicsInfo:
        if "nvidia" in graphics_info.vendorName.lower():
            # When dynamic passthrough is enabled, if Hareware Label is
            # set, save the info, avoid to choose wrong ACC card
            # For example, 2 host, each host has 1 ACC card with same pciId.
            # If the two ACC cards have same Hareware Label, no problem.
            # But if the two ACC cards have different Hareware Label,
            # they are all visiable to all VMs. Need this Hareware Label
            # info to choose one which is on VM's host
            if (
                is_dynamic_pci_passthrough
                and host.config.assignableHardwareConfig.attributeOverride
            ):
                for attr in host.config.assignableHardwareConfig.attributeOverride:
                    if graphics_info.pciId in attr.instanceId:
                        acc_card = ACCCard(graphics_info.pciId, attr.value)
                        acc_cards.append(acc_card)
                        break
            else:
                acc_card = ACCCard(graphics_info.pciId)
                acc_cards.append(acc_card)

    return acc_cards


def get_vm_2_acc_cards_map(
    pyvmomi_sdk_provider, pool_name, desired_acc_number, is_dynamic_pci_passthrough
):
    """
    This function returns "vm, acc_cards" map, the key represents the VM
    and the value lists represents the available ACCs this VM can bind.
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

        # Get all acc cards from this host
        acc_cards = get_supported_accs(host, is_dynamic_pci_passthrough)
        if len(acc_cards) < desired_acc_number:
            # This is for debug purpose
            logger.warning(
                f"No enough supported ACC cards on host {host.name}, "
                f"expected number {desired_acc_number}, only {len(acc_cards)}, "
                f"acc_cards {acc_cards}"
            )
            continue

        # Get all available acc cards to see if it can fulfill the number
        acc_idle_cards = get_idle_acc_cards(
            host,
            acc_cards,
            desired_acc_number,
        )
        if acc_idle_cards:
            logger.info(
                f"Got Frozen VM {vm.name}, Host {host.name}, ACC Cards {acc_idle_cards}"
            )
            result[vm.name] = acc_idle_cards

    if not result:
        logger.error(f"No enough unused ACC cards for any VMs of pool {pool.name}")
    return result


def split_vm_2_acc_cards_map(vm_2_acc_cards_map, requested_acc_num):
    """
    This function split the `vm, all_acc_cards` map into array of
    "vm, acc_cards_with_requested_acc_num" map. The purpose to split the acc list is for
    avioding ACC contention when creating multiple VMs on one ESXi host.

    Parameters:
        vm_2_acc_cards_map: It is `vm, all_acc_cards` map, and you can get it by call
                          function `get_vm_2_acc_cards_map`.
        requested_acc_num: The number of ACC cards is requested by each ray node.

    Returns:
        Array of "vm, acc_cards_with_requested_acc_num" map.
        Each element of this array will be used in one ray node.

    Example:
        We have 3 hosts, `host1`, `host2`, and `host3`
        Each host has 1 frozen vm, `frozen-vm-1`, `frozen-vm-2`, and `frozen-vm-3`.
        Dynamic passthrough is enabled.
        pciId: 0000:3b:00.0, customLabel:
        `host1` has 3 ACC cards, with pciId/customLabel:
            `0000:3b:00.0/training-0`,
            `0000:3b:00.1/training-1`,
            `0000:3b:00.2/training-2`
        `host2` has 2 ACC cards, with pciId/customLabel:
            `0000:3b:00.3/training-3`,
            `0000:3b:00.4/training-4`
        `host3` has 1 ACC card, with pciId/customLabel:
            `0000:3b:00.5/training-5`
        And we provision a ray cluster with 3 nodes, each node need 1 ACC card

        In this case,  vm_2_acc_cards_map is like this:
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
        requested_acc_num is 1.

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
        `requested_acc_num` ACC, no more, no less.
    """
    acc_cards_map_array = []
    for vm_name in vm_2_acc_cards_map:
        acc_cards = vm_2_acc_cards_map[vm_name]
        i = 0
        j = requested_acc_num
        while j <= len(acc_cards):
            acc_cards_map = {vm_name: acc_cards[i:j]}
            acc_cards_map_array.append(acc_cards_map)
            i = j
            j = i + requested_acc_num

    return acc_cards_map_array


def get_acc_cards_from_vm(vm, desired_acc_number, is_dynamic_pci_passthrough):
    """
    This function will be called when there is only one single frozen VM.
    It returns acc_cards if enough ACCs are available for this VM,
    Or returns an empty list.
    """
    acc_cards = get_supported_accs(vm.runtime.host, is_dynamic_pci_passthrough)
    if len(acc_cards) < desired_acc_number:
        # Below code under this if is for logging purpose
        logger.warning(
            f"No enough supported ACC cards "
            f"for VM {vm.name} on host {vm.runtime.host.name}, "
            f"expected number {desired_acc_number}, only {len(acc_cards)}, "
            f"acc_cards {acc_cards}"
        )
        return []

    acc_idle_cards = get_idle_acc_cards(vm.runtime.host, acc_cards, desired_acc_number)
    if acc_idle_cards:
        logger.info(
            f"Got Frozen VM {vm.name}, Host {vm.runtime.host.name}, "
            f"ACC Cards {acc_idle_cards}"
        )
    else:
        logger.warning(
            f"No enough unused ACC cards "
            f"for VM {vm.name} on host {vm.runtime.host.name}"
        )
    return acc_idle_cards


def add_accs_to_vm(
    pyvmomi_sdk_provider, vm_name: str, acc_cards: list, is_dynamic_pci_passthrough
):
    """
    This function helps to add a list of acc to a VM by PCI passthrough. Steps:
    1. Power off the VM if it is not at the off state.
    2. Construct a reconfigure spec and reconfigure the VM.
    3. Power on the VM.
    """
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

    # add the ACCs into the reconfigure spec.
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
    for acc_card in acc_cards:
        pci_id = acc_card.pciId
        custom_label = acc_card.customLabel

        pci_passthru_info = id_to_pci_passthru_info[pci_id]
        device_id = pci_passthru_info.pciDevice.deviceId
        vendor_id = pci_passthru_info.pciDevice.vendorId

        backing = None

        if is_dynamic_pci_passthrough:
            logger.info(
                f"Plugin ACC card - Id {pci_id} deviceId {device_id} "
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
            logger.info(f"Plugin ACC card {pci_id} into VM {vm_name}")
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

        acc = vim.VirtualPCIPassthrough(key=key, backing=backing)

        device_change = vim.vm.device.VirtualDeviceSpec(operation="add", device=acc)

        config_spec.deviceChange.append(device_change)
        key += 1

    WaitForTask(vm_obj.ReconfigVM_Task(spec=config_spec))
    logger.debug(f"Power on VM {vm_name}...")
    WaitForTask(vm_obj.PowerOnVM_Task())
    logger.debug(f"VM {vm_name} is power on. Done.")


def set_acc_placeholder(array_obj, place_holder_number):
    for i in range(place_holder_number):
        array_obj.append({})
