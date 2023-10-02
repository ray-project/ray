from enum import Enum


class Constants:
    TYPE_OF_RESOURCE = "VirtualMachine"
    NODE_CATEGORY = "ray"
    RAY_HEAD_FROZEN_VM_TAG = "ray-frozen-vm"
    VSPHERE_NODE_STATUS = "vsphere-node-status"
    CREATING_TAG_TIMEOUT = 120
    VM_FREEZE_TIMEOUT = 360
    VM_FREEZE_SLEEP_TIME = 0.5

    class VsphereNodeStatus(Enum):
        # Enum for SDK clients
        CREATING = "creating"
        CREATED = "created"

    class SessionType(Enum):
        VERIFIED = "verified"
        UNVERIFIED = "unverified"
