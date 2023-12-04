import ipaddress
import time
from datetime import datetime
from enum import Enum


class Constants:
    TYPE_OF_RESOURCE = "VirtualMachine"
    NODE_CATEGORY = "ray"
    RAY_HEAD_FROZEN_VM_TAG = "ray-frozen-vm"
    VSPHERE_NODE_STATUS = "vsphere-node-status"
    CREATING_TAG_TIMEOUT = 120
    VM_FREEZE_TIMEOUT = 360
    VM_FREEZE_SLEEP_TIME = 0.5
    ENSURE_CONNECTION_PERIOD = 300

    class VsphereNodeStatus(Enum):
        # Enum for SDK clients
        CREATING = "creating"
        CREATED = "created"

    class SessionType(Enum):
        VERIFIED = "verified"
        UNVERIFIED = "unverified"


def is_ipv4(ip):
    try:
        ipaddress.IPv4Address(ip)
        return True
    except ipaddress.AddressValueError:
        return False


def singleton_client(cls):
    """
    A singleton decorator helps us to make sure there is only one instance
    """
    instances = {}

    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = (cls(*args, **kwargs), time.time())
        else:
            instance, last_checked_time = instances[cls]
            current_time = time.time()
            if current_time - last_checked_time > Constants.ENSURE_CONNECTION_PERIOD:
                instance.ensure_connect()
                instances[cls] = (instance, current_time)
        return instances[cls][0]

    return get_instance


def now_ts():
    return datetime.now().strftime("%Y%m%d-%H%M%S")
