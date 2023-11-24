import logging
import threading
from enum import Enum

logger = logging.getLogger(__name__)


class Policy(Enum):
    ROUNDROBIN = "round-robin"


class SchedulerFactory:
    @classmethod
    def get_scheduler(cls, frozen_vms_resource_pool, policy_name=Policy.ROUNDROBIN):
        if policy_name == Policy.ROUNDROBIN:
            return RoundRobinScheduler(frozen_vms_resource_pool)
        raise RuntimeError(f"Unsupported schedule policy: {policy_name}")


class RoundRobinScheduler:
    def __init__(self, frozen_vms_resource_pool):
        self.current_vm_index = 0
        self.lock = threading.Lock()
        self.frozen_vms_resource_pool = frozen_vms_resource_pool
        self.vms = None
        logger.debug("Inited the round robin scheduler for vSphere VMs")

    def next_frozen_vm(self):
        if not self.vms:
            self.vms = self.frozen_vms_resource_pool.vm
            if len(self.vms) <= 0:
                raise ValueError(
                    f"No vm in resource pool {self.frozen_vms_resource_pool}!"
                )
        with self.lock:
            logger.debug(
                "current_vm_index=%d",
                self.current_vm_index,
            )
            vm = self.vms[self.current_vm_index]
            self.current_vm_index += 1
            if self.current_vm_index >= len(self.vms):
                self.current_vm_index = 0
        return vm
