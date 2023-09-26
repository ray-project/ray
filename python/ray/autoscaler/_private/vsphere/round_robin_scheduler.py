import logging
import threading

logger = logging.getLogger(__name__)


class RoundRobinScheduler:
    def __init__(self, frozen_resource_pool):

        self.frozen_resource_pool = frozen_resource_pool
        self.current_vm_index = 0
        self.lock = threading.Lock()
        self.vms = self.frozen_resource_pool.vm
        if len(self.vms) <= 0:
            raise ValueError(f"No vm found in resource pool {frozen_resource_pool}!")
        logger.debug("Inited the round robin scheduler for vSphere VMs")

    def choose_frozen_vm(self):
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
