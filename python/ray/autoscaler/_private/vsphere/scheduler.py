import logging
from enum import Enum

logger = logging.getLogger(__name__)


class SchedulerFactory:
    class Policy(Enum):
        ROUNDROBIN = "round-robin"

    def __init__(self, frozen_vms_resource_pool, policy_name=Policy.ROUNDROBIN):
        default_policy_name = self.Policy.ROUNDROBIN

        self.policy_name = default_policy_name if policy_name == "" else policy_name
        # Should only create class once
        if self.policy_name == self.Policy.ROUNDROBIN:
            from ray.autoscaler._private.vsphere.round_robin_scheduler import (
                RoundRobinScheduler,
            )

            self.Scheduler = RoundRobinScheduler(frozen_vms_resource_pool)
        else:
            raise ValueError(f"Unknown policy {policy_name}")

    def get_scheduler(self):
        return self.Scheduler
