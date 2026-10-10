from ray.train.v2._internal.execution.scaling_policy import (
    NoopDecision,
    ResizeDecision,
    ScalingDecision,
    ScalingPolicy,
)
from ray.train.v2._internal.execution.worker_group import (
    WorkerGroupPollStatus,
    WorkerGroupState,
)


class FixedScalingPolicy(ScalingPolicy):
    def _get_num_workers_for_resource_request(self) -> int:
        return self.scaling_config.num_workers

    def make_decision_for_non_running_worker_group(self) -> ScalingDecision:
        self._maybe_send_resource_request()
        num_workers = self.scaling_config.num_workers
        resources_per_worker = self.scaling_config._resources_per_worker_not_none

        if not self._should_pin_to_reservation():
            return ResizeDecision(
                num_workers=num_workers,
                resources_per_worker=resources_per_worker,
            )

        # Wait for the coordinator to reserve every worker, the same way the
        # elastic policy waits for `min_workers`. The cached view gates the
        # wait; the pins come from a fresh one.
        reserved_resources = self._get_reserved_resources()
        if self._get_reserved_label_selectors(reserved_resources, num_workers):
            reserved_resources = self._get_reserved_resources(recompute=True)
        label_selectors = self._get_reserved_label_selectors(
            reserved_resources, num_workers
        )
        if label_selectors is None:
            self._maybe_log_waiting_for_reservation(
                num_reserved=self._count_reserved_workers(reserved_resources),
                num_required=num_workers,
            )
            return NoopDecision()

        return ResizeDecision(
            num_workers=num_workers,
            resources_per_worker=resources_per_worker,
            label_selectors=label_selectors,
        )

    def make_decision_for_running_worker_group(
        self,
        worker_group_state: WorkerGroupState,
        worker_group_status: WorkerGroupPollStatus,
    ) -> ScalingDecision:
        self._maybe_send_resource_request()
        return NoopDecision()
