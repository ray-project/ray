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
        return ResizeDecision(
            num_workers=self.scaling_config.num_workers,
            resources_per_worker=self.scaling_config._resources_per_worker_not_none,
        )

    def make_decision_for_running_worker_group(
        self,
        worker_group_state: WorkerGroupState,
        worker_group_status: WorkerGroupPollStatus,
    ) -> ScalingDecision:
        self._maybe_send_resource_request()
        return NoopDecision()
