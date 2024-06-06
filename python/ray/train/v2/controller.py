import logging
import time
from typing import Callable

from ray.train.v2.api.config import ScalingConfig
from ray.train.v2.scaling_policy import ScalingDecision
from ray.train.v2.worker_group.worker_group import WorkerGroup, WorkerGroupStatus


logger = logging.getLogger(__file__)


class TrainController:
    def __init__(self, train_fn: Callable, scaling_config: ScalingConfig):
        self._train_fn = train_fn

        self._scaling_config = scaling_config

        self._scaling_policy = scaling_config.scaling_policy_cls(scaling_config)
        self._worker_group: WorkerGroup = self._scaling_config.worker_group_cls()

    def relaunch_training(self, num_workers: int, resources_per_worker: dict):
        self._worker_group.shutdown()

        self._worker_group.start(
            num_workers=num_workers, resources_per_worker=resources_per_worker
        )
        self._worker_group.run_train_fn(self._train_fn)

    def _handle_scaling(self, decision: ScalingDecision):
        if decision.action == ScalingDecision.RESIZE:
            self.relaunch_training(
                num_workers=decision.num_workers,
                resources_per_worker=self._scaling_config.resources_per_worker,
            )

    def _handle_failures(self, worker_group_status: WorkerGroupStatus):
        worker_errors = worker_group_status.errors
        if not worker_errors:
            return

        errors = worker_errors.values()
        logger.error(
            "Restarting workers after encountering "
            f"{len(errors)} worker errors:\n" + ("\n".join([str(e) for e in errors]))
        )
        self._worker_group.shutdown()

    def _finished(self, worker_group_status: WorkerGroupStatus):
        return not worker_group_status.errors and worker_group_status.finished

    def _poll_workers(self) -> WorkerGroupStatus:
        poll_start = time.monotonic()
        status = self._worker_group.poll(
            timeout=self._scaling_config.health_check_interval_s
        )
        poll_duration = time.monotonic() - poll_start

        if poll_duration < self._scaling_config.health_check_interval_s:
            time.sleep(self._scaling_config.health_check_interval_s - poll_duration)

        return status

    def _shutdown(self):
        self._worker_group.shutdown()

    def run(self):
        while True:
            worker_group_status = self._poll_workers()

            self._handle_failures(worker_group_status)

            scaling_decision = self._scaling_policy.make_decision(worker_group_status)
            self._handle_scaling(scaling_decision)

            if self._finished(worker_group_status):
                break

        self._shutdown()
