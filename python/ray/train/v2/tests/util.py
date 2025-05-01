from unittest.mock import MagicMock

from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2._internal.execution.failure_handling import (
    FailureDecision,
    FailurePolicy,
)
from ray.train.v2._internal.execution.scaling_policy import (
    NoopDecision,
    ScalingDecision,
    ScalingPolicy,
)
from ray.train.v2._internal.execution.worker_group import (
    WorkerGroup,
    WorkerGroupContext,
    WorkerGroupPollStatus,
    WorkerGroupState,
    WorkerStatus,
)
from ray.train.v2._internal.util import time_monotonic, ObjectRefWrapper


class DummyWorkerGroup(WorkerGroup):

    _start_failure = None

    # TODO: Clean this up and use Mocks instead.
    def __init__(
        self,
        train_run_context: TrainRunContext,
        worker_group_context: WorkerGroupContext,
        callbacks=None,
    ):
        self._num_workers = worker_group_context.num_workers
        self._worker_group_state = None
        self._worker_statuses = {}

    def poll_status(self, *args, **kwargs) -> WorkerGroupPollStatus:
        return WorkerGroupPollStatus(
            worker_statuses=self._worker_statuses,
        )

    def _start(self):
        num_workers = self._num_workers
        if self._start_failure:
            raise self._start_failure

        self._worker_group_state = WorkerGroupState(
            start_time=time_monotonic(),
            workers=[MagicMock() for i in range(num_workers)],
            placement_group=MagicMock(),
            sync_actor=None,
        )

        self._worker_statuses = {
            i: WorkerStatus(running=True, error=None) for i in range(num_workers)
        }

    def shutdown(self):
        self._worker_group_state = None

    # === Test methods ===
    def error_worker(self, worker_index):
        status = self._worker_statuses[worker_index]
        status.error = RuntimeError(f"Worker {worker_index} failed")

    def finish_worker(self, worker_index):
        status = self._worker_statuses[worker_index]
        status.running = False

    @classmethod
    def set_start_failure(cls, start_failure):
        cls._start_failure = start_failure


class MockScalingPolicy(ScalingPolicy):
    def __init__(self, scaling_config):
        self._recovery_decision_queue = []
        self._monitor_decision_queue = []

        super().__init__(scaling_config)

    def make_decision_for_non_running_worker_group(self) -> ScalingDecision:
        if self._recovery_decision_queue:
            return self._recovery_decision_queue.pop(0)
        return NoopDecision()

    def make_decision_for_running_worker_group(
        self,
        worker_group_state: WorkerGroupState,
        worker_group_status: WorkerGroupPollStatus,
    ) -> ScalingDecision:
        if self._monitor_decision_queue:
            return self._monitor_decision_queue.pop(0)
        return NoopDecision()

    # === Test methods ===
    def queue_recovery_decision(self, decision):
        self._recovery_decision_queue.append(decision)

    def queue_monitor_decision(self, decision):
        self._monitor_decision_queue.append(decision)


class MockFailurePolicy(FailurePolicy):
    def __init__(self, failure_config):
        self._decision_queue = []

        super().__init__(failure_config)

    def make_decision(
        self, worker_group_status: WorkerGroupPollStatus
    ) -> FailureDecision:
        if self._decision_queue:
            return self._decision_queue.pop(0)
        return FailureDecision.NOOP

    # === Test methods ===
    def queue_decision(self, decision):
        self._decision_queue.append(decision)


class DummyObjectRefWrapper(ObjectRefWrapper):
    """Mock object that returns the object passed in without going through ray.put."""

    def __init__(self, obj):
        self._obj = obj

    def get(self):
        return self._obj
