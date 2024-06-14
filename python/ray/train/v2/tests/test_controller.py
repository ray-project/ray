from typing import Optional

import pytest

from ray.train.v2._internal.constants import RAY_TRAIN_HEALTH_CHECK_INTERVAL_S
from ray.train.v2._internal.execution.controller import (
    TrainController,
    TrainControllerState,
)
from ray.train.v2._internal.execution.failure_handling import (
    FailureDecision,
    FailurePolicy,
)
from ray.train.v2._internal.execution.scaling_policy import (
    NoopDecision,
    ResizeDecision,
    ScalingDecision,
    ScalingPolicy,
)
from ray.train.v2._internal.execution.worker_group import (
    WorkerGroup,
    WorkerGroupStatus,
    WorkerStatus,
)
from ray.train.v2._internal.util import time_monotonic
from ray.train.v2.api.config import ScalingConfig


class DummyWorkerGroup(WorkerGroup):
    def __init__(self):
        self._active = False
        self._num_workers = 0
        self._latest_restart_time = float("-inf")
        self._worker_statuses = {}

    def run_train_fn(self, train_fn):
        pass

    def poll_status(self, timeout: Optional[float] = None) -> WorkerGroupStatus:
        return WorkerGroupStatus(
            num_workers=self._num_workers,
            latest_restart_time=self._latest_restart_time,
            worker_statuses=self._worker_statuses,
        )

    def start(self, num_workers, resources_per_worker):
        self._num_workers = num_workers
        self._latest_restart_time = time_monotonic()
        self._worker_statuses = {
            i: WorkerStatus(running=True, error=None) for i in range(num_workers)
        }

    def shutdown(self):
        self._num_workers = 0
        self._worker_statuses = {}

    # === Test methods ===
    def error_worker(self, worker_index):
        status = self._worker_statuses[worker_index]
        status.error = RuntimeError(f"Worker {worker_index} failed")

    def finish_worker(self, worker_index):
        status = self._worker_statuses[worker_index]
        status.running = False


class MockScalingPolicy(ScalingPolicy):
    def __init__(self, scaling_config):
        self._recovery_decision_queue = []
        self._monitor_decision_queue = []

    def make_decision_for_non_running_worker_group(
        self, worker_group_status: WorkerGroupStatus
    ) -> ScalingDecision:
        if self._recovery_decision_queue:
            return self._recovery_decision_queue.pop(0)
        return NoopDecision()

    def make_decision_for_running_worker_group(
        self, worker_group_status: WorkerGroupStatus
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

    def make_decision(self, worker_group_status: WorkerGroupStatus) -> FailureDecision:
        if self._decision_queue:
            return self._decision_queue.pop(0)
        return FailureDecision.NOOP

    # === Test methods ===
    def queue_decision(self, decision):
        self._decision_queue.append(decision)


@pytest.fixture(autouse=True)
def patch_worker_group(monkeypatch):
    monkeypatch.setattr(TrainController, "worker_group_cls", DummyWorkerGroup)
    # Make polling interval 0 to speed up tests
    monkeypatch.setenv(RAY_TRAIN_HEALTH_CHECK_INTERVAL_S, "0")
    yield


def test_resize():
    scaling_policy = MockScalingPolicy(scaling_config=ScalingConfig())
    controller = TrainController(
        train_fn=lambda: None,
        scaling_policy=scaling_policy,
        failure_policy=MockFailurePolicy(failure_config=None),
    )
    worker_group = controller.get_worker_group()

    decisions = [
        NoopDecision(),
        ResizeDecision(num_workers=2, resources_per_worker={}),
        NoopDecision(),
        NoopDecision(),
        ResizeDecision(num_workers=10, resources_per_worker={}),
        NoopDecision(),
        ResizeDecision(num_workers=10, resources_per_worker={}),
        ResizeDecision(num_workers=20, resources_per_worker={}),
        NoopDecision(),
        ResizeDecision(num_workers=5, resources_per_worker={}),
    ]

    # Start with 1 worker
    scaling_policy.queue_recovery_decision(
        ResizeDecision(num_workers=1, resources_per_worker={})
    )
    controller._run_control_loop_iteration()
    prev_worker_group_status = worker_group.poll_status()
    assert prev_worker_group_status.num_workers == 1

    for decision in decisions:
        scaling_policy.queue_monitor_decision(decision)
        controller._run_control_loop_iteration()
        worker_group_status = worker_group.poll_status()

        if isinstance(decision, NoopDecision):
            assert (
                worker_group_status.num_workers == prev_worker_group_status.num_workers
            )
        else:
            assert worker_group_status.num_workers == decision.num_workers

        prev_worker_group_status = worker_group_status


def test_failure_handling():
    scaling_policy = MockScalingPolicy(scaling_config=ScalingConfig())
    failure_policy = MockFailurePolicy(failure_config=None)
    controller = TrainController(
        train_fn=lambda: None,
        scaling_policy=scaling_policy,
        failure_policy=failure_policy,
    )
    worker_group = controller.get_worker_group()

    assert controller.get_state() == TrainControllerState.INITIALIZING
    scaling_policy.queue_recovery_decision(
        ResizeDecision(num_workers=2, resources_per_worker={})
    )
    controller._run_control_loop_iteration()
    assert controller.get_state() == TrainControllerState.RUNNING

    worker_group.error_worker(1)
    failure_policy.queue_decision(FailureDecision.RESTART)
    controller._run_control_loop_iteration()
    assert controller.get_state() == TrainControllerState.RECOVERING

    scaling_policy.queue_recovery_decision(
        ResizeDecision(num_workers=4, resources_per_worker={})
    )
    controller._run_control_loop_iteration()
    assert controller.get_state() == TrainControllerState.RUNNING

    worker_group.error_worker(3)
    failure_policy.queue_decision(FailureDecision.RAISE)
    controller._run_control_loop_iteration()
    assert controller.get_state() == TrainControllerState.ERRORED


def test_poll_frequency(monkeypatch):
    monkeypatch.setenv(RAY_TRAIN_HEALTH_CHECK_INTERVAL_S, "1")

    sleep_calls = []
    monkeypatch.setattr("time.sleep", lambda t: sleep_calls.append(t))

    controller = TrainController(
        train_fn=lambda: None, scaling_policy=None, failure_policy=None
    )
    num_polls = 5
    for _ in range(num_polls):
        controller._poll_workers()

    # No sleep calls for the first poll
    assert len(sleep_calls) == num_polls - 1


if __name__ == "__main__":
    pytest.main(["-v", __file__])
