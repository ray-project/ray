from unittest.mock import MagicMock

import pytest

import ray
from ray.train.v2._internal.constants import HEALTH_CHECK_INTERVAL_S_ENV_VAR
from ray.train.v2._internal.exceptions import (
    WorkerGroupStartupFailedError,
    WorkerGroupStartupTimeoutError,
)
from ray.train.v2._internal.execution.callback import ControllerCallback
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2._internal.execution.controller import TrainController
from ray.train.v2._internal.execution.controller.state import (
    InitializingState,
    SchedulingState,
    ReschedulingState,
    RunningState,
    RestartingState,
    ResizingState,
    ErroredState,
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
    WorkerGroupContext,
    WorkerGroupPollStatus,
    WorkerGroupState,
    WorkerStatus,
)
from ray.train.v2._internal.util import time_monotonic
from ray.train.v2.api.config import RunConfig, ScalingConfig


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


@pytest.fixture(autouse=True)
def patch_worker_group(monkeypatch):
    monkeypatch.setattr(TrainController, "worker_group_cls", DummyWorkerGroup)
    # Make polling interval 0 to speed up tests
    monkeypatch.setenv(HEALTH_CHECK_INTERVAL_S_ENV_VAR, "0")
    yield


@pytest.fixture(autouse=True)
def ray_start():
    ray.init()
    yield
    ray.shutdown()


def test_resize():
    scaling_policy = MockScalingPolicy(scaling_config=ScalingConfig())
    train_run_context = TrainRunContext(run_config=RunConfig())
    controller = TrainController(
        train_fn=lambda: None,
        train_run_context=train_run_context,
        scaling_policy=scaling_policy,
        failure_policy=MockFailurePolicy(failure_config=None),
    )

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

    assert isinstance(controller.get_state(), InitializingState)
    assert controller.get_worker_group() is None

    # Start with 1 worker
    scaling_policy.queue_recovery_decision(
        ResizeDecision(num_workers=1, resources_per_worker={})
    )
    controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), SchedulingState)
    assert controller.get_worker_group() is None

    controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), RunningState)

    worker_group = controller.get_worker_group()
    assert worker_group is not None
    assert worker_group.has_started()
    num_workers = len(worker_group.get_workers())
    assert num_workers == 1

    for decision in decisions:
        prev_num_workers = num_workers

        scaling_policy.queue_monitor_decision(decision)

        if isinstance(decision, NoopDecision):
            controller._run_control_loop_iteration()
            assert isinstance(controller.get_state(), RunningState)

            worker_group = controller.get_worker_group()
            assert worker_group is not None
            assert worker_group.has_started()
            num_workers = len(worker_group.get_workers())
            assert num_workers == prev_num_workers
        else:
            controller._run_control_loop_iteration()
            assert isinstance(controller.get_state(), ResizingState)
            controller._run_control_loop_iteration()
            assert isinstance(controller.get_state(), SchedulingState)
            controller._run_control_loop_iteration()
            assert isinstance(controller.get_state(), RunningState)

            worker_group = controller.get_worker_group()
            assert worker_group is not None
            assert worker_group.has_started()
            num_workers = len(worker_group.get_workers())
            assert num_workers == decision.num_workers


def test_failure_handling():
    scaling_policy = MockScalingPolicy(scaling_config=ScalingConfig())
    failure_policy = MockFailurePolicy(failure_config=None)
    train_run_context = TrainRunContext(run_config=RunConfig())
    controller = TrainController(
        train_fn=lambda: None,
        train_run_context=train_run_context,
        scaling_policy=scaling_policy,
        failure_policy=failure_policy,
    )

    assert isinstance(controller.get_state(), InitializingState)
    scaling_policy.queue_recovery_decision(
        ResizeDecision(num_workers=2, resources_per_worker={})
    )
    controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), SchedulingState)
    controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), RunningState)

    controller.get_worker_group().error_worker(1)
    failure_policy.queue_decision(FailureDecision.RESTART)
    controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), RestartingState)

    scaling_policy.queue_recovery_decision(
        ResizeDecision(num_workers=4, resources_per_worker={})
    )
    controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), SchedulingState)
    controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), RunningState)

    controller.get_worker_group().error_worker(3)
    failure_policy.queue_decision(FailureDecision.RAISE)
    controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), ErroredState)


@pytest.mark.parametrize(
    "error_type", [WorkerGroupStartupFailedError, WorkerGroupStartupTimeoutError(2)]
)
def test_worker_group_start_failure(monkeypatch, error_type):
    """Check that controller can gracefully handle worker group start failures."""
    scaling_policy = MockScalingPolicy(scaling_config=ScalingConfig())
    failure_policy = MockFailurePolicy(failure_config=None)
    train_run_context = TrainRunContext(run_config=RunConfig())
    controller = TrainController(
        train_fn=lambda: None,
        train_run_context=train_run_context,
        scaling_policy=scaling_policy,
        failure_policy=failure_policy,
    )
    DummyWorkerGroup.set_start_failure(error_type)
    monkeypatch.setattr(TrainController, "worker_group_cls", DummyWorkerGroup)

    assert isinstance(controller.get_state(), InitializingState)

    scaling_policy.queue_recovery_decision(
        ResizeDecision(num_workers=2, resources_per_worker={})
    )

    controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), SchedulingState)

    # Worker group will fail to start, but controller should not raise
    # and should go into RESCHEDULING state.
    controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), ReschedulingState)

    # Let the worker group start successfully the 2nd time.
    DummyWorkerGroup.set_start_failure(None)
    monkeypatch.setattr(TrainController, "worker_group_cls", DummyWorkerGroup)
    scaling_policy.queue_recovery_decision(
        ResizeDecision(num_workers=2, resources_per_worker={})
    )

    controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), SchedulingState)

    controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), RunningState)


def test_poll_frequency(monkeypatch):
    monkeypatch.setenv(HEALTH_CHECK_INTERVAL_S_ENV_VAR, "1")

    sleep_calls = []
    monkeypatch.setattr("time.sleep", lambda t: sleep_calls.append(t))
    train_run_context = TrainRunContext(run_config=RunConfig())

    controller = TrainController(
        train_fn=lambda: None,
        train_run_context=train_run_context,
        scaling_policy=None,
        failure_policy=None,
    )
    # Mock worker group to avoid actual polling
    controller._worker_group = MagicMock()

    num_polls = 5
    for _ in range(num_polls):
        controller._poll_workers()

    # No sleep calls for the first poll
    assert len(sleep_calls) == num_polls - 1


def test_controller_callback():
    """Check that all controller callback hooks are called."""

    class AssertCallback(ControllerCallback):
        def __init__(self):
            self.start_called = False
            self.latest_state_update = None
            self.failure_decision_called = False
            self.scaling_decision_called = False
            self.shutdown_called = False

        def after_controller_start(self):
            self.start_called = True

        def after_controller_state_update(
            self,
            previous_state: TrainControllerState,
            current_state: TrainControllerState,
        ):
            self.latest_state_update = (previous_state, current_state)

        def before_controller_execute_failure_decision(
            self,
            failure_decision: FailureDecision,
        ):
            self.failure_decision_called = True

        def before_controller_execute_scaling_decision(
            self,
            scaling_decision: ScalingDecision,
        ):
            self.scaling_decision_called = True

        def before_controller_shutdown(self):
            self.shutdown_called = True

    callback = AssertCallback()

    scaling_policy = MockScalingPolicy(scaling_config=ScalingConfig())
    failure_policy = MockFailurePolicy(failure_config=None)
    train_run_context = TrainRunContext(run_config=RunConfig())

    controller = TrainController(
        train_fn=lambda: None,
        train_run_context=train_run_context,
        scaling_policy=scaling_policy,
        failure_policy=failure_policy,
        callbacks=[callback],
    )

    controller._start()
    assert callback.start_called

    scaling_policy.queue_recovery_decision(
        ResizeDecision(num_workers=2, resources_per_worker={})
    )

    controller._run_control_loop_iteration()
    assert not callback.scaling_decision_called
    assert isinstance(callback.latest_state_update[0], InitializingState)
    assert isinstance(callback.latest_state_update[1], SchedulingState)

    controller._run_control_loop_iteration()
    assert callback.scaling_decision_called
    assert isinstance(callback.latest_state_update[0], SchedulingState)
    assert isinstance(callback.latest_state_update[1], RunningState)

    controller.get_worker_group().error_worker(1)
    failure_policy.queue_decision(FailureDecision.RAISE)

    assert not callback.failure_decision_called
    controller._run_control_loop_iteration()
    assert callback.failure_decision_called
    assert isinstance(callback.latest_state_update[0], RunningState)
    assert isinstance(callback.latest_state_update[1], ErroredState)

    controller._shutdown()
    assert callback.shutdown_called


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
