from unittest.mock import create_autospec

import pytest

import ray
from ray.train.backend import Backend, BackendConfig
from ray.train.v2._internal.constants import HEALTH_CHECK_INTERVAL_S_ENV_VAR
from ray.train.v2._internal.exceptions import (
    WorkerGroupStartupFailedError,
    WorkerGroupStartupTimeoutError,
)
from ray.train.v2._internal.execution.callback import ControllerCallback
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2._internal.execution.controller import TrainController
from ray.train.v2._internal.execution.controller.state import (
    AbortedState,
    ErroredState,
    InitializingState,
    ReschedulingState,
    ResizingState,
    RestartingState,
    RunningState,
    SchedulingState,
    ShuttingDownState,
    TrainControllerState,
)
from ray.train.v2._internal.execution.failure_handling import FailureDecision
from ray.train.v2._internal.execution.scaling_policy import (
    NoopDecision,
    ResizeDecision,
)
from ray.train.v2._internal.execution.worker_group import (
    WorkerGroupPollStatus,
    WorkerStatus,
)
from ray.train.v2.api.config import ScalingConfig
from ray.train.v2.api.exceptions import ControllerError
from ray.train.v2.tests.util import (
    DummyObjectRefWrapper,
    DummyWorkerGroup,
    MockFailurePolicy,
    MockScalingPolicy,
    create_dummy_run_context,
)

pytestmark = pytest.mark.usefixtures("mock_runtime_context")


@pytest.fixture(autouse=True)
def patch_worker_group(monkeypatch):
    monkeypatch.setattr(TrainController, "worker_group_cls", DummyWorkerGroup)
    # Make polling interval 0 to speed up tests
    monkeypatch.setenv(HEALTH_CHECK_INTERVAL_S_ENV_VAR, "0")
    yield
    DummyWorkerGroup.set_poll_failure(None)
    DummyWorkerGroup.set_start_failure(None)


@pytest.fixture(autouse=True)
def ray_start():
    ray.init()
    yield
    ray.shutdown()


@pytest.mark.asyncio
async def test_resize():
    scaling_policy = MockScalingPolicy(scaling_config=ScalingConfig())
    train_run_context = create_dummy_run_context()
    controller = TrainController(
        train_fn_ref=DummyObjectRefWrapper(lambda: None),
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

    # Noop decision should be ignored
    scaling_policy.queue_recovery_decision(NoopDecision())
    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), InitializingState)
    assert controller.get_worker_group() is None

    # Start with 1 worker
    scaling_policy.queue_recovery_decision(
        ResizeDecision(num_workers=1, resources_per_worker={})
    )
    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), SchedulingState)
    assert controller.get_worker_group() is None

    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), RunningState)

    worker_group = controller.get_worker_group()
    assert worker_group is not None
    assert worker_group.has_started()
    num_workers = len(worker_group.get_workers())
    assert num_workers == 1

    for decision in decisions:
        prev_num_workers = num_workers
        prev_worker_group = worker_group

        scaling_policy.queue_monitor_decision(decision)

        if isinstance(decision, NoopDecision):
            await controller._run_control_loop_iteration()
            assert isinstance(controller.get_state(), RunningState)

            worker_group = controller.get_worker_group()
            assert worker_group is not None
            assert worker_group is prev_worker_group
            assert worker_group.has_started()
            num_workers = len(worker_group.get_workers())
            assert num_workers == prev_num_workers
        else:
            # TODO: refactor common "run and check" sequences like this.
            await controller._run_control_loop_iteration()
            assert isinstance(controller.get_state(), ResizingState)
            await controller._run_control_loop_iteration()
            assert isinstance(controller.get_state(), SchedulingState)
            await controller._run_control_loop_iteration()
            assert isinstance(controller.get_state(), RunningState)

            worker_group = controller.get_worker_group()
            assert worker_group is not None
            assert worker_group is not prev_worker_group
            assert worker_group.has_started()
            num_workers = len(worker_group.get_workers())
            assert num_workers == decision.num_workers


@pytest.mark.asyncio
async def test_failure_handling():
    scaling_policy = MockScalingPolicy(scaling_config=ScalingConfig())
    failure_policy = MockFailurePolicy(failure_config=None)
    train_run_context = create_dummy_run_context()
    controller = TrainController(
        train_fn_ref=DummyObjectRefWrapper(lambda: None),
        train_run_context=train_run_context,
        scaling_policy=scaling_policy,
        failure_policy=failure_policy,
    )

    assert isinstance(controller.get_state(), InitializingState)
    scaling_policy.queue_recovery_decision(
        ResizeDecision(num_workers=2, resources_per_worker={})
    )
    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), SchedulingState)
    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), RunningState)

    worker_group_before_failure = controller.get_worker_group()
    controller.get_worker_group().error_worker(1)
    failure_policy.queue_decision(FailureDecision.RETRY)
    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), RestartingState)

    scaling_policy.queue_recovery_decision(
        ResizeDecision(num_workers=4, resources_per_worker={})
    )
    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), SchedulingState)
    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), RunningState)

    # After failure recovery, worker group should be a new instance (full restart).
    assert controller.get_worker_group() is not worker_group_before_failure

    DummyWorkerGroup.set_poll_failure(RuntimeError("Simulated poll failure"))
    failure_policy.queue_decision(FailureDecision.RAISE)
    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), ShuttingDownState)
    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), ErroredState)


@pytest.mark.parametrize(
    "error_type", [WorkerGroupStartupFailedError, WorkerGroupStartupTimeoutError(2)]
)
@pytest.mark.asyncio
async def test_worker_group_start_failure(error_type):
    """Check that controller can gracefully handle worker group start failures."""
    scaling_policy = MockScalingPolicy(scaling_config=ScalingConfig())
    failure_policy = MockFailurePolicy(failure_config=None)
    train_run_context = create_dummy_run_context()
    controller = TrainController(
        train_fn_ref=DummyObjectRefWrapper(lambda: None),
        train_run_context=train_run_context,
        scaling_policy=scaling_policy,
        failure_policy=failure_policy,
    )
    DummyWorkerGroup.set_start_failure(error_type)

    assert isinstance(controller.get_state(), InitializingState)

    scaling_policy.queue_recovery_decision(
        ResizeDecision(num_workers=2, resources_per_worker={})
    )

    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), SchedulingState)

    # Worker group will fail to start, but controller should not raise
    # and should go into RESCHEDULING state.
    failure_policy.queue_decision(FailureDecision.RETRY)
    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), ReschedulingState)

    # Let the worker group start successfully the 2nd time.
    DummyWorkerGroup.set_start_failure(None)
    scaling_policy.queue_recovery_decision(
        ResizeDecision(num_workers=2, resources_per_worker={})
    )

    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), SchedulingState)

    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), RunningState)


@pytest.mark.asyncio
async def test_poll_frequency(monkeypatch):
    monkeypatch.setenv(HEALTH_CHECK_INTERVAL_S_ENV_VAR, "1")

    async def sleep_mock(t):
        sleep_calls.append(t)

    sleep_calls = []
    monkeypatch.setattr("asyncio.sleep", sleep_mock)
    train_run_context = create_dummy_run_context()
    scaling_policy = MockScalingPolicy(scaling_config=ScalingConfig())

    controller = TrainController(
        train_fn_ref=DummyObjectRefWrapper(lambda: None),
        train_run_context=train_run_context,
        scaling_policy=scaling_policy,
        failure_policy=None,
    )
    # Mock worker group to avoid actual polling
    controller._worker_group = create_autospec(DummyWorkerGroup, instance=True)
    controller._worker_group.poll_status.return_value = WorkerGroupPollStatus(
        worker_statuses={}
    )

    num_polls = 5
    for _ in range(num_polls):
        await controller._poll_workers()

    # No sleep calls for the first poll
    assert len(sleep_calls) == num_polls - 1


@pytest.mark.asyncio
async def test_controller_callback():
    """Check that all controller callback hooks are called."""

    class AssertCallback(ControllerCallback):
        def __init__(self):
            self.start_called = False
            self.latest_state_update = None
            self.failure_decision_called = False
            self.resize_decision_called = False
            self.shutdown_called = False

        def after_controller_start(self, train_run_context: TrainRunContext):
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

        def before_controller_execute_resize_decision(
            self,
            resize_decision: ResizeDecision,
        ):
            self.resize_decision_called = True

        def before_controller_shutdown(self):
            self.shutdown_called = True

    callback = AssertCallback()

    scaling_policy = MockScalingPolicy(scaling_config=ScalingConfig())
    failure_policy = MockFailurePolicy(failure_config=None)
    train_run_context = create_dummy_run_context()

    controller = TrainController(
        train_fn_ref=DummyObjectRefWrapper(lambda: None),
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

    await controller._run_control_loop_iteration()
    assert not callback.resize_decision_called
    assert isinstance(callback.latest_state_update[0], InitializingState)
    assert isinstance(callback.latest_state_update[1], SchedulingState)

    await controller._run_control_loop_iteration()
    assert callback.resize_decision_called
    assert isinstance(callback.latest_state_update[0], SchedulingState)
    assert isinstance(callback.latest_state_update[1], RunningState)

    controller.get_worker_group().error_worker(1)
    failure_policy.queue_decision(FailureDecision.RAISE)

    assert not callback.failure_decision_called
    await controller._run_control_loop_iteration()
    assert callback.failure_decision_called
    assert isinstance(callback.latest_state_update[0], RunningState)
    assert isinstance(callback.latest_state_update[1], ShuttingDownState)

    await controller._run_control_loop_iteration()
    assert isinstance(callback.latest_state_update[0], ShuttingDownState)
    assert isinstance(callback.latest_state_update[1], ErroredState)
    assert callback.shutdown_called


@pytest.mark.asyncio
async def test_controller_abort(monkeypatch):
    mock_exit_actor = create_autospec(ray.actor.exit_actor)
    monkeypatch.setattr("ray.actor.exit_actor", mock_exit_actor)
    scaling_policy = MockScalingPolicy(scaling_config=ScalingConfig())
    failure_policy = MockFailurePolicy(failure_config=None)
    train_run_context = create_dummy_run_context()
    controller = TrainController(
        train_fn_ref=DummyObjectRefWrapper(lambda: None),
        train_run_context=train_run_context,
        scaling_policy=scaling_policy,
        failure_policy=failure_policy,
    )
    await controller.abort()
    assert mock_exit_actor.call_count == 1
    assert isinstance(controller.get_state(), AbortedState)


@pytest.mark.asyncio
async def test_controller_callback_error_during_state_update_is_handled():
    """A controller callback hook failure is surfaced via CallbackManager.

    This should not crash the control loop; it should transition into shutdown and
    eventually into an errored terminal state. Callback failures during terminal
    state transitions should not cause the controller to loop indefinitely.
    """

    class FailingStateUpdateCallback(ControllerCallback):
        def after_controller_state_update(
            self,
            previous_state: TrainControllerState,
            current_state: TrainControllerState,
        ):
            raise ValueError("Intentional error in state update callback")

    scaling_policy = MockScalingPolicy(scaling_config=ScalingConfig())
    failure_policy = MockFailurePolicy(failure_config=None)
    train_run_context = create_dummy_run_context()

    controller = TrainController(
        train_fn_ref=DummyObjectRefWrapper(lambda: None),
        train_run_context=train_run_context,
        scaling_policy=scaling_policy,
        failure_policy=failure_policy,
        callbacks=[FailingStateUpdateCallback()],
    )

    # When the state-update callback raises, the controller should surface the error
    # via the failure policy and transition into shutdown -> errored.
    failure_policy.queue_decision(FailureDecision.RAISE)

    scaling_policy.queue_recovery_decision(
        ResizeDecision(num_workers=2, resources_per_worker={})
    )

    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), ShuttingDownState)
    assert isinstance(controller.get_state().next_state, ErroredState)
    assert isinstance(
        controller.get_state().next_state.training_failed_error, ControllerError
    )

    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), ErroredState)
    assert isinstance(controller.get_state().training_failed_error, ControllerError)


class _MockReplicaGroupBackend(Backend):
    has_replica_groups = True


class _MockReplicaGroupBackendConfig(BackendConfig):
    @property
    def backend_cls(self):
        return _MockReplicaGroupBackend


@pytest.mark.asyncio
async def test_resize_and_fail_with_replica_groups():
    """Test partial replica group replacement vs full restart with has_replica_groups.

    Four scenarios:
    1) Same size + no poll_status → regular full restart path
    2) Same size + poll_status with errors → partial replacement path
    3) Different size + poll_status → regular full restart path
    4) Same size + all replica groups failing → regular full restart path
    """
    scaling_policy = MockScalingPolicy(scaling_config=ScalingConfig())
    failure_policy = MockFailurePolicy(failure_config=None)
    train_run_context = create_dummy_run_context(
        backend_config=_MockReplicaGroupBackendConfig(),
    )
    controller = TrainController(
        train_fn_ref=DummyObjectRefWrapper(lambda: None),
        train_run_context=train_run_context,
        scaling_policy=scaling_policy,
        failure_policy=failure_policy,
    )

    # Start with 4 workers.
    assert isinstance(controller.get_state(), InitializingState)
    scaling_policy.queue_recovery_decision(
        ResizeDecision(num_workers=4, resources_per_worker={})
    )
    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), SchedulingState)
    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), RunningState)

    initial_worker_group = controller.get_worker_group()
    assert initial_worker_group is not None
    assert initial_worker_group.has_started()
    assert len(initial_worker_group.get_workers()) == 4

    # --- Case 1: same size, no poll_status → regular full restart path ---
    controller.get_worker_group().error_worker(1)
    failure_policy.queue_decision(FailureDecision.RETRY)
    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), RestartingState)

    scaling_policy.queue_recovery_decision(
        ResizeDecision(num_workers=4, resources_per_worker={})
    )
    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), SchedulingState)
    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), RunningState)

    worker_group_after_case1 = controller.get_worker_group()
    assert worker_group_after_case1 is not initial_worker_group
    assert worker_group_after_case1.has_started()
    assert len(worker_group_after_case1.get_workers()) == 4

    # --- Case 2: same size, failure poll_status → partial replacement path ---
    poll_status = WorkerGroupPollStatus(
        worker_statuses={
            0: WorkerStatus(running=True),
            1: WorkerStatus(running=False, error=RuntimeError("Worker 1 failed")),
            2: WorkerStatus(running=True),
            3: WorkerStatus(running=True),
        },
        worker_rank_to_replica_group_rank={0: 0, 1: 0, 2: 1, 3: 1},
    )
    controller.get_worker_group().get_latest_poll_status = lambda: poll_status
    controller.get_worker_group().error_worker(1)
    failure_policy.queue_decision(FailureDecision.RETRY)
    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), RestartingState)

    scaling_policy.queue_recovery_decision(
        ResizeDecision(num_workers=4, resources_per_worker={})
    )
    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), SchedulingState)
    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), RunningState)

    worker_group_after_case2 = controller.get_worker_group()
    assert worker_group_after_case2 is worker_group_after_case1
    assert worker_group_after_case2.has_started()
    assert worker_group_after_case2._replaced_replica_groups == [0]

    # Clear the error so the next poll is clean.
    worker_group_after_case2.clear_worker()

    # --- Case 3: different size, failure poll_status → regular full restart path ---
    controller.get_worker_group().error_worker(2)
    failure_policy.queue_decision(FailureDecision.RETRY)
    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), RestartingState)

    scaling_policy.queue_recovery_decision(
        ResizeDecision(num_workers=6, resources_per_worker={})
    )
    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), SchedulingState)
    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), RunningState)

    worker_group_after_case3 = controller.get_worker_group()
    assert worker_group_after_case3 is not worker_group_after_case2
    assert worker_group_after_case3.has_started()
    assert len(worker_group_after_case3.get_workers()) == 6

    # --- Case 4: same size, all replica groups failing → regular full restart path ---
    all_failing_poll_status = WorkerGroupPollStatus(
        worker_statuses={
            0: WorkerStatus(running=False, error=RuntimeError("Worker 0 failed")),
            1: WorkerStatus(running=False, error=RuntimeError("Worker 1 failed")),
            2: WorkerStatus(running=False, error=RuntimeError("Worker 2 failed")),
            3: WorkerStatus(running=False, error=RuntimeError("Worker 3 failed")),
            4: WorkerStatus(running=False, error=RuntimeError("Worker 4 failed")),
            5: WorkerStatus(running=False, error=RuntimeError("Worker 5 failed")),
        },
        worker_rank_to_replica_group_rank={0: 0, 1: 0, 2: 0, 3: 1, 4: 1, 5: 1},
    )
    controller.get_worker_group().get_latest_poll_status = (
        lambda: all_failing_poll_status
    )
    controller.get_worker_group().error_worker(0)
    failure_policy.queue_decision(FailureDecision.RETRY)
    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), RestartingState)

    scaling_policy.queue_recovery_decision(
        ResizeDecision(num_workers=6, resources_per_worker={})
    )
    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), SchedulingState)
    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), RunningState)

    worker_group_after_case4 = controller.get_worker_group()
    assert worker_group_after_case4 is not worker_group_after_case3
    assert worker_group_after_case4.has_started()
    assert len(worker_group_after_case4.get_workers()) == 6


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
