from unittest.mock import create_autospec

import pytest

import ray
from ray.train.v2._internal.constants import HEALTH_CHECK_INTERVAL_S_ENV_VAR
from ray.train.v2._internal.exceptions import (
    WorkerGroupStartupFailedError,
    WorkerGroupStartupTimeoutError,
)
from ray.train.v2._internal.execution.controller import TrainController
from ray.train.v2._internal.execution.controller.state import (
    AbortedState,
    ErroredState,
    FinishedState,
    InitializingState,
    ReschedulingState,
    ResizingState,
    RestartingState,
    RunningState,
    SchedulingState,
    ShuttingDownState,
)
from ray.train.v2._internal.execution.failure_handling import FailureDecision
from ray.train.v2._internal.execution.scaling_policy import (
    NoopDecision,
    ResizeDecision,
)
from ray.train.v2._internal.execution.worker_group import WorkerGroupPollStatus
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

        scaling_policy.queue_monitor_decision(decision)

        if isinstance(decision, NoopDecision):
            await controller._run_control_loop_iteration()
            assert isinstance(controller.get_state(), RunningState)

            worker_group = controller.get_worker_group()
            assert worker_group is not None
            assert worker_group.has_started()
            num_workers = len(worker_group.get_workers())
            assert num_workers == prev_num_workers
        else:
            await controller._run_control_loop_iteration()
            assert isinstance(controller.get_state(), ResizingState)
            await controller._run_control_loop_iteration()
            assert isinstance(controller.get_state(), SchedulingState)
            await controller._run_control_loop_iteration()
            assert isinstance(controller.get_state(), RunningState)

            worker_group = controller.get_worker_group()
            assert worker_group is not None
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
async def test_shutdown_failure_on_finished_path():
    """Shutdown failure on the finished path transitions to ErroredState."""

    def failing_shutdown():
        raise RuntimeError("Simulated shutdown failure")

    scaling_policy = MockScalingPolicy(scaling_config=ScalingConfig())
    failure_policy = MockFailurePolicy(failure_config=None)
    controller = TrainController(
        train_fn_ref=DummyObjectRefWrapper(lambda: None),
        train_run_context=create_dummy_run_context(),
        scaling_policy=scaling_policy,
        failure_policy=failure_policy,
    )
    scaling_policy.queue_recovery_decision(
        ResizeDecision(num_workers=2, resources_per_worker={})
    )
    await controller._run_control_loop_iteration()  # Init -> Scheduling
    await controller._run_control_loop_iteration()  # Scheduling -> Running

    for i in range(2):
        controller.get_worker_group().finish_worker(i)
    await controller._run_control_loop_iteration()  # Running -> ShuttingDown(Finished)
    assert isinstance(controller.get_state().next_state, FinishedState)

    controller.get_worker_group().shutdown = failing_shutdown
    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), ErroredState)
    assert isinstance(controller.get_state().training_failed_error, ControllerError)


@pytest.mark.asyncio
async def test_shutdown_failure_on_errored_path():
    """Shutdown failure on the errored path preserves the original training error."""

    def failing_shutdown():
        raise RuntimeError("Simulated shutdown failure")

    scaling_policy = MockScalingPolicy(scaling_config=ScalingConfig())
    failure_policy = MockFailurePolicy(failure_config=None)
    controller = TrainController(
        train_fn_ref=DummyObjectRefWrapper(lambda: None),
        train_run_context=create_dummy_run_context(),
        scaling_policy=scaling_policy,
        failure_policy=failure_policy,
    )
    scaling_policy.queue_recovery_decision(
        ResizeDecision(num_workers=2, resources_per_worker={})
    )
    await controller._run_control_loop_iteration()  # Init -> Scheduling
    await controller._run_control_loop_iteration()  # Scheduling -> Running

    controller.get_worker_group().error_worker(0)
    failure_policy.queue_decision(FailureDecision.RAISE)
    await controller._run_control_loop_iteration()  # Running -> ShuttingDown(Errored)
    original_error = controller.get_state().next_state.training_failed_error

    controller.get_worker_group().shutdown = failing_shutdown
    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), ErroredState)
    assert controller.get_state().training_failed_error is original_error


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
