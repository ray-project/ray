"""Tests for controller callback behaviour.

Verifies that controller callback hooks are invoked at the correct lifecycle
points, and that callback exceptions are properly caught, routed through the
failure decision pipeline, and do not crash the control loop.
"""

import pytest

import ray
from ray.train.v2._internal.constants import HEALTH_CHECK_INTERVAL_S_ENV_VAR
from ray.train.v2._internal.execution.callback import ControllerCallback
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2._internal.execution.controller import TrainController
from ray.train.v2._internal.execution.controller.state import (
    ErroredState,
    FinishedState,
    InitializingState,
    RunningState,
    SchedulingState,
    ShuttingDownState,
    TrainControllerState,
)
from ray.train.v2._internal.execution.failure_handling import FailureDecision
from ray.train.v2._internal.execution.scaling_policy import ResizeDecision
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
    monkeypatch.setenv(HEALTH_CHECK_INTERVAL_S_ENV_VAR, "0")
    yield
    DummyWorkerGroup.set_poll_failure(None)
    DummyWorkerGroup.set_start_failure(None)


@pytest.fixture(autouse=True)
def ray_start():
    ray.init()
    yield
    ray.shutdown()


async def _create_controller_and_drive_to_running(callbacks=None, num_workers=2):
    """Create a controller and drive it to RunningState."""
    scaling_policy = MockScalingPolicy(scaling_config=ScalingConfig())
    failure_policy = MockFailurePolicy(failure_config=None)
    controller = TrainController(
        train_fn_ref=DummyObjectRefWrapper(lambda: None),
        train_run_context=create_dummy_run_context(),
        scaling_policy=scaling_policy,
        failure_policy=failure_policy,
        callbacks=callbacks or [],
    )
    scaling_policy.queue_recovery_decision(
        ResizeDecision(num_workers=num_workers, resources_per_worker={})
    )
    await controller._run_control_loop_iteration()  # Init -> Scheduling
    await controller._run_control_loop_iteration()  # Scheduling -> Running
    assert isinstance(controller.get_state(), RunningState)
    return controller, scaling_policy, failure_policy


# ---------------------------------------------------------------------------
# Happy-path: verify hooks are invoked
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_controller_callback_hooks_are_invoked():
    """Check that all controller callback hooks are called at the right time."""

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

    controller = TrainController(
        train_fn_ref=DummyObjectRefWrapper(lambda: None),
        train_run_context=create_dummy_run_context(),
        scaling_policy=scaling_policy,
        failure_policy=failure_policy,
        callbacks=[callback],
    )

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


# ---------------------------------------------------------------------------
# Error handling: callback failures during lifecycle hooks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_callback_error_during_state_update():
    """A callback error in after_controller_state_update should not crash the
    control loop; it should transition into shutdown → errored. Callback
    failures during terminal state transitions must not loop indefinitely."""

    class FailingStateUpdateCallback(ControllerCallback):
        def after_controller_state_update(
            self,
            previous_state: TrainControllerState,
            current_state: TrainControllerState,
        ):
            raise ValueError("Intentional error in state update callback")

    scaling_policy = MockScalingPolicy(scaling_config=ScalingConfig())
    failure_policy = MockFailurePolicy(failure_config=None)

    controller = TrainController(
        train_fn_ref=DummyObjectRefWrapper(lambda: None),
        train_run_context=create_dummy_run_context(),
        scaling_policy=scaling_policy,
        failure_policy=failure_policy,
        callbacks=[FailingStateUpdateCallback()],
    )

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


@pytest.mark.asyncio
async def test_callback_error_during_start():
    """A callback error in after_controller_start routes through the failure
    policy and transitions into ShuttingDownState(ErroredState)."""

    class FailingStartCallback(ControllerCallback):
        def after_controller_start(self, train_run_context):
            raise ValueError("Intentional error in start callback")

    scaling_policy = MockScalingPolicy(scaling_config=ScalingConfig())
    failure_policy = MockFailurePolicy(failure_config=None)
    failure_policy.queue_decision(FailureDecision.RAISE)

    controller = TrainController(
        train_fn_ref=DummyObjectRefWrapper(lambda: None),
        train_run_context=create_dummy_run_context(),
        scaling_policy=scaling_policy,
        failure_policy=failure_policy,
        callbacks=[FailingStartCallback()],
    )

    # _start() failed during __init__.
    assert isinstance(controller.get_state(), ShuttingDownState)
    assert isinstance(controller.get_state().next_state, ErroredState)
    assert isinstance(
        controller.get_state().next_state.training_failed_error, ControllerError
    )

    # Complete shutdown → terminal ErroredState.
    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), ErroredState)
    assert isinstance(controller.get_state().training_failed_error, ControllerError)


@pytest.mark.asyncio
async def test_callback_error_during_finish_is_suppressed():
    """A callback error in after_controller_finish is caught and suppressed.
    The controller's terminal state must be preserved."""

    class FailingFinishCallback(ControllerCallback):
        def after_controller_finish(self, result):
            raise ValueError("Intentional error in finish callback")

    controller, _, _ = await _create_controller_and_drive_to_running(
        callbacks=[FailingFinishCallback()]
    )

    # Finish all workers → ShuttingDown(Finished) → Finished.
    for i in range(2):
        controller.get_worker_group().finish_worker(i)
    await controller._run_control_loop_iteration()  # Running -> ShuttingDown(Finished)
    await controller._run_control_loop_iteration()  # ShuttingDown -> Finished
    assert isinstance(controller.get_state(), FinishedState)

    # Simulate what run() does after the loop: invoke after_controller_finish.
    result = controller._build_result()
    failure_result = controller._run_controller_hook("after_controller_finish", result)

    # Callback error produces a failure result, but state stays FinishedState.
    assert failure_result is not None
    assert isinstance(controller.get_state(), FinishedState)


@pytest.mark.asyncio
async def test_callback_error_during_shutdown_hook_on_finished_path():
    """A before_controller_shutdown callback failure on the finished path
    transitions to ErroredState with a ControllerError."""

    class FailingShutdownHookCallback(ControllerCallback):
        def before_controller_shutdown(self):
            raise ValueError("Intentional error in shutdown callback")

    controller, _, _ = await _create_controller_and_drive_to_running(
        callbacks=[FailingShutdownHookCallback()]
    )
    for i in range(2):
        controller.get_worker_group().finish_worker(i)
    await controller._run_control_loop_iteration()  # Running -> ShuttingDown(Finished)
    assert isinstance(controller.get_state().next_state, FinishedState)

    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), ErroredState)
    assert isinstance(controller.get_state().training_failed_error, ControllerError)


@pytest.mark.asyncio
async def test_callback_error_during_shutdown_hook_on_errored_path():
    """A before_controller_shutdown callback failure on the errored path
    preserves the original training error."""

    class FailingShutdownHookCallback(ControllerCallback):
        def before_controller_shutdown(self):
            raise ValueError("Intentional error in shutdown callback")

    controller, _, failure_policy = await _create_controller_and_drive_to_running(
        callbacks=[FailingShutdownHookCallback()]
    )
    controller.get_worker_group().error_worker(0)
    failure_policy.queue_decision(FailureDecision.RAISE)
    await controller._run_control_loop_iteration()  # Running -> ShuttingDown(Errored)
    original_error = controller.get_state().next_state.training_failed_error

    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), ErroredState)
    assert controller.get_state().training_failed_error is original_error


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
