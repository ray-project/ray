"""The controller owns what a HealthDecision does.

DIAGNOSE keeps running, REATTEMPT goes through the FailurePolicy and its
``max_failures``, and EVICT restarts through the scaling policy without using
a retry.
"""
import sys

import pytest

import ray
from ray.train.health import (
    Diagnose,
    Evict,
    HealthConfig,
    HealthDecisionError,
    HealthPolicy,
    Reattempt,
)
from ray.train.v2._internal.callbacks.health_callback import HealthCallback
from ray.train.v2._internal.constants import HEALTH_CHECK_INTERVAL_S_ENV_VAR
from ray.train.v2._internal.execution.callback import ControllerCallback
from ray.train.v2._internal.execution.controller import TrainController
from ray.train.v2._internal.execution.controller.state import (
    ErroredState,
    RestartingState,
    RunningState,
    SchedulingState,
    ShuttingDownState,
)
from ray.train.v2._internal.execution.failure_handling.default import (
    DefaultFailurePolicy,
)
from ray.train.v2._internal.execution.scaling_policy import ResizeDecision
from ray.train.v2.api.config import FailureConfig, RunConfig, ScalingConfig
from ray.train.v2.tests.util import (
    DummyObjectRefWrapper,
    DummyWorkerGroup,
    MockScalingPolicy,
    create_dummy_run_context,
)

pytestmark = pytest.mark.usefixtures("mock_runtime_context")


class _CapturingWorkerGroup(DummyWorkerGroup):
    contexts = []

    def __init__(self, train_run_context, worker_group_context, callbacks=None):
        super().__init__(train_run_context, worker_group_context, callbacks)
        _CapturingWorkerGroup.contexts.append(worker_group_context)


@pytest.fixture(autouse=True)
def patch_worker_group(monkeypatch):
    _CapturingWorkerGroup.contexts = []
    monkeypatch.setattr(TrainController, "worker_group_cls", _CapturingWorkerGroup)
    monkeypatch.setenv(HEALTH_CHECK_INTERVAL_S_ENV_VAR, "0")


@pytest.fixture(autouse=True)
def ray_start():
    ray.init()
    yield
    ray.shutdown()


class Recorder(ControllerCallback):
    def __init__(self):
        self.decisions = []

    def after_health_decision(self, health_decision):
        self.decisions.append(health_decision)


@pytest.fixture
def health(monkeypatch):
    """Stubs HealthCallback's capabilities; the controller logic is under test."""
    state = {"queue": [], "diagnosed": [], "preflight": None}

    def poll_decision(self, worker_group_status):
        if not state["queue"]:
            return None
        decision = state["queue"].pop(0)
        if isinstance(decision, Evict):
            self.manager._evicted.update(decision.target_nodes)
        return decision

    def run_preflight(self, resources_per_worker):
        decision, state["preflight"] = state["preflight"], None
        if decision is not None:
            self.manager._evicted.update(decision.target_nodes)
        return decision

    monkeypatch.setattr(HealthCallback, "poll_decision", poll_decision)
    monkeypatch.setattr(HealthCallback, "run_preflight", run_preflight)
    monkeypatch.setattr(
        HealthCallback, "diagnose", lambda self, d: state["diagnosed"].append(d)
    )
    return state


async def _running_controller(max_failures=0):
    run_config = RunConfig(
        name="test",
        health_config=HealthConfig([HealthPolicy(evaluator_creator=lambda: [])]),
    )
    scaling_policy = MockScalingPolicy(scaling_config=ScalingConfig())
    recorder = Recorder()
    controller = TrainController(
        train_fn_ref=DummyObjectRefWrapper(lambda: None),
        train_run_context=create_dummy_run_context(run_config=run_config),
        scaling_policy=scaling_policy,
        failure_policy=DefaultFailurePolicy(FailureConfig(max_failures=max_failures)),
        callbacks=[recorder],
    )
    scaling_policy.queue_recovery_decision(
        ResizeDecision(num_workers=1, resources_per_worker={})
    )
    await controller._run_control_loop_iteration()
    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), RunningState)
    return controller, scaling_policy, recorder


def _selector(worker_group_context):
    selectors = worker_group_context.label_selector or [{}]
    return selectors[0].get("ray.io/node-id")


@pytest.mark.asyncio
async def test_no_health_config_creates_no_health_callback():
    controller = TrainController(
        train_fn_ref=DummyObjectRefWrapper(lambda: None),
        train_run_context=create_dummy_run_context(),
        scaling_policy=MockScalingPolicy(scaling_config=ScalingConfig()),
        failure_policy=DefaultFailurePolicy(FailureConfig()),
    )
    assert controller._health is None


@pytest.mark.asyncio
async def test_diagnose_runs_probes_and_keeps_running(health):
    controller, _, recorder = await _running_controller()
    decision = Diagnose(reason="look")
    health["queue"].append(decision)

    await controller._run_control_loop_iteration()
    assert isinstance(controller.get_state(), RunningState)
    assert health["diagnosed"] == [decision]
    assert recorder.decisions == [decision]


@pytest.mark.parametrize(
    "max_failures, expected", [(0, ShuttingDownState), (1, RestartingState)]
)
@pytest.mark.asyncio
async def test_reattempt_goes_through_the_failure_policy(
    health, max_failures, expected
):
    controller, _, recorder = await _running_controller(max_failures=max_failures)
    health["queue"].append(Reattempt(reason="hang"))

    await controller._run_control_loop_iteration()
    state = controller.get_state()
    assert isinstance(state, expected)
    if isinstance(state, ShuttingDownState):
        assert isinstance(state.next_state, ErroredState)
        error = state.next_state.training_failed_error
        assert isinstance(error, HealthDecisionError)
        assert error.decision.reason == "hang"
    assert [d.reason for d in recorder.decisions] == ["hang"]


@pytest.mark.asyncio
async def test_evict_restarts_without_the_node_and_uses_no_retry(health):
    controller, scaling_policy, recorder = await _running_controller(max_failures=0)

    for bad in ("node-a", "node-b"):
        health["queue"].append(Evict(reason="hot", target_nodes=[bad]))
        await controller._run_control_loop_iteration()
        assert isinstance(controller.get_state(), RestartingState)

        scaling_policy.queue_recovery_decision(
            ResizeDecision(num_workers=1, resources_per_worker={})
        )
        await controller._run_control_loop_iteration()
        assert isinstance(controller.get_state(), SchedulingState)
        await controller._run_control_loop_iteration()
        assert isinstance(controller.get_state(), RunningState)

    # max_failures=0, yet two evictions did not end the run.
    assert _selector(_CapturingWorkerGroup.contexts[-1]) == "!in(node-a,node-b)"
    assert len(recorder.decisions) == 2


@pytest.mark.asyncio
async def test_preflight_rejections_are_excluded_from_the_first_worker_group(
    health,
):
    health["preflight"] = Evict(reason="pre-flight failed", target_nodes=["node-x"])
    controller, _, recorder = await _running_controller()
    assert _selector(_CapturingWorkerGroup.contexts[0]) == "!in(node-x)"
    assert [d.reason for d in recorder.decisions] == ["pre-flight failed"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
