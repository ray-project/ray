"""HealthCallback driven through the controller's hooks with a fake worker group."""
import sys
import threading
import time
from types import SimpleNamespace

import pytest

from ray.train.health import (
    WORKER_SCOPE,
    Action,
    Cause,
    ClusterProbe,
    Diagnose,
    Evaluator,
    Evict,
    HealthConfig,
    HealthDecisionError,
    HealthPolicy,
    OnDemandProbe,
    ProbeResult,
    Reattempt,
    WorkerHealth,
)
from ray.train.health._internal import callback as callback_module
from ray.train.health._internal.callback import HealthCallback
from ray.train.health._internal.on_demand import OnDemandRunner
from ray.train.v2._internal.execution.context import TrainContext
from ray.train.v2._internal.execution.worker_group.poll import (
    WorkerGroupPollStatus,
    WorkerStatus,
)


def _worker_group(n=2):
    workers = [
        SimpleNamespace(
            distributed_context=SimpleNamespace(world_rank=r),
            metadata=SimpleNamespace(node_id=f"node{r}"),
        )
        for r in range(n)
    ]
    return SimpleNamespace(
        get_workers=lambda: workers,
        _storage_context=SimpleNamespace(
            storage_filesystem=None, experiment_fs_path="/tmp/exp"
        ),
    )


def _status(health_by_rank=None):
    health_by_rank = health_by_rank or {}
    return WorkerGroupPollStatus(
        worker_statuses={
            r: WorkerStatus(running=True, health=h) for r, h in health_by_rank.items()
        }
        or {0: WorkerStatus(running=True)}
    )


class Stuck(ClusterProbe):
    name = "Stuck"
    entity = "comm"
    interval_s = 0.01

    def poll(self, ctx):
        return {"c1": ProbeResult(events=["frozen"], passed=False)}


class RetryWhenStuck(Evaluator):
    def evaluate(self, state):
        if any("frozen" in r.events for r in state.results(Stuck).values()):
            return [Reattempt(cause=Cause.NO_PROGRESS, reason="c1 frozen")]
        return []


class Recorder:
    def __init__(self):
        self.decisions = []

    def after_health_decision(self, run_context, health_decision):
        self.decisions.append(health_decision)


def _poll_until_raises(callback, timeout_s=3.0):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        callback.after_worker_group_poll_status(_status())
        time.sleep(0.02)
    pytest.fail("no decision reached the controller")


# ----------------------------------------------------------------------
# Collect -> Decide -> Act, end to end
# ----------------------------------------------------------------------
def test_a_cluster_probe_decision_reaches_the_failure_path():
    recorder = Recorder()
    callback = HealthCallback(
        HealthConfig(
            policies=[
                HealthPolicy(
                    probe_creator=lambda: [Stuck()],
                    evaluator_creator=lambda: [RetryWhenStuck()],
                )
            ]
        ),
        user_callbacks=[recorder],
    )
    callback.after_worker_group_start(_worker_group())
    try:
        with pytest.raises(HealthDecisionError) as info:
            _poll_until_raises(callback)
    finally:
        callback.before_worker_group_shutdown(None)

    assert info.value.decision.action is Action.REATTEMPT
    assert [d.action for d in recorder.decisions] == [Action.REATTEMPT]


def test_worker_health_reaches_evaluators():
    seen = []

    class Watch(Evaluator):
        def evaluate(self, state):
            seen.append(dict(state.reported))
            return []

    callback = HealthCallback(
        HealthConfig(policies=[HealthPolicy(evaluator_creator=lambda: [Watch()])])
    )
    callback.after_worker_group_start(_worker_group())
    callback.after_worker_group_poll_status(
        _status(
            {
                0: WorkerHealth(0, "node0", 1.0, step=5, reported={"loss": 2.0}),
                1: WorkerHealth(1, "node1", 1.0, step=5, reported={"loss": 2.1}),
            }
        )
    )
    callback.before_worker_group_shutdown(None)
    assert seen[-1] == {0: {"loss": 2.0}, 1: {"loss": 2.1}}


def test_a_broken_evaluator_never_fails_the_run():
    class Boom(Evaluator):
        def evaluate(self, state):
            raise RuntimeError("detector bug")

    callback = HealthCallback(
        HealthConfig(policies=[HealthPolicy(evaluator_creator=lambda: [Boom()])])
    )
    callback.after_worker_group_start(_worker_group())
    callback.after_worker_group_poll_status(_status())
    callback.before_worker_group_shutdown(None)


def test_a_raising_user_hook_does_not_stop_the_action():
    class BadHook:
        def after_health_decision(self, run_context, health_decision):
            raise RuntimeError("webhook down")

    callback = HealthCallback(
        HealthConfig(
            policies=[
                HealthPolicy(
                    probe_creator=lambda: [Stuck()],
                    evaluator_creator=lambda: [RetryWhenStuck()],
                )
            ]
        ),
        user_callbacks=[BadHook()],
    )
    callback.after_worker_group_start(_worker_group())
    try:
        with pytest.raises(HealthDecisionError):
            _poll_until_raises(callback)
    finally:
        callback.before_worker_group_shutdown(None)


# ----------------------------------------------------------------------
# DIAGNOSE: results are in the next HealthState
# ----------------------------------------------------------------------
class Stacks(OnDemandProbe):
    name = "Stacks"
    scope = WORKER_SCOPE

    def poll(self, ctx):
        return ProbeResult(detail=f"rank {ctx.rank} stack", passed=True)


class DiagnoseThenRetry(Evaluator):
    def evaluate(self, state):
        if state.on_demand_probe_results(Stacks):
            return [Reattempt(reason="diagnosed; software")]
        return [Diagnose(reason="look first", on_demand_probes=[Stacks()])]


def test_diagnose_pushes_probes_and_the_next_poll_reads_them():
    callback = HealthCallback(
        HealthConfig(
            policies=[HealthPolicy(evaluator_creator=lambda: [DiagnoseThenRetry()])]
        )
    )
    callback.after_worker_group_start(_worker_group())

    def direct(probe, contexts):
        return {ctx.entity_id: probe.poll(ctx) for ctx in contexts}

    callback._runner = OnDemandRunner(run_on_node=direct, run_on_worker=direct)

    callback.after_worker_group_poll_status(_status())
    state = callback.manager.build_state()
    assert sorted(state.on_demand_probe_results(Stacks)) == ["0", "1"]

    with pytest.raises(HealthDecisionError) as info:
        callback.after_worker_group_poll_status(_status())
    assert "diagnosed" in info.value.decision.reason
    callback.before_worker_group_shutdown(None)


# ----------------------------------------------------------------------
# Pre-flight
# ----------------------------------------------------------------------
class Screen(OnDemandProbe):
    def poll(self, ctx):
        return ProbeResult(passed=ctx.node_id != "bad")


def test_preflight_evicts_failing_nodes_and_screens_each_node_once(monkeypatch):
    screened = []

    def run_on_nodes(probe, contexts):
        screened.extend(ctx.node_id for ctx in contexts)
        return {ctx.entity_id: probe.poll(ctx) for ctx in contexts}

    nodes = ["good", "bad"]
    monkeypatch.setattr(callback_module, "_candidate_nodes", lambda _: list(nodes))
    monkeypatch.setattr(callback_module, "_run_on_nodes", run_on_nodes)

    recorder = Recorder()
    callback = HealthCallback(
        HealthConfig(
            policies=[HealthPolicy(probe_creator=lambda: [Screen()], preflight=True)]
        ),
        user_callbacks=[recorder],
    )
    scaling_config = SimpleNamespace(_resources_per_worker_not_none={"GPU": 1})

    selector = callback.on_controller_start_worker_group(
        scaling_config=scaling_config, num_workers=1
    )
    assert selector == {"ray.io/node-id": "!in(bad)"}
    assert isinstance(recorder.decisions[0], Evict)

    nodes.append("new")
    callback.on_controller_start_worker_group(
        scaling_config=scaling_config, num_workers=1
    )
    assert screened == ["good", "bad", "new"]


# ----------------------------------------------------------------------
# The worker side of health.report()
# ----------------------------------------------------------------------
def test_the_worker_reports_nothing_until_the_loop_does(monkeypatch):
    from ray.train.v2._internal.execution.worker_group.worker import RayTrainWorker

    monkeypatch.setattr(
        "ray.get_runtime_context",
        lambda: SimpleNamespace(get_node_id=lambda: "nodeX"),
    )
    ctx = SimpleNamespace(
        distributed_context=SimpleNamespace(world_rank=3),
        health_metrics={},
        health_step=None,
        health_reported_at=None,
        health_lock=threading.Lock(),
    )
    assert RayTrainWorker._get_worker_health(ctx) is None

    TrainContext.report_health(ctx, {"step_time_s": 0.4}, step=12)
    TrainContext.report_health(ctx, {"grad_norm": 1.0})
    snap = RayTrainWorker._get_worker_health(ctx)
    assert snap.worker_rank == 3 and snap.node_id == "nodeX" and snap.step == 12
    assert snap.reported == {"step_time_s": 0.4, "grad_norm": 1.0}


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
