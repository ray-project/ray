"""HealthCallback: collection, and the capabilities the controller calls."""
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
    HealthPolicy,
    OnDemandProbe,
    ProbeResult,
    Reattempt,
    WorkerHealth,
)
from ray.train.health._internal.on_demand import OnDemandRunner
from ray.train.v2._internal.callbacks import health_callback as callback_module
from ray.train.v2._internal.callbacks.health_callback import HealthCallback
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


def _callback(*policies):
    return HealthCallback(HealthConfig(policies=list(policies)))


class Stuck(ClusterProbe):
    name = "Stuck"
    interval_s = 0.01

    def poll(self, ctx):
        return {"c1": ProbeResult(events=["frozen"], passed=False)}


class RetryWhenStuck(Evaluator):
    def evaluate(self, state):
        if any("frozen" in r.events for r in state.results(Stuck).values()):
            return [Reattempt(cause=Cause.NO_PROGRESS, reason="c1 frozen")]
        return []


def _poll_until_decision(callback, timeout_s=3.0):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        decision = callback.poll_decision(_status())
        if decision is not None:
            return decision
        time.sleep(0.02)
    pytest.fail("no decision")


# ----------------------------------------------------------------------
# Collect and decide
# ----------------------------------------------------------------------
def test_a_cluster_probe_on_its_thread_yields_a_decision():
    callback = _callback(
        HealthPolicy(
            probe_creator=lambda: [Stuck()],
            evaluator_creator=lambda: [RetryWhenStuck()],
        )
    )
    callback.after_worker_group_start(_worker_group())
    try:
        decision = _poll_until_decision(callback)
    finally:
        callback.before_worker_group_shutdown(None)
    assert decision.action is Action.REATTEMPT


def test_worker_health_reaches_evaluators():
    seen = []

    class Watch(Evaluator):
        def evaluate(self, state):
            seen.append(dict(state.reported))
            return []

    callback = _callback(HealthPolicy(evaluator_creator=lambda: [Watch()]))
    callback.after_worker_group_start(_worker_group())
    callback.poll_decision(
        _status(
            {
                0: WorkerHealth(0, "node0", 1.0, step=5, reported={"loss": 2.0}),
                1: WorkerHealth(1, "node1", 1.0, step=5, reported={"loss": 2.1}),
            }
        )
    )
    callback.before_worker_group_shutdown(None)
    assert seen[-1] == {0: {"loss": 2.0}, 1: {"loss": 2.1}}


def test_a_broken_evaluator_yields_no_decision():
    class Boom(Evaluator):
        def evaluate(self, state):
            raise RuntimeError("detector bug")

    callback = _callback(HealthPolicy(evaluator_creator=lambda: [Boom()]))
    callback.after_worker_group_start(_worker_group())
    assert callback.poll_decision(_status()) is None
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


def test_diagnose_results_reach_the_next_decision():
    callback = _callback(HealthPolicy(evaluator_creator=lambda: [DiagnoseThenRetry()]))
    callback.after_worker_group_start(_worker_group())

    def direct(probe, contexts):
        return {ctx.entity_id: probe.poll(ctx) for ctx in contexts}

    callback._runner = OnDemandRunner(run_on_node=direct, run_on_worker=direct)

    decision = callback.poll_decision(_status())
    assert decision.action is Action.DIAGNOSE
    callback.diagnose(decision)
    state = callback.manager.build_state()
    assert sorted(state.on_demand_probe_results(Stacks)) == ["0", "1"]

    assert "diagnosed" in callback.poll_decision(_status()).reason
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

    callback = _callback(HealthPolicy(probe_creator=lambda: [Screen()], preflight=True))
    decision = callback.run_preflight({"GPU": 1})
    assert isinstance(decision, Evict) and decision.target_nodes == ["bad"]
    assert callback.on_controller_start_worker_group(
        scaling_config=None, num_workers=1
    ) == {"ray.io/node-id": "!in(bad)"}

    nodes.append("new")
    assert callback.run_preflight({"GPU": 1}) is None
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
