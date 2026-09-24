"""The health contracts and the HealthManager, with toy probes."""
import sys
from typing import Dict

import pytest

import ray.train.health as health
from ray.train.health import (
    Action,
    Cause,
    ClusterContext,
    ClusterProbe,
    Diagnose,
    Evaluator,
    Evict,
    HealthConfig,
    HealthPolicy,
    HealthState,
    NodeHealth,
    Noop,
    OnDemandProbe,
    ProbeResult,
    Reattempt,
    WorkerHealth,
)
from ray.train.health._internal.callback import build_node_exclusion_selector
from ray.train.health._internal.manager import HealthManager, merge_decisions


class NodeTemps(ClusterProbe):
    name = "NodeTemps"
    entity = "node"

    def __init__(self, temps: Dict[str, float]):
        self.temps = temps

    def poll(self, ctx):
        return {
            n: ProbeResult(metrics={"temp_c": t}, passed=t < 90)
            for n, t in self.temps.items()
            if not ctx.node_ids or n in ctx.node_ids
        }


class Queues(ClusterProbe):
    name = "Queues"
    entity = "queue"

    def poll(self, ctx):
        return {"q1": ProbeResult(metrics={"depth": 3.0})}


class EvictHot(Evaluator):
    def evaluate(self, state):
        hot = [n for n, r in state.results(NodeTemps).items() if r.passed is False]
        if not hot:
            return []
        return [Evict(cause=Cause.HARDWARE, reason="hot", target_nodes=hot)]


# ----------------------------------------------------------------------
# The public import path
# ----------------------------------------------------------------------
def test_public_api():
    for name in health.__all__:
        assert hasattr(health, name), name
    for internal in ("HealthManager", "OnDemandRunner", "merge_decisions"):
        assert not hasattr(health, internal)


def test_report_outside_a_train_worker_raises():
    with pytest.raises(RuntimeError, match="cannot be used outside"):
        health.report({"loss": 1.0})


def test_run_config_takes_a_health_config():
    from ray.train import RunConfig

    cfg = HealthConfig(policies=[HealthPolicy(evaluator_creator=lambda: [])])
    assert RunConfig(health_config=cfg).health_config is cfg
    assert RunConfig().health_config is None  # off by default


# ----------------------------------------------------------------------
# Merge: one decision per poll
# ----------------------------------------------------------------------
def test_the_most_severe_action_wins():
    merged = merge_decisions(
        [
            Diagnose(reason="slow", target_ranks=[3]),
            Evict(reason="nic", target_nodes=["n1"], cause=Cause.HARDWARE),
            Reattempt(reason="transient"),
        ]
    )
    assert merged.action is Action.EVICT


def test_decisions_merge_at_the_winning_severity():
    merged = merge_decisions(
        [
            Evict(reason="ecc", target_nodes=["n1"], cause=Cause.HARDWARE),
            Evict(reason="nvlink", target_nodes=["n2", "n1"], cause=Cause.NO_PROGRESS),
        ]
    )
    assert merged.target_nodes == ["n1", "n2"]
    assert "ecc" in merged.reason and "nvlink" in merged.reason
    # HARDWARE is what authorizes quarantine downstream, so it must survive.
    assert merged.cause is Cause.HARDWARE


def test_noop_and_empty_produce_nothing():
    assert merge_decisions([]) is None
    assert merge_decisions([Noop(), Noop(reason="fine")]) is None


@pytest.mark.parametrize(
    "decision",
    [
        Evict(target_nodes=["n1"], cause=Cause.HARDWARE),
        Diagnose(target_nodes=["n1"], target_ranks=[4]),
    ],
)
def test_actions_on_an_already_handled_node_are_suppressed(decision):
    assert merge_decisions([decision], handled_nodes=["n1"]) is None


def test_a_diagnose_naming_no_node_is_not_suppressed():
    merged = merge_decisions([Diagnose(target_ranks=[4])], handled_nodes=["n1"])
    assert merged.action is Action.DIAGNOSE


# ----------------------------------------------------------------------
# HealthState
# ----------------------------------------------------------------------
def test_health_state_typed_reads():
    state = HealthState(
        workers={
            0: WorkerHealth(0, "nA", 1.0, step=10, reported={"grad_norm": 1.8}),
            1: WorkerHealth(1, "nB", 1.0, step=10),
        },
        nodes={"nB": NodeHealth("nB", 1.0, {"NodeTemps": ProbeResult(detail="x")})},
        entities={"Queues": {"q1": ProbeResult(detail="y")}},
    )
    assert state.results(NodeTemps)["nB"].detail == "x"
    assert state.results(Queues)["q1"].detail == "y"
    assert state.ranks_on("nB") == [1]
    assert state.node_of(0) == "nA"
    assert state.reported == {0: {"grad_norm": 1.8}}


# ----------------------------------------------------------------------
# HealthManager
# ----------------------------------------------------------------------
def test_policy_creators_run_per_run_not_at_import():
    built = []
    policy = HealthPolicy(evaluator_creator=lambda: built.append(1) or [EvictHot()])
    assert built == []
    HealthManager([policy])
    assert built == [1]


def test_node_keyed_results_land_in_the_node_map():
    manager = HealthManager(
        [HealthPolicy(probe_creator=lambda: [NodeTemps({"n1": 60, "n2": 95})])]
    )
    manager.run_cluster_probes(ClusterContext(node_ids=["n1", "n2"]))
    state = manager.build_state()
    assert set(state.nodes) == {"n1", "n2"}
    assert state.results(NodeTemps)["n2"].passed is False


def test_non_node_entities_never_invent_a_host():
    manager = HealthManager([HealthPolicy(probe_creator=lambda: [Queues()])])
    manager.run_cluster_probes(ClusterContext(node_ids=["n1"]))
    state = manager.build_state()
    assert state.nodes == {}
    assert set(state.results(Queues)) == {"q1"}


def test_node_samples_from_two_sources_merge_rather_than_clobber():
    manager = HealthManager([])
    manager.ingest_node_health(NodeHealth("n1", 1.0, {"A": ProbeResult(detail="a")}))
    manager.ingest_node_health(NodeHealth("n1", 2.0, {"B": ProbeResult(detail="b")}))
    assert set(manager.build_state().nodes["n1"].probe_results) == {"A", "B"}


def test_an_eviction_is_decided_once_and_remembered():
    manager = HealthManager(
        [
            HealthPolicy(
                probe_creator=lambda: [NodeTemps({"n1": 60, "n2": 95})],
                evaluator_creator=lambda: [EvictHot()],
            )
        ]
    )
    manager.run_cluster_probes(ClusterContext(node_ids=["n1", "n2"]))
    decision = manager.poll_decision()
    assert decision.action is Action.EVICT and decision.target_nodes == ["n2"]

    manager.run_cluster_probes(ClusterContext(node_ids=["n1", "n2"]))
    assert manager.poll_decision() is None
    assert manager.evicted_nodes == ["n2"]

    manager.on_worker_group_start()
    assert manager.evicted_nodes == ["n2"]


class Explodes(Evaluator):
    calls = 0

    def evaluate(self, state):
        Explodes.calls += 1
        raise RuntimeError("detector bug")


def test_a_raising_evaluator_is_disabled_not_fatal():
    Explodes.calls = 0
    manager = HealthManager([HealthPolicy(evaluator_creator=lambda: [Explodes()])])
    assert manager.poll_decision() is None
    assert manager.poll_decision() is None
    assert Explodes.calls == 1


class Flaky(ClusterProbe):
    name = "Flaky"
    entity = "node"

    def __init__(self):
        self.calls = 0

    def poll(self, ctx):
        self.calls += 1
        if self.calls == 2:
            raise RuntimeError("transient")
        return {"n1": ProbeResult(metrics={"call": float(self.calls)})}


def test_a_transient_probe_failure_keeps_the_previous_snapshot():
    manager = HealthManager([HealthPolicy(probe_creator=lambda: [Flaky()])])
    manager.run_cluster_probes(ClusterContext(node_ids=["n1"]))
    manager.run_cluster_probes(ClusterContext(node_ids=["n1"]))
    assert manager.build_state().results(Flaky)["n1"].metrics["call"] == 1.0


# ----------------------------------------------------------------------
# Pre-flight
# ----------------------------------------------------------------------
class GpuScreen(OnDemandProbe):
    def poll(self, ctx):
        return ProbeResult(passed=True)


class RunsHot(Evaluator):
    def evaluate(self, state):
        return [
            Evict(reason="hot at idle", target_nodes=[n])
            for n, r in state.on_demand_probe_results(GpuScreen).items()
            if r.metrics.get("temp_c", 0) > 85
        ]


def test_preflight_runs_only_on_demand_probes_of_preflight_policies():
    manager = HealthManager(
        [
            HealthPolicy(probe_creator=lambda: [GpuScreen(), Queues()], preflight=True),
            HealthPolicy(probe_creator=lambda: [GpuScreen()]),
        ]
    )
    assert [type(p) for p in manager.preflight_probes()] == [GpuScreen]


def test_preflight_rejects_failed_checks_and_evaluator_evictions():
    manager = HealthManager(
        [
            HealthPolicy(
                probe_creator=lambda: [GpuScreen()],
                evaluator_creator=lambda: [RunsHot()],
                preflight=True,
            )
        ]
    )
    decision = manager.evaluate_preflight(
        {
            "GpuScreen": {
                "n0": ProbeResult(passed=True, metrics={"temp_c": 60}),
                "n1": ProbeResult(passed=False, detail="ECC"),
                "n2": ProbeResult(passed=True, metrics={"temp_c": 92}),
            }
        }
    )
    assert decision.action is Action.EVICT
    assert decision.target_nodes == ["n1", "n2"]
    assert manager.evicted_nodes == ["n1", "n2"]


def test_preflight_passing_decides_nothing():
    manager = HealthManager(
        [HealthPolicy(probe_creator=lambda: [GpuScreen()], preflight=True)]
    )
    ok = {"GpuScreen": {"n0": ProbeResult(passed=True)}}
    assert manager.evaluate_preflight(ok) is None
    assert manager.evicted_nodes == []


# ----------------------------------------------------------------------
# Eviction plumbing
# ----------------------------------------------------------------------
def test_node_exclusion_selector():
    assert build_node_exclusion_selector([]) is None
    assert build_node_exclusion_selector(["n2", "n1"]) == {
        "ray.io/node-id": "!in(n1,n2)"
    }


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
