"""The RAS + health.report() join, and pre-flight.

The join is the case that answers "why does this belong in Ray Train": RAS
alone cannot name the communicator that froze, and cannot know how long a
stall is abnormal for *this* job. Both answers come from the training loop.
"""
import sys

import pytest

from ray.train.v2._internal.execution.health import (
    Action,
    Cause,
    HealthState,
    NodeHealth,
    PreflightRunner,
    ProbeResult,
    WorkerHealth,
    preflight_policy,
)
from ray.train.v2._internal.execution.health.adapters.collective_join import (
    CollectiveHangEvaluator,
    parallelism_groups,
)
from ray.train.v2._internal.execution.health.adapters.nvsentinel import (
    K8sNodeStatus,
    NodeCondition,
    NVSentinelProbe,
)
from ray.train.v2._internal.execution.health.decision import Evict
from ray.train.v2._internal.execution.health.policy import Evaluator
from ray.train.v2._internal.execution.health.probe import (
    NODE_SCOPE,
    OnDemandProbe,
    WORKER_SCOPE,
)

# 8 ranks: tp=2, pp=2, dp=2. rank = dp*4 + pp*2 + tp
TOPOLOGY = {r: f"node{r // 2}" for r in range(8)}


def coords(rank):
    return {"dp_rank": rank // 4, "pp_rank": (rank // 2) % 2, "tp_rank": rank % 2}


def _state(frozen_comms, step, step_time=2.0, nodes=None, ranks_per_comm=None):
    """A HealthState with a RAS slice and matching health.report() data."""
    ranks_per_comm = ranks_per_comm or {}
    comms = {}
    for comm_id, ranks in ranks_per_comm.items():
        comms[comm_id] = ProbeResult(
            metrics={"ranks": float(len(ranks))},
            devices={str(r): {"ops_advanced": 0.0} for r in ranks},
            events=["frozen"] if comm_id in frozen_comms else [],
            passed=comm_id not in frozen_comms,
        )
    return HealthState(
        workers={
            r: WorkerHealth(
                r,
                node,
                1.0,
                step=step,
                reported={**coords(r), "step": step, "step_time_s": step_time},
            )
            for r, node in TOPOLOGY.items()
        },
        nodes=nodes or {},
        entities={"NcclRasProbe": comms},
    )


class _Clock:
    """Drives the evaluator's monotonic clock."""

    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


@pytest.fixture
def clock(monkeypatch):
    c = _Clock()
    monkeypatch.setattr("time.monotonic", c)
    return c


# ----------------------------------------------------------------------
# Naming the communicator: the part RAS cannot do
# ----------------------------------------------------------------------
def test_parallelism_groups_are_derived_from_reported_coordinates():
    reported = {r: coords(r) for r in range(8)}
    groups = parallelism_groups(reported)

    assert groups[frozenset({0, 1})] == "tp_rank"  # same dp, same pp
    assert groups[frozenset({0, 2})] == "pp_rank"  # same dp, same tp
    assert groups[frozenset({0, 4})] == "dp_rank"  # same pp, same tp


def test_a_job_that_reports_no_coordinates_gets_no_groups():
    """Reporting nothing degrades to RAS-only behavior, not to a crash."""
    assert parallelism_groups({r: {"step": 1} for r in range(8)}) == {}


# ----------------------------------------------------------------------
# The two false positives RAS alone cannot avoid
# ----------------------------------------------------------------------
def test_a_frozen_pipeline_group_while_stepping_is_a_bubble_not_a_hang(clock):
    """PP send/recv idles by design. Calling it a hang kills a healthy run."""
    evaluator = CollectiveHangEvaluator(min_stall_s=10.0)
    ranks = {"pp_comm": [0, 2]}

    for step in range(1, 12):
        clock.t += 2.0  # the job is advancing normally
        decisions = evaluator.evaluate(
            _state({"pp_comm"}, step=step, ranks_per_comm=ranks)
        )
        assert decisions == [], f"fired at step {step}"


def test_the_same_frozen_pipeline_group_does_fire_once_steps_stop(clock):
    """The bubble is only benign while the job is making progress."""
    evaluator = CollectiveHangEvaluator(min_stall_s=10.0, stall_factor=5.0)
    ranks = {"pp_comm": [0, 2]}

    evaluator.evaluate(_state({"pp_comm"}, step=5, ranks_per_comm=ranks))
    clock.t += 120.0  # step stays at 5: nothing is advancing
    decisions = evaluator.evaluate(_state({"pp_comm"}, step=5, ranks_per_comm=ranks))

    assert decisions and decisions[0].action is Action.REATTEMPT
    assert "PP group" in decisions[0].reason


def test_a_frozen_tensor_parallel_group_does_not_wait_for_progress(clock):
    """A TP all-reduce runs every step, so a freeze there is unambiguous."""
    evaluator = CollectiveHangEvaluator(min_stall_s=10.0)
    ranks = {"tp_comm": [0, 1]}

    evaluator.evaluate(_state({"tp_comm"}, step=5, ranks_per_comm=ranks))
    clock.t += 11.0
    decisions = evaluator.evaluate(_state({"tp_comm"}, step=6, ranks_per_comm=ranks))

    assert decisions and decisions[0].action is Action.REATTEMPT
    assert "TP group" in decisions[0].reason


# ----------------------------------------------------------------------
# A threshold in the job's own units
# ----------------------------------------------------------------------
def test_the_stall_threshold_scales_with_the_jobs_step_time(clock):
    """A fixed timeout is wrong for every job but one.

    Same freeze, same elapsed time, different verdicts -- because 60s of
    silence is nothing for a 40s-step job and an eternity for a 2s-step job.
    """
    ranks = {"dp_comm": [0, 4]}

    fast = CollectiveHangEvaluator(min_stall_s=5.0, stall_factor=5.0)
    fast.evaluate(_state({"dp_comm"}, step=5, step_time=2.0, ranks_per_comm=ranks))
    clock.t += 60.0
    assert fast.evaluate(
        _state({"dp_comm"}, step=5, step_time=2.0, ranks_per_comm=ranks)
    ), "a 2s-step job should have confirmed after 60s"

    clock.t = 1000.0
    slow = CollectiveHangEvaluator(min_stall_s=5.0, stall_factor=5.0)
    slow.evaluate(_state({"dp_comm"}, step=5, step_time=40.0, ranks_per_comm=ranks))
    clock.t += 60.0
    assert (
        slow.evaluate(_state({"dp_comm"}, step=5, step_time=40.0, ranks_per_comm=ranks))
        == []
    ), "a 40s-step job should still be within its own noise floor"


def test_a_job_reporting_nothing_falls_back_to_a_wall_clock_ceiling(clock):
    evaluator = CollectiveHangEvaluator(max_stall_s=100.0)
    state = HealthState(
        workers={r: WorkerHealth(r, n, 1.0) for r, n in TOPOLOGY.items()},
        entities={
            "NcclRasProbe": {
                "c1": ProbeResult(devices={"0": {}, "1": {}}, events=["frozen"])
            }
        },
    )
    evaluator.evaluate(state)
    clock.t += 50.0
    assert evaluator.evaluate(state) == []
    clock.t += 60.0
    assert evaluator.evaluate(state)


# ----------------------------------------------------------------------
# Escalation when the hardware agrees
# ----------------------------------------------------------------------
def test_a_confirmed_hang_over_faulty_hardware_evicts(clock):
    faulty = NVSentinelProbe._to_result(
        K8sNodeStatus(
            name="gpu-0",
            conditions={"GpuNvlinkWatch": NodeCondition("GpuNvlinkWatch", "True")},
        )
    )
    nodes = {"node0": NodeHealth("node0", 1.0, {"NVSentinelProbe": faulty})}
    ranks = {"tp_comm": [0, 1]}

    evaluator = CollectiveHangEvaluator(min_stall_s=1.0)
    evaluator.evaluate(_state({"tp_comm"}, step=5, nodes=nodes, ranks_per_comm=ranks))
    clock.t += 30.0
    decision = evaluator.evaluate(
        _state({"tp_comm"}, step=5, nodes=nodes, ranks_per_comm=ranks)
    )[0]

    assert decision.action is Action.EVICT
    assert decision.cause is Cause.HARDWARE
    assert decision.target_nodes == ["node0"]


def test_a_recovered_communicator_clears_its_streak(clock):
    evaluator = CollectiveHangEvaluator(min_stall_s=10.0)
    ranks = {"tp_comm": [0, 1]}

    evaluator.evaluate(_state({"tp_comm"}, step=5, ranks_per_comm=ranks))
    clock.t += 5.0
    evaluator.evaluate(_state(set(), step=6, ranks_per_comm=ranks))  # recovered
    clock.t += 30.0
    assert evaluator.evaluate(_state({"tp_comm"}, step=7, ranks_per_comm=ranks)) == []


# ----------------------------------------------------------------------
# Pre-flight
# ----------------------------------------------------------------------
class _GpuScreen(OnDemandProbe):
    name = "GpuScreen"
    scope = NODE_SCOPE

    def poll(self, ctx):  # pragma: no cover - driven through the runner
        raise NotImplementedError


class _WorkerCheck(OnDemandProbe):
    name = "WorkerCheck"
    scope = WORKER_SCOPE

    def poll(self, ctx):  # pragma: no cover
        raise NotImplementedError


def _runner(verdicts):
    """verdicts: {node_id: passed}"""

    def run_on_node(node_id, probe, ctx):
        ok = verdicts.get(node_id, True)
        return ProbeResult(
            passed=ok, detail="" if ok else "GPU failed its screen", metrics={}
        )

    return PreflightRunner(run_on_node=run_on_node)


def test_preflight_rejects_the_nodes_that_fail_and_keeps_the_rest():
    policy = preflight_policy(probe_creator=lambda: [_GpuScreen()])
    result = _runner({"n2": False}).run([policy], ["n0", "n1", "n2", "n3"])

    assert not result.passed
    assert result.healthy == ["n0", "n1", "n3"]
    assert "GPU failed its screen" in result.rejected["n2"]
    assert result.ran == ["GpuScreen"]


def test_preflight_says_whether_the_run_can_still_start():
    """Excluding nodes is only free if spares were provisioned."""
    policy = preflight_policy(probe_creator=lambda: [_GpuScreen()])
    result = _runner({"n3": False}).run([policy], ["n0", "n1", "n2", "n3"])

    assert result.enough_for(num_workers=3, per_node=1)
    assert not result.enough_for(num_workers=4, per_node=1)


def test_a_check_that_cannot_complete_is_not_a_pass():
    def explodes(node_id, probe, ctx):
        if node_id == "n1":
            raise RuntimeError("nvidia-smi hung")
        return ProbeResult(passed=True)

    result = PreflightRunner(run_on_node=explodes).run(
        [preflight_policy(probe_creator=lambda: [_GpuScreen()])], ["n0", "n1"]
    )
    # Before training starts, refusing a node costs almost nothing; accepting a
    # bad one costs the run.
    assert result.healthy == ["n0"]
    assert "nvidia-smi hung" in result.rejected["n1"]


def test_worker_scoped_probes_are_skipped_because_no_worker_exists_yet():
    policy = preflight_policy(probe_creator=lambda: [_WorkerCheck()])
    result = _runner({}).run([policy], ["n0"])
    assert result.ran == []
    assert result.passed


def test_an_evaluator_can_reject_a_node_the_probes_let_through():
    """A probe reports; an evaluator decides. Thermal outliers are the case."""

    class _Outlier(Evaluator):
        def evaluate(self, state):
            hot = [
                n
                for n, health in state.nodes.items()
                if health.probe_results["GpuScreen"].metrics.get("temp", 0) > 85
            ]
            return [Evict(reason="runs hot at idle", target_nodes=hot)] if hot else []

    def run_on_node(node_id, probe, ctx):
        temp = 92.0 if node_id == "n1" else 60.0
        return ProbeResult(passed=True, metrics={"temp": temp})

    result = PreflightRunner(run_on_node=run_on_node).run(
        [
            preflight_policy(
                probe_creator=lambda: [_GpuScreen()],
                evaluator_creator=lambda: [_Outlier()],
            )
        ],
        ["n0", "n1"],
    )
    assert result.healthy == ["n0"]
    assert "runs hot at idle" in result.rejected["n1"]


def test_policies_without_preflight_do_not_run():
    from ray.train.v2._internal.execution.health.policy import HealthPolicy

    normal = HealthPolicy(probe_creator=lambda: [_GpuScreen()])  # preflight=False
    result = _runner({"n0": False}).run([normal], ["n0"])
    assert result.passed and result.ran == []


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
