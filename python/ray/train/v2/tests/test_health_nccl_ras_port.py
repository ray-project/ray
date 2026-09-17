"""The NCCL RAS port: does the detector's behavior survive the contracts?

Each test here mirrors a behavior the existing ``NCCLRASCallback`` has, so a
regression in the port is visible as a failing test rather than a quieter
detector in production.
"""
import sys

import pytest

from ray.train.v2._internal.callbacks.nccl_ras import RASReport
from ray.train.v2._internal.execution.health import (
    Action,
    Cause,
    ClusterContext,
    HealthManager,
    HealthState,
    NodeHealth,
    ProbeResult,
    WorkerHealth,
)
from ray.train.v2._internal.execution.health.adapters.nccl_ras_policy import (
    NcclHangEvaluator,
    NcclRasProbe,
    nccl_ras_policy,
)
from ray.train.v2._internal.execution.health.adapters.nvsentinel import (
    K8sNodeStatus,
    NodeCondition,
    NVSentinelProbe,
)


def _report(counts, running=True, ts="t"):
    """A RASReport from ``{comm: {rank: {op: count}}}``."""
    status = {
        comm: {rank: "RUNNING" if running else "ABORT" for rank in ranks}
        for comm, ranks in counts.items()
    }
    return RASReport(ts, counts, status)


class _Queue:
    """Stands in for RASPoller.next_result."""

    def __init__(self, reports):
        self._reports = list(reports)

    def __call__(self):
        return self._reports.pop(0) if self._reports else None


class _Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


# ----------------------------------------------------------------------
# Probe: the reduction from raw op counts to per-communicator signals
# ----------------------------------------------------------------------
_CTX = ClusterContext(node_ids=["nodeA", "nodeB"])


def test_probe_needs_two_samples_before_it_can_report_a_delta():
    probe = NcclRasProbe(_Queue([_report({"c1": {0: {"AllReduce": 5}}})]))
    first = probe.poll(_CTX)["c1"]
    assert first.events == []
    assert first.passed is None  # takes no position without a delta


def test_probe_flags_a_mismatched_communicator_that_made_no_progress():
    stalled = {"c1": {0: {"AllReduce": 10}, 1: {"AllReduce": 7}}}
    probe = NcclRasProbe(_Queue([_report(stalled), _report(stalled)]))
    probe.poll(_CTX)
    result = probe.poll(_CTX)["c1"]
    assert result.events == ["frozen"]
    assert result.passed is False
    assert result.metrics["mismatched"] == 1.0
    assert result.metrics["ops_advanced"] == 0.0
    assert result.metrics["ranks"] == 2.0


def test_the_communicator_is_the_entity():
    """One result per communicator, which is what actually hangs.

    A healthy communicator on the same poll keeps its own verdict rather than
    being averaged into a run-level number.
    """
    counts = {
        "c1": {0: {"AllReduce": 10}, 1: {"AllReduce": 7}},  # stuck
        "c2": {0: {"AllGather": 3}, 1: {"AllGather": 3}},  # fine
    }
    moved = {
        "c1": {0: {"AllReduce": 10}, 1: {"AllReduce": 7}},
        "c2": {0: {"AllGather": 4}, 1: {"AllGather": 4}},
    }
    probe = NcclRasProbe(_Queue([_report(counts), _report(moved)]))
    probe.poll(_CTX)
    results = probe.poll(_CTX)
    assert set(results) == {"c1", "c2"}
    assert results["c1"].passed is False
    assert results["c2"].passed is True


def test_a_mismatched_communicator_that_is_still_advancing_is_not_frozen():
    # Skew alone is not a hang: ranks can be transiently out of step.
    probe = NcclRasProbe(
        _Queue(
            [
                _report({"c1": {0: {"AllReduce": 10}, 1: {"AllReduce": 7}}}),
                _report({"c1": {0: {"AllReduce": 12}, 1: {"AllReduce": 9}}}),
            ]
        )
    )
    probe.poll(_CTX)
    result = probe.poll(_CTX)["c1"]
    assert result.events == []
    assert result.passed is True


def test_an_aborted_communicator_is_not_a_hang():
    # mismatched_comms requires every rank RUNNING; a torn-down comm is not stuck.
    stalled = {"c1": {0: {"AllReduce": 10}, 1: {"AllReduce": 7}}}
    probe = NcclRasProbe(
        _Queue([_report(stalled, running=False), _report(stalled, running=False)])
    )
    probe.poll(_CTX)
    assert probe.poll(_CTX)["c1"].events == []


def test_probe_returns_none_when_the_poll_produced_nothing():
    # A poll that produced no report must leave the previous snapshot alone.
    assert NcclRasProbe(_Queue([])).poll(_CTX) == {}


# ----------------------------------------------------------------------
# Evaluator: confirmation on wall clock, and attribution
# ----------------------------------------------------------------------
def _state_with(comms, workers=((0, "nodeA"), (1, "nodeB")), nodes=None):
    return HealthState(
        workers={r: WorkerHealth(r, n, 1.0) for r, n in workers},
        nodes=nodes or {},
        entities={"NcclRasProbe": comms} if comms else {},
    )


_FROZEN = {"c1": ProbeResult(metrics={"ranks": 2.0}, events=["frozen"], passed=False)}
_HEALTHY = {"c1": ProbeResult(metrics={"ranks": 2.0}, events=[], passed=True)}


def test_a_single_frozen_sample_is_not_a_hang():
    clock = _Clock()
    evaluator = NcclHangEvaluator(confirm_duration_s=600, clock=clock)
    assert evaluator.evaluate(_state_with(_FROZEN)) == []


def test_confirmation_is_wall_clock_not_poll_count():
    """The callback counts polls and assumes each is one interval apart.

    Here the same two samples confirm or not purely on elapsed time, so a
    controller that drains slower or faster than the poller cannot shift when
    the detector fires.
    """
    clock = _Clock()
    evaluator = NcclHangEvaluator(confirm_duration_s=600, clock=clock)
    evaluator.evaluate(_state_with(_FROZEN))

    clock.t = 599
    assert evaluator.evaluate(_state_with(_FROZEN)) == []

    clock.t = 600
    decisions = evaluator.evaluate(_state_with(_FROZEN))
    assert len(decisions) == 1
    assert decisions[0].action is Action.REATTEMPT


def test_a_communicator_that_recovers_resets_its_streak():
    clock = _Clock()
    evaluator = NcclHangEvaluator(confirm_duration_s=600, clock=clock)
    evaluator.evaluate(_state_with(_FROZEN))

    clock.t = 590
    evaluator.evaluate(_state_with(_HEALTHY))  # recovered

    clock.t = 1200  # long past the original deadline
    assert evaluator.evaluate(_state_with(_FROZEN)) == []


def test_an_unattributed_hang_is_a_reattempt_not_an_eviction():
    """A hang alone says nothing about whose fault it is.

    Evicting on a hang alone would let a bug in user code cordon healthy
    hardware, so the bare case must stay NO_PROGRESS.
    """
    clock = _Clock()
    evaluator = NcclHangEvaluator(confirm_duration_s=0, clock=clock)
    decision = evaluator.evaluate(_state_with(_FROZEN))[0]
    assert decision.action is Action.REATTEMPT
    assert decision.cause is Cause.NO_PROGRESS


def test_observe_mode_takes_no_action():
    evaluator = NcclHangEvaluator(
        confirm_duration_s=0, observe_only=True, clock=_Clock()
    )
    assert evaluator.evaluate(_state_with(_FROZEN)) == []


# ----------------------------------------------------------------------
# The join: this is what the callback cannot do today
# ----------------------------------------------------------------------
def _faulty_node_health(node_id):
    status = K8sNodeStatus(
        name="gpu-node-01",
        conditions={
            "GpuNvlinkWatch": NodeCondition(
                type="GpuNvlinkWatch",
                status="True",
                message=(
                    "[DCGM_FR_NVLINK_ERROR] NVLink down "
                    "- RecommendedAction: RESTART_VM"
                ),
            )
        },
    )
    return NodeHealth(
        node_id, 1.0, {"NVSentinelProbe": NVSentinelProbe._to_result(status)}
    )


def test_a_hang_over_faulty_hardware_becomes_an_eviction():
    clock = _Clock()
    evaluator = NcclHangEvaluator(confirm_duration_s=0, clock=clock)
    state = _state_with(_FROZEN, nodes={"nodeB": _faulty_node_health("nodeB")})

    decision = evaluator.evaluate(state)[0]
    assert decision.action is Action.EVICT
    assert decision.cause is Cause.HARDWARE
    assert decision.target_nodes == ["nodeB"]


def test_a_fault_on_a_node_this_run_does_not_use_does_not_attribute():
    clock = _Clock()
    evaluator = NcclHangEvaluator(confirm_duration_s=0, clock=clock)
    state = _state_with(_FROZEN, nodes={"nodeZ": _faulty_node_health("nodeZ")})
    assert evaluator.evaluate(state)[0].action is Action.REATTEMPT


# ----------------------------------------------------------------------
# Wiring
# ----------------------------------------------------------------------
def test_policy_registers_one_cluster_probe():
    manager = HealthManager([nccl_ras_policy(query=_Queue([]))])
    assert [p.probe_name() for p in manager.cluster_probes()] == ["NcclRasProbe"]


def test_non_node_entities_do_not_land_in_the_node_map():
    """RAS keys by communicator, so its results must not be mistaken for hosts.

    Filing "c1" into HealthState.nodes would invent a node that does not exist
    and make it evictable.
    """
    stalled = {"c1": {0: {"AllReduce": 10}, 1: {"AllReduce": 7}}}
    manager = HealthManager(
        [nccl_ras_policy(query=_Queue([_report(stalled), _report(stalled)]))]
    )
    manager.run_cluster_probes(["nodeA", "nodeB"])
    manager.run_cluster_probes(["nodeA", "nodeB"])

    state = manager.build_state()
    assert state.nodes == {}
    assert state.results(NcclRasProbe)["c1"].events == ["frozen"]


def test_one_read_shape_for_both_entity_kinds():
    """An evaluator reads a node-keyed and a communicator-keyed probe the same way."""
    from ray.train.v2._internal.execution.health.adapters.nvsentinel import (
        StaticNodeStatusSource,
        nvsentinel_policy,
    )

    stalled = {"c1": {0: {"AllReduce": 10}, 1: {"AllReduce": 7}}}
    faulty = K8sNodeStatus(
        name="gpu-node-01",
        conditions={
            "GpuNvlinkWatch": NodeCondition(type="GpuNvlinkWatch", status="True")
        },
    )
    nvs = nvsentinel_policy(source=StaticNodeStatusSource({"gpu-node-01": faulty}))
    manager = HealthManager(
        [nccl_ras_policy(query=_Queue([_report(stalled), _report(stalled)])), nvs]
    )
    for p in manager.cluster_probes():
        if isinstance(p, NVSentinelProbe):
            p._resolve = lambda ids: {"nodeB": "gpu-node-01"}

    manager.run_cluster_probes(["nodeA", "nodeB"])
    manager.run_cluster_probes(["nodeA", "nodeB"])
    state = manager.build_state()

    assert set(state.results(NcclRasProbe)) == {"c1"}
    assert set(state.results(NVSentinelProbe)) == {"nodeB"}


def test_a_degraded_probe_is_retired_for_the_run():
    """The callback's `_is_ras_degraded` latch: a missing binary is not retryable."""
    from ray.train.v2._internal.execution.health import ProbeDegraded

    calls = []

    def query():
        calls.append(1)
        raise ProbeDegraded("binary ncclras not found on the worker")

    manager = HealthManager([nccl_ras_policy(query=query)])
    manager.run_cluster_probes(["nodeA"])
    manager.run_cluster_probes(["nodeA"])
    assert len(calls) == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
