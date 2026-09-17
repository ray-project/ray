"""Why this belongs in Ray Train and not in a node agent.

Each scenario here is run twice: once with the full policy set, and once with
the hardware-only policies a node agent could supply on its own. The
hardware-only run is the control. If it reached the same decision, the case for
putting this loop in the training framework would be weak.
"""
import sys

import pytest

from ray.train.v2._internal.execution.health import (
    Action,
    Cause,
    HealthManager,
    HealthState,
    NodeHealth,
    ProbeResult,
    WorkerHealth,
)
from ray.train.v2._internal.execution.health.adapters.nvsentinel import (
    K8sNodeStatus,
    NodeCondition,
    NVSentinelEvaluator,
    NVSentinelProbe,
)
from ray.train.v2._internal.execution.health.adapters.udf_signals import (
    SdcEvaluator,
    StragglerEvaluator,
    hardware_evidence_against,
    sdc_policy,
    straggler_policy,
)
from ray.train.v2._internal.execution.health.report import report, reset, snapshot

# 6 ranks over 3 nodes. Rank 4 lives on nodeC.
TOPOLOGY = {0: "nodeA", 1: "nodeA", 2: "nodeB", 3: "nodeB", 4: "nodeC", 5: "nodeC"}


def _state(reported, step=100, nodes=None, on_demand=None, steps=None):
    """A HealthState as if every rank had called health.report()."""
    steps = steps or {}
    workers = {
        rank: WorkerHealth(
            worker_rank=rank,
            node_id=node,
            snapshot_at=1.0,
            step=steps.get(rank, step),
            reported=reported.get(rank, {}),
        )
        for rank, node in TOPOLOGY.items()
    }
    return HealthState(
        workers=workers,
        nodes=nodes or {},
        on_demand_probes=on_demand or {},
    )


def _healthy_nvsentinel_nodes():
    """What NVSentinel reports when it sees nothing wrong. Which is the point."""
    clean = NVSentinelProbe._to_result(
        K8sNodeStatus(
            name="n", conditions={"Ready": NodeCondition(type="Ready", status="True")}
        )
    )
    return {
        node: NodeHealth(node, 1.0, {"NVSentinelProbe": clean})
        for node in set(TOPOLOGY.values())
    }


# ----------------------------------------------------------------------
# health.report(): the one line in the training loop
# ----------------------------------------------------------------------
def test_report_accumulates_and_snapshots():
    reset()
    report({"step_time_s": 0.41}, step=10)
    report({"grad_norm": 1.83}, step=11)

    snap = snapshot()
    assert snap.metrics == {"step_time_s": 0.41, "grad_norm": 1.83}
    assert snap.step == 11  # last write wins
    reset()


def test_a_snapshot_does_not_clear_the_accumulator():
    """A rank that stops reporting keeps its last reading.

    Vanishing would look like missing data. A step that stops advancing is a
    signal a policy can act on.
    """
    reset()
    report({"step_time_s": 0.41}, step=10)
    assert snapshot().step == 10
    assert snapshot().step == 10
    reset()


def test_report_rejects_non_dict_metrics():
    with pytest.raises(TypeError):
        report(["step_time_s", 0.41])


# ----------------------------------------------------------------------
# Scenario 1: the degraded-but-alive straggler
# ----------------------------------------------------------------------
def _straggler_history(evaluator, slow_rank=4, ratio=2.4, steps=8):
    """Feed per-step timings: everyone at 0.40s, one rank slower.

    Returns every decision the evaluator made across those steps, so a test can
    assert both what fired and how often.
    """
    seen = []
    for step in range(steps):
        reported = {
            rank: {"step_time_s": 0.40 * (ratio if rank == slow_rank else 1.0)}
            for rank in TOPOLOGY
        }
        seen.extend(evaluator.evaluate(_state(reported, step=step)))
    return seen


def test_hardware_alone_sees_nothing_wrong_with_a_straggler():
    """The control. NCCL RAS says progressing; NVSentinel says the node is fine.

    Thermal throttling is non-fatal in NVSentinel, so it writes a Kubernetes
    Event and never a node condition -- nothing cordons, nothing evicts. A node
    agent has no reason to act, and it is right not to: a throttled GPU is
    usually harmless. It has no way to know this one is costing 2.4x.
    """
    state = _state({rank: {} for rank in TOPOLOGY}, nodes=_healthy_nvsentinel_nodes())
    assert NVSentinelEvaluator().evaluate(state) == []


def test_a_straggler_alone_asks_rather_than_evicts():
    """Slow is ambiguous. It buys a diagnostic, not a quarantine.

    And exactly one, across eight slow steps: a rank that stays slow must not
    re-push py-spy at it on every poll.
    """
    evaluator = StragglerEvaluator(diagnostics=[object()])
    decisions = _straggler_history(evaluator)

    assert [d.action for d in decisions] == [Action.DIAGNOSE]
    assert decisions[0].target_ranks == [4]
    assert decisions[0].target_nodes == ["nodeC"]


def test_a_straggler_over_a_throttling_gpu_is_attributable():
    """The join. Neither number alone gets here.

    The UDF says which rank is slow; the topology says which host; the pushed
    nvidia-smi says that host is throttling. Now it is hardware.
    """
    evaluator = StragglerEvaluator()
    _straggler_history(evaluator)

    throttling = {
        "NvidiaSmiProbe": {
            "nodeC": ProbeResult(
                metrics={"max_temp_c": 88.0},
                events=["thermal_throttle"],
                passed=False,
            )
        }
    }
    reported = {
        rank: {"step_time_s": 0.40 * (2.4 if rank == 4 else 1.0)} for rank in TOPOLOGY
    }
    decisions = evaluator.evaluate(
        _state(
            reported, step=99, nodes=_healthy_nvsentinel_nodes(), on_demand=throttling
        )
    )

    assert len(decisions) == 1
    assert decisions[0].action is Action.EVICT
    assert decisions[0].cause is Cause.HARDWARE
    assert decisions[0].target_nodes == ["nodeC"]
    assert "thermal_throttle" in decisions[0].reason


def test_everyone_slowing_together_is_not_a_node_fault():
    """A stalled data loader must not evict a node.

    This is the false positive a throughput alarm would produce, and the reason
    the check compares ranks to each other rather than to a fixed threshold.
    """
    evaluator = StragglerEvaluator(diagnostics=[object()])
    decisions = []
    for step in range(8):
        reported = {rank: {"step_time_s": 1.6} for rank in TOPOLOGY}
        decisions = evaluator.evaluate(_state(reported, step=step))
    assert decisions == []


def test_a_single_slow_step_is_not_a_straggler():
    evaluator = StragglerEvaluator(min_samples=5)
    for step in range(5):
        reported = {
            rank: {"step_time_s": 2.0 if (rank == 4 and step == 2) else 0.40}
            for rank in TOPOLOGY
        }
        decisions = evaluator.evaluate(_state(reported, step=step))
    assert decisions == []


def test_samples_are_taken_per_step_not_per_poll():
    """The controller polls faster than the loop steps.

    Sampling per poll would fill the window with repeats and make a rank that
    has stopped stepping look perfectly stable.
    """
    evaluator = StragglerEvaluator(min_samples=5)
    reported = {rank: {"step_time_s": 0.40} for rank in TOPOLOGY}
    for _ in range(20):
        evaluator.evaluate(_state(reported, step=7))  # same step, many polls
    assert len(evaluator._history[0]) == 1


# ----------------------------------------------------------------------
# Scenario 2: silent data corruption
# ----------------------------------------------------------------------
def _checksums(bad_rank=None, value=0xABCD, bad_value=0x1234):
    return {
        rank: {"weight_checksum": bad_value if rank == bad_rank else value}
        for rank in TOPOLOGY
    }


def test_hardware_alone_can_never_see_silent_corruption():
    """The control, and the strongest case in the set.

    Silent data corruption is silent *by definition*: no XID, no ECC, clocks
    nominal, every node condition green, and NCCL advancing normally. There is
    no node-level telemetry that could ever fire here. A node agent is not
    missing a rule -- it is missing the numbers, and the numbers only exist
    inside the training loop.
    """
    state = _state(_checksums(bad_rank=4), nodes=_healthy_nvsentinel_nodes())

    # Every hardware source: clean.
    assert NVSentinelEvaluator().evaluate(state) == []
    assert hardware_evidence_against(state, "nodeC") is None

    # The application signal: not clean.
    decisions = SdcEvaluator(confirm_steps=1).evaluate(state)
    assert decisions[0].action is Action.EVICT
    assert decisions[0].target_nodes == ["nodeC"]


def test_sdc_needs_the_divergence_to_repeat():
    evaluator = SdcEvaluator(confirm_steps=2)
    assert evaluator.evaluate(_state(_checksums(bad_rank=4), step=1)) == []
    decisions = evaluator.evaluate(_state(_checksums(bad_rank=4), step=2))
    assert decisions[0].action is Action.EVICT
    assert decisions[0].cause is Cause.HARDWARE


def test_matching_checksums_produce_nothing():
    evaluator = SdcEvaluator(confirm_steps=1)
    assert evaluator.evaluate(_state(_checksums(), step=1)) == []


def test_no_majority_is_a_collective_fault_not_a_rank_fault():
    """Three values, no majority: evicting one node would not fix it."""
    reported = {
        0: {"weight_checksum": 1},
        1: {"weight_checksum": 1},
        2: {"weight_checksum": 2},
        3: {"weight_checksum": 2},
        4: {"weight_checksum": 3},
        5: {"weight_checksum": 3},
    }
    assert SdcEvaluator(confirm_steps=1).evaluate(_state(reported, step=1)) == []


def test_ranks_are_only_compared_at_the_same_step():
    """Ranks drift apart; comparing across steps would invent disagreements."""
    reported = _checksums()
    reported[4] = {"weight_checksum": 0x1234}  # differs, but it is a step behind
    state = _state(reported, step=10, steps={4: 9})

    # Only 5 ranks reached step 10 and they all agree.
    assert SdcEvaluator(confirm_steps=1).evaluate(state) == []


def test_sdc_is_corroborated_when_hardware_does_have_something_to_say():
    evaluator = SdcEvaluator(confirm_steps=1)
    faulty = K8sNodeStatus(
        name="gpu-node-c",
        conditions={"GpuMemWatch": NodeCondition(type="GpuMemWatch", status="True")},
    )
    nodes = _healthy_nvsentinel_nodes()
    nodes["nodeC"] = NodeHealth(
        "nodeC", 1.0, {"NVSentinelProbe": NVSentinelProbe._to_result(faulty)}
    )
    decisions = evaluator.evaluate(_state(_checksums(bad_rank=4), nodes=nodes))
    assert decisions[0].action is Action.EVICT
    assert "GpuMemWatch" in decisions[0].reason


# ----------------------------------------------------------------------
# Composition: the policies are peers in one manager
# ----------------------------------------------------------------------
def test_policies_compose_and_the_most_severe_wins():
    manager = HealthManager([straggler_policy(diagnostics=[object()]), sdc_policy()])
    evaluators = manager._evaluators
    assert len(evaluators) == 2

    seen = []
    for step in range(8):
        reported = {
            rank: {
                "step_time_s": 0.40 * (2.4 if rank == 4 else 1.0),
                "weight_checksum": 0x1234 if rank == 4 else 0xABCD,
            }
            for rank in TOPOLOGY
        }
        for rank, node in TOPOLOGY.items():
            manager.ingest_worker_health(
                WorkerHealth(rank, node, 1.0, step=step, reported=reported[rank])
            )
        decision = manager.poll_decision()
        if decision is not None:
            seen.append(decision)

    # The straggler check wants a diagnostic; SDC wants an eviction. EVICT is
    # more severe, so the run takes the action that covers the worst finding,
    # and takes it exactly once -- nodeC is then already being handled.
    assert [d.action for d in seen] == [Action.EVICT]
    assert seen[0].target_nodes == ["nodeC"]
    assert seen[0].cause is Cause.HARDWARE


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
