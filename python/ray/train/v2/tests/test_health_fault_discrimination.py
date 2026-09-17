"""Same symptom, different cause, different right answer.

Every fault here is injected in pure software -- a number the training loop
reports -- so all of it runs in CI with no GPU and no special hardware. That is
the point: a detector consumes the *symptom*, and the symptom is a number.

The cases are chosen from what actually happens. In a 55-day study of a 504-GPU
pre-training run, 35.3% of incidents were performance degradation with no error
code at all, and pre-XID detection from hardware metrics succeeded in only 2 of
10 hardware failures. Separately, 42.5% of production LLM jobs are affected by
stragglers, wasting 10.4% of GPU hours -- with the dominant causes being
workload imbalance rather than hardware. Acting on "slow" without knowing why
is how you evict healthy nodes at scale.
"""
import sys

import pytest

from ray.train.v2._internal.execution.health import (
    Action,
    Cause,
    HealthState,
    NodeHealth,
    ProbeResult,
    WorkerHealth,
)
from ray.train.v2._internal.execution.health.adapters.nvsentinel import (
    K8sNodeStatus,
    NodeCondition,
    NVSentinelProbe,
)
from ray.train.v2._internal.execution.health.adapters.udf_signals import (
    NumericalEvaluator,
    StragglerEvaluator,
)

NOMINAL = 0.40
TOPOLOGY = {0: "nodeA", 1: "nodeA", 2: "nodeB", 3: "nodeB", 4: "nodeC", 5: "nodeC"}


def _state(reported, step, nodes=None, on_demand=None):
    return HealthState(
        workers={
            rank: WorkerHealth(
                rank, node, 1.0, step=step, reported=reported.get(rank, {})
            )
            for rank, node in TOPOLOGY.items()
        },
        nodes=nodes or {},
        on_demand_probes=on_demand or {},
    )


def _drive(evaluator, per_step, steps=12, **state_kwargs):
    """Feed ``per_step(step) -> {rank: metrics}`` and collect every decision."""
    seen = []
    for step in range(steps):
        seen.extend(
            evaluator.evaluate(_state(per_step(step), step=step, **state_kwargs))
        )
    return seen


def _throttling_nodec():
    return {
        "NvidiaSmiProbe": {
            "nodeC": ProbeResult(
                metrics={"max_temp_c": 88.0},
                events=["thermal_throttle"],
                passed=False,
            )
        }
    }


# ======================================================================
# The straggler, four ways. Same symptom every time: one rank is slow.
# ======================================================================
def test_cause_1_a_degrading_gpu_is_the_only_one_that_evicts():
    """Same rank slow every window, excess in compute, host degrading."""

    def per_step(step):
        return {
            rank: {
                "step_time_s": NOMINAL * (2.4 if rank == 4 else 1.0),
                "compute_time_s": NOMINAL * (2.3 if rank == 4 else 0.9),
            }
            for rank in TOPOLOGY
        }

    evaluator = StragglerEvaluator()
    decisions = _drive(evaluator, per_step, on_demand=_throttling_nodec())

    evictions = [d for d in decisions if d.action is Action.EVICT]
    assert evictions, "a pinned straggler on a degrading host must evict"
    assert evictions[0].cause is Cause.HARDWARE
    assert evictions[0].target_nodes == ["nodeC"]


def test_cause_2_host_jitter_moves_between_ranks_and_must_not_evict():
    """A GC pause or noisy neighbour makes a *different* rank slow each time.

    This is the most common false positive and the cheapest to rule out: real
    degradation stays put.
    """

    def per_step(step):
        slow_rank = step % 6  # the straggler wanders
        return {
            rank: {"step_time_s": NOMINAL * (2.4 if rank == slow_rank else 1.0)}
            for rank in TOPOLOGY
        }

    decisions = _drive(
        StragglerEvaluator(diagnostics=[object()]),
        per_step,
        on_demand=_throttling_nodec(),
    )
    assert decisions == [], "a wandering straggler is jitter, not a bad node"


def test_cause_3_pipeline_stage_skew_must_not_evict():
    """Every slow rank sits in the same stage: the schedule is imbalanced.

    Evicting one of its nodes moves the bubble; it does not remove it.
    """

    def per_step(step):
        # ranks 4 and 5 are both stage 3, and both slow.
        return {
            rank: {
                "step_time_s": NOMINAL * (1.9 if rank in (4, 5) else 1.0),
                "pipeline_stage": 3 if rank in (4, 5) else rank // 2,
            }
            for rank in TOPOLOGY
        }

    decisions = _drive(
        StragglerEvaluator(diagnostics=[object()]),
        per_step,
        on_demand=_throttling_nodec(),
    )
    assert decisions == []


def test_cause_4_a_slow_data_shard_is_not_the_gpus_fault():
    """The rank is slow, but the excess is outside compute.

    Its GPU is fine; it is being starved. Evicting the host would move the
    same shard to a different host and reproduce the problem there.
    """

    def per_step(step):
        return {
            rank: {
                "step_time_s": NOMINAL * (2.4 if rank == 4 else 1.0),
                # compute is normal; the extra second is data loading
                "compute_time_s": NOMINAL * 0.9,
            }
            for rank in TOPOLOGY
        }

    decisions = _drive(
        StragglerEvaluator(diagnostics=[object()]),
        per_step,
        on_demand=_throttling_nodec(),
    )
    assert decisions == []


def test_the_discriminators_need_the_training_loop():
    """None of the four are separable from outside the process.

    From a node agent all four look the same: a host at nominal utilization
    with no error code. The rank identity, the phase split and the stage map
    are application facts.
    """
    evaluator = StragglerEvaluator()
    assert evaluator._stage_metric == "pipeline_stage"
    assert evaluator._compute_metric == "compute_time_s"


# ======================================================================
# The numbers go wrong, two ways.
# ======================================================================
def test_one_rank_always_anomalous_is_attributable():
    """The observable signature of silent corruption.

    Peers on the same batch are fine, so the data is fine. Something under
    this rank is computing differently.
    """

    def per_step(step):
        return {rank: {"grad_norm": 240.0 if rank == 4 else 1.8} for rank in TOPOLOGY}

    decisions = _drive(NumericalEvaluator(confirm_steps=3), per_step)
    assert decisions[0].action is Action.EVICT
    assert decisions[0].cause is Cause.HARDWARE
    assert decisions[0].target_nodes == ["nodeC"]


def test_a_nan_on_one_rank_counts_as_anomalous():
    def per_step(step):
        return {
            rank: {"grad_norm": float("nan") if rank == 4 else 1.8} for rank in TOPOLOGY
        }

    decisions = _drive(NumericalEvaluator(confirm_steps=2), per_step)
    assert decisions[0].action is Action.EVICT


def test_every_rank_spiking_on_one_step_is_data_not_hardware():
    """A bad batch or a learning-rate event. Evicting a node would hide it."""

    def per_step(step):
        spike = step == 5
        return {rank: {"grad_norm": 900.0 if spike else 1.8} for rank in TOPOLOGY}

    decisions = _drive(NumericalEvaluator(confirm_steps=1), per_step)
    assert len(decisions) == 1
    assert decisions[0].action is Action.REATTEMPT
    assert decisions[0].cause is Cause.APPLICATION


def test_a_global_spike_needs_the_temporal_axis_to_be_visible():
    """When every rank spikes, the peer median spikes with them.

    A purely peer-relative check sees nothing wrong, because nobody stands out.
    Only a comparison against the cluster's own recent history catches it --
    which is why the evaluator keeps both axes.
    """
    evaluator = NumericalEvaluator(confirm_steps=1)

    def per_step(step):
        spike = step >= 8
        return {rank: {"grad_norm": 900.0 if spike else 1.8} for rank in TOPOLOGY}

    # Peer comparison alone: every rank is at the median, so no outliers.
    values = {rank: {"grad_norm": 900.0} for rank in TOPOLOGY}
    assert evaluator._outliers(values) == set()

    # With history behind it, the same step is a global anomaly.
    decisions = _drive(evaluator, per_step, steps=12)
    assert decisions[0].action is Action.REATTEMPT
    assert decisions[0].cause is Cause.APPLICATION
    assert "jumped" in decisions[0].reason


def test_a_single_anomalous_step_on_one_rank_is_not_enough():
    def per_step(step):
        odd = step == 5
        return {
            rank: {"grad_norm": 240.0 if (rank == 4 and odd) else 1.8}
            for rank in TOPOLOGY
        }

    assert _drive(NumericalEvaluator(confirm_steps=3), per_step) == []


def test_healthy_numbers_produce_nothing():
    def per_step(step):
        return {rank: {"grad_norm": 1.8 + rank * 0.01} for rank in TOPOLOGY}

    assert _drive(NumericalEvaluator(confirm_steps=2), per_step) == []


def test_a_numerical_outlier_is_corroborated_when_hardware_agrees():
    faulty = NVSentinelProbe._to_result(
        K8sNodeStatus(
            name="gpu-node-c",
            conditions={
                "SysLogsXIDError": NodeCondition(type="SysLogsXIDError", status="True")
            },
        )
    )
    nodes = {"nodeC": NodeHealth("nodeC", 1.0, {"NVSentinelProbe": faulty})}

    def per_step(step):
        return {rank: {"grad_norm": 240.0 if rank == 4 else 1.8} for rank in TOPOLOGY}

    decisions = _drive(NumericalEvaluator(confirm_steps=2), per_step, nodes=nodes)
    assert decisions[0].action is Action.EVICT
    assert "SysLogsXIDError" in decisions[0].reason


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
