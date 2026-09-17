import sys

import pytest

from ray.train.v2._internal.callbacks.health_callback import (
    build_node_exclusion_selector,
)
from ray.train.v2._internal.execution.health import (
    Action,
    Cause,
    ClusterContext,
    Diagnose,
    Evaluator,
    Evict,
    HealthConfig,
    HealthManager,
    HealthPolicy,
    HealthState,
    NodeHealth,
    Noop,
    ProbeResult,
    Reattempt,
    WorkerHealth,
    merge_decisions,
)
from ray.train.v2._internal.execution.health.adapters.nvsentinel import (
    K8sNodeStatus,
    NodeCondition,
    NVSentinelEvaluator,
    NVSentinelProbe,
    StaticNodeStatusSource,
    build_health_event,
    nvsentinel_policy,
)


# ----------------------------------------------------------------------
# Decision merging
# ----------------------------------------------------------------------
def test_merge_picks_most_severe_action():
    merged = merge_decisions(
        [
            Diagnose(reason="slow rank", target_ranks=[3]),
            Evict(reason="bad nic", target_nodes=["n1"], cause=Cause.HARDWARE),
            Reattempt(reason="transient"),
        ]
    )
    assert merged.action is Action.EVICT
    assert merged.target_nodes == ["n1"]


def test_merge_unions_at_the_winning_severity():
    merged = merge_decisions(
        [
            Evict(reason="ecc", target_nodes=["n1"], cause=Cause.HARDWARE),
            Evict(reason="nvlink", target_nodes=["n2", "n1"], cause=Cause.HARDWARE),
        ]
    )
    assert merged.target_nodes == ["n1", "n2"]
    # Every evaluator that fired is named in the reason.
    assert "ecc" in merged.reason and "nvlink" in merged.reason


def test_merge_prefers_the_actionable_cause():
    merged = merge_decisions(
        [
            Evict(reason="no progress", target_nodes=["n1"], cause=Cause.NO_PROGRESS),
            Evict(reason="gpu ecc", target_nodes=["n1"], cause=Cause.HARDWARE),
        ]
    )
    # HARDWARE is what authorizes quarantine downstream, so it must survive.
    assert merged.cause is Cause.HARDWARE


def test_merge_drops_noop_and_empty():
    assert merge_decisions([]) is None
    assert merge_decisions([Noop(), Noop(reason="fine")]) is None


def test_merge_suppresses_a_diagnose_on_an_already_condemned_node():
    """Diagnosing a node the run has already decided to evict changes nothing,
    and costs a py-spy attach on every rank there."""
    assert (
        merge_decisions(
            [Diagnose(target_nodes=["n1"], target_ranks=[4])], handled_nodes=["n1"]
        )
        is None
    )
    # A diagnose that names no node is not about a specific host, so it stands.
    merged = merge_decisions([Diagnose(target_ranks=[4])], handled_nodes=["n1"])
    assert merged.action is Action.DIAGNOSE


def test_merge_suppresses_already_handled_nodes():
    assert (
        merge_decisions(
            [Evict(target_nodes=["n1"], cause=Cause.HARDWARE)], handled_nodes=["n1"]
        )
        is None
    )
    # A second, different node still gets through.
    merged = merge_decisions(
        [Evict(target_nodes=["n1", "n2"], cause=Cause.HARDWARE)], handled_nodes=["n1"]
    )
    assert merged.target_nodes == ["n2"]


# ----------------------------------------------------------------------
# HealthState assembly
# ----------------------------------------------------------------------
class _Probe(NVSentinelProbe):
    pass


def test_health_state_typed_reads():
    state = HealthState(
        workers={
            0: WorkerHealth(0, "nodeA", 1.0, step=10, reported={"grad_norm": 1.8}),
            1: WorkerHealth(1, "nodeB", 1.0, step=10),
        },
        nodes={
            "nodeB": NodeHealth(
                "nodeB", 1.0, {"NVSentinelProbe": ProbeResult(metrics={"cordoned": 1})}
            )
        },
    )
    assert state.results(NVSentinelProbe)["nodeB"].metrics["cordoned"] == 1
    assert state.ranks_on("nodeB") == [1]
    assert state.node_of(0) == "nodeA"
    assert state.reported == {0: {"grad_norm": 1.8}}


def test_ingest_node_health_merges_across_sources():
    manager = HealthManager([])
    manager.ingest_node_health(NodeHealth("n1", 1.0, {"A": ProbeResult(detail="a")}))
    manager.ingest_node_health(NodeHealth("n1", 2.0, {"B": ProbeResult(detail="b")}))
    results = manager.build_state().nodes["n1"].probe_results
    # A NodeMonitor sample and a cluster probe sample must not clobber each other.
    assert set(results) == {"A", "B"}


# ----------------------------------------------------------------------
# Manager robustness: a broken detector must not fail the run
# ----------------------------------------------------------------------
class _Exploding(Evaluator):
    def __init__(self):
        self.calls = 0

    def evaluate(self, state):
        self.calls += 1
        raise RuntimeError("detector bug")


def test_raising_evaluator_is_disabled_not_fatal():
    boom = _Exploding()
    manager = HealthManager([HealthPolicy(evaluator_creator=lambda: [boom])])
    assert manager.poll_decision() is None
    assert manager.poll_decision() is None
    assert boom.calls == 1  # disabled after the first raise


def test_policy_creators_run_per_run_not_at_import():
    built = []

    def make():
        built.append(1)
        return [NVSentinelEvaluator()]

    policy = HealthPolicy(evaluator_creator=make)
    assert built == []
    HealthManager([policy])
    assert built == [1]


# ----------------------------------------------------------------------
# NVSentinel adapter
# ----------------------------------------------------------------------
def _fatal_gpu_node(name="gpu-node-01"):
    return K8sNodeStatus(
        name=name,
        conditions={
            "GpuMemWatch": NodeCondition(
                type="GpuMemWatch",
                status="True",
                reason="HardwareFailure",
                message=(
                    "[DCGM_FR_FAULTY_MEMORY] GPU memory failure detected on GPU 0 "
                    "- RecommendedAction: RESTART_VM"
                ),
            ),
            "Ready": NodeCondition(type="Ready", status="True"),
        },
        taints=[
            {"key": "nvidia.com/gpu-xid-error", "value": "true", "effect": "NoSchedule"}
        ],
        unschedulable=True,
    )


def test_condition_message_parsing():
    parsed = _fatal_gpu_node().conditions["GpuMemWatch"].parsed()
    assert parsed["codes"] == ["DCGM_FR_FAULTY_MEMORY"]
    assert parsed["action"] == "RESTART_VM"
    assert "GPU memory failure" in parsed["text"]


def test_probe_maps_node_conditions_to_evidence():
    source = StaticNodeStatusSource({"gpu-node-01": _fatal_gpu_node()})
    probe = NVSentinelProbe(
        source=source, node_name_resolver=lambda ids: {"rayA": "gpu-node-01"}
    )
    result = probe.poll(ClusterContext(node_ids=["rayA"]))["rayA"]
    assert result.passed is False
    assert result.metrics["fatal_conditions"] == 1
    assert result.metrics["cordoned"] == 1
    assert "condition:GpuMemWatch" in result.events
    assert "taint:nvidia.com/gpu-xid-error" in result.events


def test_probe_reports_nothing_for_unknown_nodes():
    # An unmapped node must not be reported as healthy -- absence of evidence
    # is not evidence of health.
    probe = NVSentinelProbe(
        source=StaticNodeStatusSource({}), node_name_resolver=lambda ids: {}
    )
    assert probe.poll(ClusterContext(node_ids=["rayA"])) == {}


def test_healthy_node_produces_no_decision():
    healthy = K8sNodeStatus(
        name="gpu-node-02",
        conditions={"Ready": NodeCondition(type="Ready", status="True")},
    )
    probe = NVSentinelProbe(
        source=StaticNodeStatusSource({"gpu-node-02": healthy}),
        node_name_resolver=lambda ids: {"rayB": "gpu-node-02"},
    )
    result = probe.poll(ClusterContext(node_ids=["rayB"]))["rayB"]
    assert result.passed is True

    state = HealthState(
        workers={0: WorkerHealth(0, "rayB", 1.0)},
        nodes={"rayB": NodeHealth("rayB", 1.0, {"NVSentinelProbe": result})},
    )
    assert NVSentinelEvaluator().evaluate(state) == []


def test_evaluator_ignores_faults_on_nodes_this_run_does_not_use():
    result = NVSentinelProbe._to_result(_fatal_gpu_node())
    state = HealthState(
        workers={0: WorkerHealth(0, "rayA", 1.0)},
        nodes={"rayZ": NodeHealth("rayZ", 1.0, {"NVSentinelProbe": result})},
    )
    assert NVSentinelEvaluator().evaluate(state) == []


def test_evaluator_confirm_polls_debounce():
    result = NVSentinelProbe._to_result(_fatal_gpu_node())
    state = HealthState(
        workers={0: WorkerHealth(0, "rayA", 1.0)},
        nodes={"rayA": NodeHealth("rayA", 1.0, {"NVSentinelProbe": result})},
    )
    evaluator = NVSentinelEvaluator(confirm_polls=2)
    assert evaluator.evaluate(state) == []
    assert evaluator.evaluate(state)[0].action is Action.EVICT


def test_cordon_alone_is_infrastructure_not_hardware():
    cordoned = K8sNodeStatus(name="gpu-node-03", unschedulable=True)
    result = NVSentinelProbe._to_result(cordoned)
    state = HealthState(
        workers={0: WorkerHealth(0, "rayC", 1.0)},
        nodes={"rayC": NodeHealth("rayC", 1.0, {"NVSentinelProbe": result})},
    )
    decision = NVSentinelEvaluator().evaluate(state)[0]
    assert decision.action is Action.EVICT
    assert decision.cause is Cause.INFRASTRUCTURE


# ----------------------------------------------------------------------
# End to end: fault -> decision -> the selector that excludes the node
# ----------------------------------------------------------------------
def test_end_to_end_fault_to_node_exclusion():
    source = StaticNodeStatusSource({"gpu-node-01": _fatal_gpu_node()})
    policy = nvsentinel_policy(source=source)
    manager = HealthManager([policy])

    # Two ranks per node, the fault is on the node backing ranks 2 and 3.
    for rank, node in [(0, "rayA"), (1, "rayA"), (2, "rayB"), (3, "rayB")]:
        manager.ingest_worker_health(WorkerHealth(rank, node, 1.0, step=1450))

    probe = manager.cluster_probes()[0]
    probe._resolve = lambda ids: {"rayA": "gpu-node-00", "rayB": "gpu-node-01"}

    manager.run_cluster_probes(["rayA", "rayB"])
    decision = manager.poll_decision()

    assert isinstance(decision, Evict)
    assert decision.cause is Cause.HARDWARE
    assert decision.target_nodes == ["rayB"]
    assert "ranks [2, 3]" in decision.reason

    # The same fault on the next poll is suppressed: it is already handled.
    manager.run_cluster_probes(["rayA", "rayB"])
    assert manager.poll_decision() is None

    # And the next worker group is scheduled away from it.
    selector = build_node_exclusion_selector(manager.evicted_nodes)
    assert selector == {"ray.io/node-id": "!in(rayB)"}


def test_node_exclusion_selector_is_noop_when_nothing_is_evicted():
    assert build_node_exclusion_selector([]) is None


def test_node_exclusion_selector_accumulates():
    assert build_node_exclusion_selector(["n2", "n1"]) == {
        "ray.io/node-id": "!in(n1,n2)"
    }


# ----------------------------------------------------------------------
# Outbound: Ray Train -> NVSentinel
# ----------------------------------------------------------------------
def test_health_event_shadow_mode_changes_nothing_in_the_cluster():
    event = build_health_event(
        Evict(cause=Cause.HARDWARE, reason="nccl hang", target_nodes=["rayB"]),
        node_name="gpu-node-01",
        run_id="run-123",
        shadow=True,
    )
    assert event["processingStrategy"] == "STORE_ONLY"
    assert event["isFatal"] is False


def test_health_event_enforcing_mode_asks_for_quarantine_but_not_drain():
    event = build_health_event(
        Evict(cause=Cause.HARDWARE, reason="nccl hang", target_nodes=["rayB"]),
        node_name="gpu-node-01",
        shadow=False,
    )
    assert event["processingStrategy"] == "EXECUTE_REMEDIATION"
    assert event["isFatal"] is True
    assert event["quarantineOverrides"]["force"] is True
    # Ray Train moves its own workers; a concurrent drain would race the restart.
    assert event["drainOverrides"]["skip"] is True
    assert event["nodeName"] == "gpu-node-01"
    assert event["agent"] == "ray-train"


def test_non_hardware_cause_never_asks_for_quarantine():
    event = build_health_event(
        Reattempt(cause=Cause.APPLICATION, reason="nan loss"),
        node_name="gpu-node-01",
        shadow=False,
    )
    assert event["isFatal"] is False
    assert event["quarantineOverrides"]["force"] is False


def test_health_config_defaults_to_off():
    assert HealthConfig().policies == []
    assert HealthManager(HealthConfig().policies).enabled is False


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
