"""The DIAGNOSE push path, end to end, with no cluster.

The flow under test is the one the hang detector wants:

  1. a run starts with the NCCL RAS probe enabled,
  2. a communicator wedges and is confirmed,
  3. the controller pushes diagnostics at the ranks and hosts involved,
  4. what they find lands in the next HealthState and decides the action.

Step 4 is what ray-project/ray#64928 and #66229 cannot do: there, the
diagnostics write files and the detector raises. Here they are evidence.
"""
import sys

import pytest

from ray.train.v2._internal.callbacks.nccl_ras import RASReport
from ray.train.v2._internal.execution.health import (
    Action,
    Cause,
    Diagnose,
    DiagnosticRunner,
    HealthManager,
    ProbeResult,
    WorkerHealth,
)
from ray.train.v2._internal.execution.health.adapters.nccl_ras_diagnostics import (
    NvidiaSmiProbe,
    StackTraceProbe,
    _parse_nvidia_smi,
    hang_diagnostics,
)
from ray.train.v2._internal.execution.health.adapters.nccl_ras_policy import (
    nccl_ras_policy,
)

# Two nodes, two ranks each.
TOPOLOGY = [(0, "nodeA"), (1, "nodeA"), (2, "nodeB"), (3, "nodeB")]

# Trimmed from a real `nvidia-smi -q` on an A10G. The nesting matters: the
# uncorrectable counters live under `Volatile` (this run) and `Aggregate`
# (lifetime), and there is no field called plain "Uncorrectable". The full
# capture is in tests/data/health/.
_HEALTHY_SMI = """
    Clocks Event Reasons
        HW Slowdown                                    : Not Active
            HW Thermal Slowdown                        : Not Active
            HW Power Brake Slowdown                    : Not Active
        SW Thermal Slowdown                            : Not Active
    ECC Errors
        Volatile
            SRAM Uncorrectable Parity                  : 0
            SRAM Uncorrectable SEC-DED                 : 0
            DRAM Uncorrectable                         : 0
        Aggregate
            SRAM Uncorrectable Parity                  : 0
            DRAM Uncorrectable                         : 0
    Remapped Rows
        Uncorrectable Error                            : 0
        Remapping Failure Occurred                     : No
    Temperature
        GPU Current Temp                               : 39 C
        GPU Slowdown Temp                              : 95 C
"""

_FAULTY_SMI = """
    Clocks Event Reasons
        HW Slowdown                                    : Not Active
            HW Thermal Slowdown                        : Not Active
            HW Power Brake Slowdown                    : Not Active
        SW Thermal Slowdown                            : Active
    ECC Errors
        Volatile
            SRAM Uncorrectable Parity                  : 0
            SRAM Uncorrectable SEC-DED                 : 0
            DRAM Uncorrectable                         : 4
        Aggregate
            SRAM Uncorrectable Parity                  : 0
            DRAM Uncorrectable                         : 4
    Remapped Rows
        Uncorrectable Error                            : 0
        Remapping Failure Occurred                     : No
    Temperature
        GPU Current Temp                               : 96 C
        GPU Slowdown Temp                              : 95 C
"""


def _stalled_report(comm="c1", ranks=(2, 3)):
    """A communicator whose ranks are skewed and RUNNING."""
    counts = {comm: {r: {"AllReduce": 10 + i} for i, r in enumerate(ranks)}}
    status = {comm: {r: "RUNNING" for r in ranks}}
    return RASReport("t", counts, status)


class _Uploads:
    """Stands in for the run's storage filesystem."""

    def __init__(self):
        self.written = {}

    def __call__(self, tool, files):
        self.written.setdefault(tool, {}).update(files)
        return f"s3://run/hang_detector/{tool}"


def _runner(smi_by_node=None, uploads=None, **kwargs):
    """A DiagnosticRunner whose dispatch runs the probes in-process."""
    smi_by_node = smi_by_node or {}
    uploads = uploads if uploads is not None else _Uploads()

    def run_on_worker(entity, probe, ctx):
        return probe.poll(ctx)

    def run_on_node(entity, probe, ctx):
        if isinstance(probe, NvidiaSmiProbe):
            out = smi_by_node.get(entity)
            if out is None:
                return ProbeResult(
                    metrics={"driver_responded": 0.0},
                    events=["nvidia_smi_unresponsive"],
                    passed=False,
                    detail=f"node {entity}: driver did not answer",
                )
            metrics, events = _parse_nvidia_smi(out)
            return ProbeResult(
                metrics={"driver_responded": 1.0, **metrics},
                events=events,
                passed=not events,
                detail=f"node {entity}",
                artifacts=[ctx.upload("nvidia_smi", {f"{entity}.log": out})],
            )
        return probe.poll(ctx)

    return (
        DiagnosticRunner(
            run_on_worker=run_on_worker,
            run_on_node=run_on_node,
            upload=uploads,
            **kwargs,
        ),
        uploads,
    )


# ----------------------------------------------------------------------
# Targeting: who a Diagnose actually reaches
# ----------------------------------------------------------------------
def test_worker_scoped_probes_go_to_the_named_ranks_only():
    runner, _ = _runner()
    decision = Diagnose(on_demand_probes=[StackTraceProbe()], target_ranks=[2, 3])
    report = runner.run(decision, TOPOLOGY)
    assert sorted(report.results["StackTraceProbe"]) == ["2", "3"]


def test_node_scoped_probes_go_to_hosts_not_ranks():
    """Two ranks on one host is one snapshot, not two.

    nvidia-smi reports every GPU on the node, so per-rank fan-out would just
    repeat itself -- #66229 dedups by node_ip for exactly this reason.
    """
    runner, _ = _runner(smi_by_node={"nodeB": _HEALTHY_SMI})
    decision = Diagnose(on_demand_probes=[NvidiaSmiProbe()], target_nodes=["nodeB"])
    report = runner.run(decision, TOPOLOGY)
    assert list(report.results["NvidiaSmiProbe"]) == ["nodeB"]


def test_ranks_localize_to_their_hosts_for_node_scoped_probes():
    # A rank-level finding still drives a host-level check.
    runner, _ = _runner(smi_by_node={"nodeB": _HEALTHY_SMI})
    decision = Diagnose(on_demand_probes=[NvidiaSmiProbe()], target_ranks=[2, 3])
    report = runner.run(decision, TOPOLOGY)
    assert list(report.results["NvidiaSmiProbe"]) == ["nodeB"]


def test_an_untargeted_diagnose_sweeps_everything():
    # A hang nobody could localize still wants a full sweep.
    runner, _ = _runner(smi_by_node={"nodeA": _HEALTHY_SMI, "nodeB": _HEALTHY_SMI})
    report = runner.run(
        Diagnose(on_demand_probes=[StackTraceProbe(), NvidiaSmiProbe()]), TOPOLOGY
    )
    assert sorted(report.results["StackTraceProbe"]) == ["0", "1", "2", "3"]
    assert sorted(report.results["NvidiaSmiProbe"]) == ["nodeA", "nodeB"]


# ----------------------------------------------------------------------
# Isolation: a diagnostic must never take the run down
# ----------------------------------------------------------------------
def test_a_probe_that_raises_is_recorded_not_propagated():
    def boom(entity, probe, ctx):
        raise RuntimeError("py-spy segfaulted")

    runner = DiagnosticRunner(run_on_worker=boom, run_on_node=boom)
    report = runner.run(
        Diagnose(on_demand_probes=[StackTraceProbe()], target_ranks=[2]), TOPOLOGY
    )
    result = report.results["StackTraceProbe"]["2"]
    assert result.passed is False
    assert "py-spy segfaulted" in result.detail


def test_an_unresponsive_driver_is_a_finding_not_a_gap():
    """nvidia-smi blocking is the single most diagnostic thing it can do."""
    runner, _ = _runner(smi_by_node={})  # nodeB never answers
    report = runner.run(
        Diagnose(on_demand_probes=[NvidiaSmiProbe()], target_nodes=["nodeB"]), TOPOLOGY
    )
    result = report.results["NvidiaSmiProbe"]["nodeB"]
    assert result.passed is False
    assert "nvidia_smi_unresponsive" in result.events


def test_workers_are_paused_once_for_a_batch_that_needs_the_gpu():
    calls = []

    class Heavy(StackTraceProbe):
        name = "Heavy"
        stop_workers = True

    runner, _ = _runner(
        smi_by_node={"nodeA": _HEALTHY_SMI, "nodeB": _HEALTHY_SMI},
        pause_workers=lambda: calls.append("pause"),
        resume_workers=lambda: calls.append("resume"),
    )
    runner.run(Diagnose(on_demand_probes=[Heavy(), Heavy()]), TOPOLOGY)
    # Stopping and restarting workers costs more than the checks do.
    assert calls == ["pause", "resume"]


def test_nothing_is_paused_when_no_probe_needs_the_gpu():
    calls = []
    runner, _ = _runner(
        smi_by_node={"nodeA": _HEALTHY_SMI, "nodeB": _HEALTHY_SMI},
        pause_workers=lambda: calls.append("pause"),
        resume_workers=lambda: calls.append("resume"),
    )
    runner.run(
        Diagnose(on_demand_probes=[StackTraceProbe(), NvidiaSmiProbe()]), TOPOLOGY
    )
    assert calls == []


# ----------------------------------------------------------------------
# Artifacts
# ----------------------------------------------------------------------
def test_bulk_output_goes_to_storage_and_the_result_carries_the_path():
    runner, uploads = _runner(smi_by_node={"nodeB": _FAULTY_SMI})
    report = runner.run(
        Diagnose(
            on_demand_probes=[StackTraceProbe(), NvidiaSmiProbe()],
            target_ranks=[2],
            target_nodes=["nodeB"],
        ),
        TOPOLOGY,
    )
    assert "rank_2.log" in uploads.written["stack_traces"]
    assert "nodeB.log" in uploads.written["nvidia_smi"]
    assert "s3://run/hang_detector/nvidia_smi" in report.artifacts


def test_nvidia_smi_parsing_pulls_only_what_a_policy_acts_on():
    metrics, events = _parse_nvidia_smi(_FAULTY_SMI)
    assert metrics["ecc_uncorrectable"] == 4.0  # Volatile only, not Volatile+Aggregate
    assert metrics["ecc_uncorrectable_lifetime"] == 4.0
    assert metrics["max_temp_c"] == 96.0
    # 96 C is past this card's own 95 C slowdown point, read from the report.
    assert events == ["ecc_uncorrectable", "gpu_hot", "thermal_throttle"]

    metrics, events = _parse_nvidia_smi(_HEALTHY_SMI)
    assert metrics["ecc_uncorrectable"] == 0.0
    assert metrics["slowdown_temp_c"] == 95.0
    assert events == []


# ----------------------------------------------------------------------
# The whole flow
# ----------------------------------------------------------------------
def _run_until_decision(manager, reports_seen=3):
    for _ in range(reports_seen):
        manager.run_cluster_probes(["nodeA", "nodeB"])
        decision = manager.poll_decision()
        if decision is not None:
            return decision
    return None


def _start_run(diagnostics, confirm_s=0.0):
    """A manager with the RAS probe enabled and a wedged communicator."""
    stalled = [_stalled_report(), _stalled_report(), _stalled_report()]
    policy = nccl_ras_policy(
        query=lambda: stalled.pop(0) if stalled else None,
        confirm_duration_s=confirm_s,
        diagnostics=diagnostics,
    )
    manager = HealthManager([policy])
    for rank, node in TOPOLOGY:
        manager.ingest_worker_health(WorkerHealth(rank, node, 1.0, step=1450))
    return manager


def test_step_1_and_2_a_confirmed_hang_asks_for_diagnostics():
    manager = _start_run(diagnostics=hang_diagnostics())
    decision = _run_until_decision(manager)

    assert decision.action is Action.DIAGNOSE
    assert decision.cause is Cause.NO_PROGRESS
    # Targeted at the stalled ranks and their host, not the whole job.
    assert decision.target_ranks == [2, 3]
    assert decision.target_nodes == ["nodeB"]
    assert [p.probe_name() for p in decision.on_demand_probes] == [
        "StackTraceProbe",
        "NvidiaSmiProbe",
    ]


def test_step_3_and_4_faulty_hardware_escalates_to_an_eviction():
    manager = _start_run(diagnostics=hang_diagnostics())
    decision = _run_until_decision(manager)
    assert decision.action is Action.DIAGNOSE

    runner, _ = _runner(smi_by_node={"nodeB": _FAULTY_SMI})
    manager.ingest_diagnostic_report(runner.run(decision, TOPOLOGY))

    # The loop closes here: the next pass reads what the action produced.
    followup = manager.poll_decision()
    assert followup.action is Action.EVICT
    assert followup.cause is Cause.HARDWARE
    assert followup.target_nodes == ["nodeB"]
    assert "ecc_uncorrectable" in followup.reason


def test_step_3_and_4_clean_hardware_stays_a_reattempt():
    """Hardware ruled out means the hang is software or data.

    Evicting here would cordon a healthy host for a bug in user code.
    """
    manager = _start_run(diagnostics=hang_diagnostics())
    decision = _run_until_decision(manager)

    runner, _ = _runner(smi_by_node={"nodeB": _HEALTHY_SMI})
    manager.ingest_diagnostic_report(runner.run(decision, TOPOLOGY))

    followup = manager.poll_decision()
    assert followup.action is Action.REATTEMPT
    assert followup.cause is Cause.NO_PROGRESS
    assert "no hardware fault" in followup.reason


def test_diagnostics_are_pushed_once_per_hang_not_every_poll():
    """A persistent hang must not re-attach py-spy to 1,024 ranks every poll."""
    manager = _start_run(diagnostics=hang_diagnostics())
    first = _run_until_decision(manager)
    assert first.action is Action.DIAGNOSE

    manager.run_cluster_probes(["nodeA", "nodeB"])
    second = manager.poll_decision()
    assert second is None or second.action is not Action.DIAGNOSE


def test_without_diagnostics_a_confirmed_hang_still_acts():
    # Back-compatible with the detector as it ships today.
    manager = _start_run(diagnostics=None)
    decision = _run_until_decision(manager)
    assert decision.action is Action.REATTEMPT


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
