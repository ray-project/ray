"""Walk the DIAGNOSE loop end to end, printing each stage. No cluster needed.

    python release/train_tests/health/diagnose_flow_demo.py

Simulates a 4-rank / 2-node run where one communicator wedges, and shows the
four steps: the RAS probe collects, the evaluator confirms and asks, the
controller pushes diagnostics at the ranks and host involved, and the evidence
they produce decides the action.

Pass ``--healthy-gpus`` to see the same hang with clean hardware, which stays a
reattempt instead of evicting a host that did nothing wrong.
"""
import sys

from ray.train.v2._internal.callbacks.nccl_ras import RASReport
from ray.train.v2._internal.execution.health import (
    DiagnosticRunner,
    HealthManager,
    ProbeResult,
    WorkerHealth,
)
from ray.train.v2._internal.execution.health.adapters.nccl_ras_diagnostics import (
    NvidiaSmiProbe,
    _parse_nvidia_smi,
    hang_diagnostics,
)
from ray.train.v2._internal.execution.health.adapters.nccl_ras_policy import (
    nccl_ras_policy,
)

TOPOLOGY = [(0, "nodeA"), (1, "nodeA"), (2, "nodeB"), (3, "nodeB")]

FAULTY_SMI = """
GPU 00000000:07:00.0
    GPU Current Temp                  : 93 C
    ECC Errors
        Volatile
            Uncorrectable             : 4
    Clocks Event Reasons
        SW Thermal Slowdown           : Active
"""

HEALTHY_SMI = """
GPU 00000000:07:00.0
    GPU Current Temp                  : 61 C
    ECC Errors
        Volatile
            Uncorrectable             : 0
    Clocks Event Reasons
        SW Thermal Slowdown           : Not Active
"""


def wedged_report():
    """Ranks 2 and 3 skewed inside communicator c1, both still RUNNING."""
    return RASReport(
        "t",
        {"c1": {2: {"AllReduce": 1450}, 3: {"AllReduce": 1447}}},
        {"c1": {2: "RUNNING", 3: "RUNNING"}},
    )


def main(healthy_gpus: bool = False) -> int:
    smi = HEALTHY_SMI if healthy_gpus else FAULTY_SMI
    uploaded = {}

    def upload(tool, files):
        uploaded.setdefault(tool, []).extend(files)
        return f"s3://my-run/hang_detector/{tool}"

    def run_on_worker(entity, probe, ctx):
        return probe.poll(ctx)

    def run_on_node(entity, probe, ctx):
        if isinstance(probe, NvidiaSmiProbe):
            metrics, events = _parse_nvidia_smi(smi)
            return ProbeResult(
                metrics={"driver_responded": 1.0, **metrics},
                events=events,
                passed=not events,
                detail=f"node {entity}",
                artifacts=[ctx.upload("nvidia_smi", {f"{entity}.log": smi})],
            )
        return probe.poll(ctx)

    # 1. Launch: the RAS probe and its diagnostics are one policy.
    print("1. starting run with the NCCL RAS probe enabled")
    reports = [wedged_report() for _ in range(3)]
    manager = HealthManager(
        [
            nccl_ras_policy(
                query=lambda: reports.pop(0) if reports else None,
                confirm_duration_s=0.0,  # demo: confirm on the first frozen sample
                diagnostics=hang_diagnostics(),
            )
        ]
    )
    for rank, node in TOPOLOGY:
        manager.ingest_worker_health(WorkerHealth(rank, node, 1.0, step=1450))

    # 2. Collect and decide, until something fires.
    decision = None
    for poll in range(1, 4):
        manager.run_cluster_probes(["nodeA", "nodeB"])
        comms = manager.build_state().results(type(manager.cluster_probes()[0]))
        state = ", ".join(
            f"{c}={'frozen' if 'frozen' in r.events else 'ok'}"
            for c, r in sorted(comms.items())
        )
        print(f"   poll {poll}: {state or 'no report yet'}")
        decision = manager.poll_decision()
        if decision is not None:
            break

    if decision is None:
        print("   no decision — nothing wrong")
        return 0

    print(f"\n2. decision: {decision.action.name} ({decision.cause.name})")
    print(f"   {decision.reason}")
    print(f"   ranks {decision.target_ranks} on nodes {decision.target_nodes}")
    print("   probes: " + ", ".join(p.probe_name() for p in decision.on_demand_probes))

    # 3. Act: push the diagnostics.
    print("\n3. pushing diagnostics")
    runner = DiagnosticRunner(
        run_on_worker=run_on_worker, run_on_node=run_on_node, upload=upload
    )
    report = runner.run(decision, TOPOLOGY)
    for probe_name, results in sorted(report.results.items()):
        for entity, result in sorted(results.items()):
            print(f"   {probe_name}[{entity}]: {result.detail}")
    for tool, files in sorted(uploaded.items()):
        print(f"   uploaded {tool}/: {', '.join(sorted(files))}")

    # 4. The loop closes: that evidence is in the next HealthState.
    manager.ingest_diagnostic_report(report)
    followup = manager.poll_decision()
    print(f"\n4. decision: {followup.action.name} ({followup.cause.name})")
    print(f"   {followup.reason}")
    if getattr(followup, "target_nodes", None):
        print(f"   → restart without {followup.target_nodes}")
    else:
        print("   → restart from the last checkpoint, same hardware")
    return 0


if __name__ == "__main__":
    sys.exit(main(healthy_gpus="--healthy-gpus" in sys.argv))
