"""What a node agent cannot decide, side by side with what Ray Train can.

    python release/train_tests/health/why_in_ray_demo.py

Two faults on a 6-rank / 3-node run. Each is put to two policy sets:

  hardware-only   what NVSentinel and NCCL RAS can see on their own
  with UDF        the same, plus metrics the training loop reports

The training loop's contribution is one line:

    health.report({"step_time_s": dt, "weight_checksum": ck}, step=step)
"""
import sys

from ray.train.v2._internal.execution.health import (
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
)

TOPOLOGY = {0: "nodeA", 1: "nodeA", 2: "nodeB", 3: "nodeB", 4: "nodeC", 5: "nodeC"}
NOMINAL_STEP_S = 0.40


def clean_nodes():
    """Every node condition green. This is what NVSentinel reports here."""
    clean = NVSentinelProbe._to_result(
        K8sNodeStatus(
            name="n", conditions={"Ready": NodeCondition(type="Ready", status="True")}
        )
    )
    return {
        node: NodeHealth(node, 1.0, {"NVSentinelProbe": clean})
        for node in set(TOPOLOGY.values())
    }


def state(reported, step, nodes=None, on_demand=None):
    return HealthState(
        workers={
            rank: WorkerHealth(rank, node, 1.0, step=step, reported=reported[rank])
            for rank, node in TOPOLOGY.items()
        },
        nodes=nodes if nodes is not None else clean_nodes(),
        on_demand_probes=on_demand or {},
    )


def verdict(decisions):
    if not decisions:
        return "no decision — nothing to act on"
    d = decisions[0]
    target = getattr(d, "target_nodes", None)
    return f"{d.action.name} ({d.cause.name})" + (f" → {target}" if target else "")


def banner(title, subtitle):
    print(f"\n{'=' * 74}\n{title}\n{subtitle}\n{'=' * 74}")


def scenario_sdc():
    banner(
        "Fault 1 — a GPU quietly computes the wrong answer",
        "rank 4's post-allreduce checksum disagrees with its DP peers",
    )
    reported = {
        rank: {"weight_checksum": 0x1234 if rank == 4 else 0xABCD} for rank in TOPOLOGY
    }

    print("\n  what the hardware reports:")
    print("    NVSentinel node conditions ....... all green")
    print("    NCCL RAS ......................... communicators progressing")
    print("    DCGM / XID / ECC ................. nothing")
    print("  what the training loop reports:")
    print("    rank 4 weight_checksum ........... 0x1234")
    print("    ranks 0,1,2,3,5 .................. 0xabcd")

    hw = NVSentinelEvaluator().evaluate(state(reported, step=1))
    sdc = SdcEvaluator(confirm_steps=2)
    sdc.evaluate(state(reported, step=1))
    udf = sdc.evaluate(state(reported, step=2))

    print(f"\n  hardware-only : {verdict(hw)}")
    print(f"  with UDF      : {verdict(udf)}")
    if udf:
        print(f"                  {udf[0].reason}")
    print(
        "\n  Silent corruption is silent by definition: the hardware raised\n"
        "  nothing, so no node agent can ever fire here. It is not missing a\n"
        "  rule, it is missing the numbers."
    )


def scenario_straggler():
    banner(
        "Fault 2 — a GPU is throttling and nobody is allowed to care",
        "rank 4 takes 2.4x as long per step; its host is thermally throttled",
    )
    reported = {
        rank: {"step_time_s": NOMINAL_STEP_S * (2.4 if rank == 4 else 1.0)}
        for rank in TOPOLOGY
    }

    print("\n  what the hardware reports:")
    print("    NVSentinel ....................... thermal throttling is NON-FATAL,")
    print("                                       so it writes a k8s Event and")
    print("                                       never cordons")
    print("    NCCL RAS ......................... op counts advancing, not a hang")
    print("  what the training loop reports:")
    print(f"    rank 4 step_time_s ............... {NOMINAL_STEP_S * 2.4:.2f}s")
    print(f"    every other rank ................. {NOMINAL_STEP_S:.2f}s")

    hw = NVSentinelEvaluator().evaluate(state(reported, step=1))

    straggler = StragglerEvaluator(diagnostics=[object()])
    asked = []
    for step in range(8):
        asked = straggler.evaluate(state(reported, step=step)) or asked

    print(f"\n  hardware-only : {verdict(hw)}")
    print(f"  with UDF      : {verdict(asked)}")
    if asked:
        print(f"                  {asked[0].reason}")

    # The diagnostic comes back: that host is throttling.
    throttling = {
        "NvidiaSmiProbe": {
            "nodeC": ProbeResult(
                metrics={"max_temp_c": 88.0},
                events=["thermal_throttle"],
                passed=False,
            )
        }
    }
    acted = straggler.evaluate(state(reported, step=99, on_demand=throttling))
    print(f"  after diagnose: {verdict(acted)}")
    if acted:
        print(f"                  {acted[0].reason}")

    print(
        "\n  Neither half is actionable alone. Throttling happens constantly and\n"
        "  is usually harmless — evict on it and you cordon healthy hosts all day.\n"
        "  A slow rank is ambiguous — it could be the data loader. Joined, they\n"
        "  are a specific degraded GPU costing 58% of this run's throughput."
    )


def scenario_false_positive():
    banner(
        "Control — the data loader stalls and every rank slows together",
        "the case a throughput alarm gets wrong",
    )
    reported = {rank: {"step_time_s": 1.6} for rank in TOPOLOGY}
    straggler = StragglerEvaluator(diagnostics=[object()])
    seen = []
    for step in range(8):
        seen.extend(straggler.evaluate(state(reported, step=step)))

    print("\n  every rank ....................... 1.60s (4x nominal)")
    print(f"  with UDF      : {verdict(seen)}")
    print(
        "\n  Throughput collapsed and no node is at fault. The check compares\n"
        "  ranks to each other, not to a threshold, so it stays quiet."
    )


def main() -> int:
    print("Ray Train health: the join, and why it has to live here")
    scenario_sdc()
    scenario_straggler()
    scenario_false_positive()
    print(
        "\n"
        + "=" * 74
        + "\nThe division of labour\n"
        + "=" * 74
        + "\n  NVSentinel owns everything below the node, and owns repair.\n"
        "  Ray Train owns the numbers inside the training process, the\n"
        "  rank→host map, and the decision to restart.\n"
        "  The faults above need all three, and only one process has them.\n"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
