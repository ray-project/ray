"""Drive the real detectors with the real injectors. Runs anywhere, no GPU.

    python release/train_tests/health/injected_faults_demo.py

Each scenario runs a simulated 6-rank training loop where one rank has a fault
injected from ``health.testing.symptoms``. The loop calls ``health.report()``
exactly as user code would; the numbers the evaluators see are the numbers the
injected fault actually produced, not fixtures.

This is the fast half of the test plan. The slow half swaps ``symptoms`` for
``nvrx`` (real process faults) and ``dcgm`` (real device telemetry through
NVSentinel) -- see INJECTION.md.
"""
import os
import sys
import time

from ray.train.v2._internal.execution.health import (
    HealthState,
    ProbeResult,
    WorkerHealth,
)
from ray.train.v2._internal.execution.health.adapters.udf_signals import (
    NumericalEvaluator,
    StragglerEvaluator,
)
from ray.train.v2._internal.execution.health.report import reset, snapshot
from ray.train.v2._internal.execution.health.testing import symptoms

WORLD_SIZE = 6
TOPOLOGY = {0: "nodeA", 1: "nodeA", 2: "nodeB", 3: "nodeB", 4: "nodeC", 5: "nodeC"}
STEPS = 14
WORK_S = 0.004  # stand-in for a training step


def run_loop(make_fault, steps=STEPS):
    """Run every rank through the loop, collecting what each reported per step.

    ``make_fault(rank)`` returns the fault object for that rank. Ranks run one
    after another here rather than in parallel, which changes nothing the
    evaluators look at: they compare ranks at the same step.
    """
    per_step = [dict() for _ in range(steps)]
    for rank in range(WORLD_SIZE):
        os.environ["RANK"] = str(rank)
        reset()
        fault = make_fault(rank)
        for step in range(steps):
            if hasattr(fault, "step"):
                with fault.step(step):
                    time.sleep(WORK_S)
            else:
                time.sleep(WORK_S)
                fault.observe(step)
            per_step[step][rank] = dict(snapshot().metrics)
    os.environ.pop("RANK", None)
    reset()
    return per_step


def drive(evaluator, per_step, on_demand=None):
    seen = []
    for step, reported in enumerate(per_step):
        state = HealthState(
            workers={
                rank: WorkerHealth(rank, node, 1.0, step=step, reported=reported[rank])
                for rank, node in TOPOLOGY.items()
            },
            on_demand_probes=on_demand or {},
        )
        seen.extend(evaluator.evaluate(state))
    return seen


def throttling(node="nodeC"):
    return {
        "NvidiaSmiProbe": {
            node: ProbeResult(
                metrics={"max_temp_c": 88.0},
                events=["thermal_throttle"],
                passed=False,
            )
        }
    }


def verdict(decisions):
    if not decisions:
        return "no action"
    d = decisions[0]
    nodes = getattr(d, "target_nodes", None)
    return f"{d.action.name} ({d.cause.name})" + (f" → {nodes}" if nodes else "")


def row(name, injected, decisions, expected):
    got = verdict(decisions)
    mark = "ok " if expected in got else "BAD"
    print(f"  [{mark}] {name:<34} {injected:<26} {got}")
    return expected in got


def main(verbose: bool = False) -> int:
    import logging

    # The evaluators log their reasoning at INFO -- why a straggler was ruled
    # out, which stage was imbalanced. Worth reading once.
    logging.basicConfig(
        level=logging.INFO if verbose else logging.WARNING,
        format="      %(message)s",
    )
    print("Injected fault → what the detector decides\n")
    print(f"  {'':<5}{'scenario':<34} {'injected':<26} decision")
    print("  " + "-" * 84)
    ok = True

    # --- stragglers: same symptom, four causes ------------------------
    ok &= row(
        "degrading GPU",
        "Straggler(rank=4, x2.4)",
        drive(
            StragglerEvaluator(),
            run_loop(lambda r: symptoms.Straggler(rank=4, slowdown=2.4)),
            on_demand=throttling(),
        ),
        "EVICT",
    )
    ok &= row(
        "GC pause / host jitter",
        "WanderingStraggler(x2.4)",
        drive(
            StragglerEvaluator(diagnostics=[object()]),
            run_loop(
                lambda r: symptoms.WanderingStraggler(
                    world_size=WORLD_SIZE, slowdown=2.4
                )
            ),
            on_demand=throttling(),
        ),
        "no action",
    )
    ok &= row(
        "pipeline-stage skew",
        "Straggler on a whole stage",
        drive(
            StragglerEvaluator(diagnostics=[object()]),
            run_loop(
                lambda r: symptoms.Straggler(
                    rank=r if r in (4, 5) else -1,
                    slowdown=1.9,
                    pipeline_stage=3 if r in (4, 5) else r // 2,
                )
            ),
            on_demand=throttling(),
        ),
        "no action",
    )
    ok &= row(
        "slow data shard",
        "Straggler(phase='dataload')",
        drive(
            StragglerEvaluator(diagnostics=[object()]),
            run_loop(
                lambda r: symptoms.Straggler(rank=4, slowdown=2.4, phase="dataload")
            ),
            on_demand=throttling(),
        ),
        "no action",
    )

    # --- numerics: one rank vs everyone -------------------------------
    print()
    ok &= row(
        "one rank, NaN gradients",
        "NumericalFault(rank=4)",
        drive(
            NumericalEvaluator(confirm_steps=3),
            run_loop(lambda r: symptoms.NumericalFault(rank=4)),
        ),
        "EVICT",
    )
    ok &= row(
        "every rank, one bad batch",
        "NumericalFault(everywhere)",
        drive(
            NumericalEvaluator(confirm_steps=1),
            run_loop(
                lambda r: symptoms.NumericalFault(
                    rank=r, everywhere=True, only_step=10, bad_value=900.0
                )
            ),
        ),
        "REATTEMPT",
    )
    ok &= row(
        "healthy run",
        "nothing injected",
        drive(
            NumericalEvaluator(confirm_steps=2),
            run_loop(lambda r: symptoms.NumericalFault(rank=-1)),
        ),
        "no action",
    )

    print("\n  Only the first of the four stragglers evicts. The other three are")
    print("  the same symptom with a different cause, and evicting on any of them")
    print("  would take out a healthy node.")
    if not verbose:
        print("  Re-run with --verbose to see why each one was ruled out.\n")
    else:
        print()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(verbose="--verbose" in sys.argv))
