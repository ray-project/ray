#!/usr/bin/env python
"""Inject a real hang and check the health loop reacts to it.

    python release/train_tests/health/02_injected_fault.py            # detector only
    python release/train_tests/health/02_injected_fault.py --ported   # + the REP port

Step 1 proves RAS produces data. This proves something acts on it.

Two things are under test, and they are separable on purpose:

  *the merged detector* (ray-project/ray#64928) runs today with no new code --
  it raises NCCLHangError on a confirmed hang, which the failure policy turns
  into a retry. Run this first: if it does not fire, the problem is the
  cluster, not our change.

  *the ported policy* (--ported) feeds the same RAS reports through
  NcclRasProbe / NcclHangEvaluator and prints the HealthDecision. This is the
  migration's acceptance criterion: same detection, same timing, better action.

The fault is a real one -- one rank stops calling the collective while staying
alive, so its op count falls a step behind and every peer blocks inside the
all-reduce. No NVRx needed; it does not install on this image.
"""
import argparse
import os
import subprocess
import sys
import threading
import time

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

# Fast enough to iterate on. The 600s default confirm window is not.
RAS_ENV = {
    "NCCL_RAS_ENABLE": "1",
    "RAY_TRAIN_ENABLE_NCCL_HANG_DETECTOR": "1",
    "RAY_TRAIN_NCCL_RAS_ACTION": "fail",
    "RAY_TRAIN_NCCL_RAS_MIN_POLL_INTERVAL_S": "2",
    "RAY_TRAIN_NCCL_RAS_CONFIRM_DURATION_S": "20",
}


def train_func(config):
    """All-reduce loop where one rank walks away from the collective."""
    import time

    import torch
    import torch.distributed as dist

    import ray.train
    from ray.train.v2._internal.execution.health.testing import symptoms

    rank = ray.train.get_context().get_world_rank()
    device = torch.device(f"cuda:{torch.cuda.current_device()}")
    tensor = torch.ones(512, 512, device=device)

    desync = symptoms.CollectiveDesync(
        rank=config["hang_rank"], start_step=config["hang_step"]
    )
    straggler = symptoms.Straggler(
        rank=config["hang_rank"], slowdown=1.0  # timing only, no slowdown
    )

    for step in range(config["steps"]):
        with straggler.step(step):
            if desync.maybe_skip(step):
                print(
                    f"[fault-injection] rank {rank} left the collective at step "
                    f"{step} at t={time.time():.3f}",
                    flush=True,
                )
                time.sleep(3600)
            dist.all_reduce(tensor)
            torch.cuda.synchronize()
        if step % 25 == 0:
            print(f"rank {rank} step {step}", flush=True)
        time.sleep(0.05)


# ----------------------------------------------------------------------
# Part A: does the merged detector fire?
# ----------------------------------------------------------------------
def run_merged_detector(args) -> int:
    from ray.train import FailureConfig, RunConfig, ScalingConfig
    from ray.train.torch import TorchTrainer
    from ray.train.v2.api.exceptions import NCCLHangError

    print("=" * 70)
    print("A. merged NCCL RAS detector (#64928) -- no new code")
    print("=" * 70)
    for k, v in RAS_ENV.items():
        os.environ.setdefault(k, v)
        print(f"  {k}={os.environ[k]}")

    trainer = TorchTrainer(
        train_func,
        train_loop_config={
            "steps": args.steps,
            "hang_rank": args.hang_rank,
            "hang_step": args.hang_step,
        },
        scaling_config=ScalingConfig(num_workers=args.workers, use_gpu=True),
        run_config=RunConfig(
            worker_runtime_env={"env_vars": {"NCCL_RAS_ENABLE": "1"}},
            # Fail on the first hang instead of retrying into a second one.
            failure_config=FailureConfig(max_failures=0),
        ),
    )

    started = time.monotonic()
    try:
        trainer.fit()
    except Exception as e:  # noqa: BLE001
        elapsed = time.monotonic() - started
        root = e
        while getattr(root, "__cause__", None) is not None:
            root = root.__cause__
        is_hang = isinstance(e, NCCLHangError) or "NCCLHang" in repr(e)
        print(f"\n  run ended after {elapsed:.1f}s")
        print(f"  error type: {type(e).__name__}")
        if is_hang:
            print("\n  PASS -- the detector attributed this to a NCCL hang.")
            print("  Record the interval from the [fault-injection] log line to")
            print("  the first detector warning. That number is the whole point.")
            return 0
        print("\n  FAIL -- the run failed, but not as a detected hang:")
        print(f"  {str(e)[:800]}")
        return 1

    print("\n  FAIL -- training completed. The injected rank never wedged, or")
    print("  the detector never confirmed. Check for `unsupported_f_option` in")
    print("  the controller log, which means ncclras is too old.")
    return 1


# ----------------------------------------------------------------------
# Part B: does the ported policy reach the same conclusion?
# ----------------------------------------------------------------------
@ray.remote(num_cpus=0)
def _ras_json():
    p = subprocess.run(
        "ncclras -f json -t 5", shell=True, capture_output=True, text=True
    )
    return p.returncode, p.stdout


def run_ported_policy(args) -> int:
    """Drive NcclRasProbe/NcclHangEvaluator off the same live job."""
    from ray.train import RunConfig, ScalingConfig
    from ray.train.torch import TorchTrainer
    from ray.train.v2._internal.callbacks.nccl_ras import parse_ras_schema
    from ray.train.v2._internal.execution.health import HealthManager, WorkerHealth
    from ray.train.v2._internal.execution.health.adapters.nccl_ras_policy import (
        NcclRasProbe,
        nccl_ras_policy,
    )

    print("\n" + "=" * 70)
    print("B. ported HealthPolicy -- the same reports, through the REP contracts")
    print("=" * 70)

    trainer = TorchTrainer(
        train_func,
        train_loop_config={
            "steps": args.steps,
            "hang_rank": args.hang_rank,
            "hang_step": args.hang_step,
        },
        scaling_config=ScalingConfig(num_workers=args.workers, use_gpu=True),
        run_config=RunConfig(
            worker_runtime_env={"env_vars": {"NCCL_RAS_ENABLE": "1"}},
        ),
    )
    threading.Thread(target=trainer.fit, daemon=True).start()

    gpu_nodes = [
        n for n in ray.nodes() if n["Alive"] and n["Resources"].get("GPU", 0) > 0
    ]

    def query():
        """Latest RAS report from any node that answers."""
        for node in gpu_nodes:
            try:
                rc, out = ray.get(
                    _ras_json.options(
                        scheduling_strategy=NodeAffinitySchedulingStrategy(
                            node_id=node["NodeID"], soft=False
                        )
                    ).remote(),
                    timeout=30,
                )
            except Exception:  # noqa: BLE001
                continue
            if rc == 0 and out.strip():
                report = parse_ras_schema(out)
                if report is not None:
                    return report
        return None

    manager = HealthManager(
        [nccl_ras_policy(query=query, confirm_duration_s=args.confirm_s)]
    )

    print(f"  polling every {args.poll_s:.0f}s, confirming after {args.confirm_s:.0f}s")
    started = time.monotonic()
    saw_frozen_at = None

    while time.monotonic() - started < args.timeout_s:
        # The ranks are all on GPU nodes; the evaluator needs the rank->node map
        # to target a Diagnose, so feed it what the trainer is using.
        for rank in range(args.workers):
            node_id = gpu_nodes[rank % len(gpu_nodes)]["NodeID"]
            manager.ingest_worker_health(WorkerHealth(rank, node_id, time.time()))

        manager.run_cluster_probes([n["NodeID"] for n in gpu_nodes])
        state = manager.build_state()
        comms = state.results(NcclRasProbe)
        frozen = [c for c, r in comms.items() if "frozen" in r.events]
        elapsed = time.monotonic() - started
        if frozen and saw_frozen_at is None:
            saw_frozen_at = elapsed
            print(f"  t={elapsed:6.1f}s  first frozen communicator: {frozen}")
        elif comms:
            print(f"  t={elapsed:6.1f}s  {len(comms)} comm(s), frozen={frozen}")

        decision = manager.poll_decision()
        if decision is not None:
            print(
                f"\n  t={elapsed:6.1f}s  DECISION: {decision.action.name} "
                f"({decision.cause.name})"
            )
            print(f"  {decision.reason}")
            if getattr(decision, "on_demand_probes", None):
                names = [p.probe_name() for p in decision.on_demand_probes]
                print(f"  probes requested: {names}")
            print(
                f"\n  PASS -- detected at t={saw_frozen_at:.1f}s, "
                f"decided at t={elapsed:.1f}s"
            )
            return 0

        time.sleep(args.poll_s)

    print(f"\n  FAIL -- no decision within {args.timeout_s:.0f}s")
    if saw_frozen_at is None:
        print("  No communicator was ever seen frozen. Either the rank did not")
        print("  wedge, or RAS is not reporting -- run 01_nccl_ras.py.")
    return 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--steps", type=int, default=4000)
    parser.add_argument("--hang-rank", type=int, default=1)
    parser.add_argument("--hang-step", type=int, default=100)
    parser.add_argument("--poll-s", type=float, default=5.0)
    parser.add_argument("--confirm-s", type=float, default=20.0)
    parser.add_argument("--timeout-s", type=float, default=600.0)
    parser.add_argument(
        "--ported",
        action="store_true",
        help="Run part B (the REP port) instead of part A (the merged detector).",
    )
    args = parser.parse_args()

    ray.init(address="auto")
    if args.hang_rank >= args.workers:
        print(f"--hang-rank must be < --workers ({args.workers})", file=sys.stderr)
        return 2
    if args.hang_rank == 0:
        print(
            "WARNING: wedging rank 0 removes the healthy peer the checks compare\n"
            "         against. Prefer a non-zero rank.",
            file=sys.stderr,
        )

    return run_ported_policy(args) if args.ported else run_merged_detector(args)


if __name__ == "__main__":
    sys.exit(main())
