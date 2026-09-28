#!/usr/bin/env python
"""The merged NCCL RAS detector, then the same detection on ``ray.train.health``.

    python release/train_tests/health/nccl_hang.py           # both, compared
    python release/train_tests/health/nccl_hang.py merged    # one of them
    python release/train_tests/health/nccl_hang.py health

Both runs get the same job, the same fault (``harness.LeaveCollective``) and
the same timing: RAS polled every 2s, a hang confirmed after 20s without
progress. The merged detector's defaults are 15s and 600s.

  merged  ``RAY_TRAIN_ENABLE_NCCL_HANG_DETECTOR=1`` turns on
          ``NCCLRASCallback`` (#64928). On a confirmed hang it writes stack
          traces and the RAS query history, and raises ``NCCLHangError``.
  health  ``nccl_ras_policy()`` from ``nccl_ras_health.py``, passed through
          ``RunConfig(health_config=...)``. Pre-flight on every node, then on a
          confirmed hang a DIAGNOSE (stacks, nvidia-smi, RAS text report), then
          a REATTEMPT. With ``max_failures=0`` the failure policy ends the run
          with ``HealthDecisionError``.

Pass: both detect the hang, within a few seconds of each other.
"""
import os
import sys
import time
from pathlib import Path

import ray

sys.path.insert(0, str(Path(__file__).parent))
import harness  # noqa: E402
import nccl_ras_health  # noqa: E402

harness.ship_by_value(harness, nccl_ras_health)

WORKERS = 4
POLL_S = 2
CONFIRM_S = 20
FAULT = harness.LeaveCollective(rank=1, at_step=100)

MERGED_ENV = {
    "RAY_TRAIN_ENABLE_NCCL_HANG_DETECTOR": "1",
    "RAY_TRAIN_NCCL_RAS_ACTION": "fail",
    "RAY_TRAIN_NCCL_RAS_MIN_POLL_INTERVAL_S": str(POLL_S),
    "RAY_TRAIN_NCCL_RAS_CONFIRM_DURATION_S": str(CONFIRM_S),
}


def train_func():
    import time

    import torch
    import torch.distributed as dist

    import ray.train

    rank = ray.train.get_context().get_world_rank()
    tensor = torch.ones(512, 512, device=f"cuda:{torch.cuda.current_device()}")
    for step in range(100_000):
        FAULT.maybe_inject(rank, step)
        dist.all_reduce(tensor)
        torch.cuda.synchronize()
        time.sleep(0.05)


def run(mode: str) -> dict:
    import ray.train.health as health
    from ray.train import FailureConfig, NCCLHangError, RunConfig, ScalingConfig
    from ray.train.torch import TorchTrainer

    harness.banner(
        f"{mode}: " + ("NCCLRASCallback" if mode == "merged" else "ray.train.health"),
        FAULT,
    )
    for key, value in MERGED_ENV.items():
        if mode == "merged":
            os.environ[key] = value
        else:
            os.environ.pop(key, None)

    name = f"nccl_hang_{mode}_{int(time.time())}"
    health_config = None
    if mode == "health":
        policy = nccl_ras_health.nccl_ras_policy(
            confirm_duration_s=CONFIRM_S, interval_s=POLL_S
        )
        health_config = health.HealthConfig(policies=[policy])

    events = harness.start_events()
    trainer = TorchTrainer(
        train_func,
        scaling_config=ScalingConfig(num_workers=WORKERS, use_gpu=True),
        run_config=RunConfig(
            name=name,
            storage_path=harness.storage_path(),
            health_config=health_config,
            callbacks=[harness.Timeline()],
            failure_config=FailureConfig(max_failures=0),
        ),
    )
    error = None
    try:
        trainer.fit()
    except (NCCLHangError, health.HealthDecisionError) as e:
        error = e
    timeline = ray.get(events.get.remote())
    ray.kill(events)

    harness.print_timeline(timeline, "timeline (seconds after the fault fired):")
    storage = harness.storage_path()
    if storage:
        print("\n  artifacts:")
        harness.list_artifacts(
            os.path.join(storage, name), ["hang_detector", "health_diagnostics"]
        )
    else:
        print(f"\n  artifacts: not listed; {harness.SHARED_STORAGE} is not mounted")

    detected = "DIAGNOSE" if mode == "health" else "SHUTTINGDOWN"
    return {
        "error": type(error).__name__ if error else None,
        "detected_s": harness.seconds_to(timeline, {detected, "REATTEMPT"}),
    }


def main() -> int:
    modes = sys.argv[1:] or ["merged", "health"]
    if set(modes) - {"merged", "health"}:
        print("usage: nccl_hang.py [merged] [health]")
        return 2

    ray.init(address="auto")
    results = {mode: run(mode) for mode in modes}

    expected = {"merged": "NCCLHangError", "health": "HealthDecisionError"}
    print("\n" + "=" * 78)
    ok = True
    for mode, r in results.items():
        seconds = f"{r['detected_s']:.1f}s" if r["detected_s"] is not None else "never"
        print(f"  {mode:<7} detected after {seconds:<8} ended with {r['error']}")
        ok &= r["error"] == expected[mode] and r["detected_s"] is not None
    if len(results) == 2 and ok:
        gap = results["health"]["detected_s"] - results["merged"]["detected_s"]
        print(f"  health minus merged: {gap:+.1f}s")
        ok &= abs(gap) <= 2 * POLL_S + 1
    print(f"\n  {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
