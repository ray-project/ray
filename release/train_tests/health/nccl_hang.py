#!/usr/bin/env python
"""One NCCL hang, caught by the merged ``NCCLRASCallback`` and by the same
detection on ``ray.train.health``.

    python nccl_hang.py                  # merged, then health, compared
    python nccl_hang.py health           # one of them
    python nccl_hang.py health slow_step # a slow step, which must not fire

Both runs train the same job with the same fault and timing: RAS polled every
2s, a hang confirmed after 20s without progress (the defaults are 15s and
600s).

  merged  ``RAY_TRAIN_ENABLE_NCCL_HANG_DETECTOR=1`` turns on
          ``NCCLRASCallback``. On a confirmed hang it writes the diagnostics
          to ``hang_detector/`` and raises ``NCCLHangError``.
  health  ``nccl_ras_health.nccl_ras_preflight_check()`` and
          ``nccl_hang_check()`` in ``RunConfig(health_config=...)``. On a
          confirmed hang a ``Diagnose`` captures the diagnostics under
          ``health_diagnostics/``, then reattempts, which with
          ``max_failures=0`` ends the run with ``HealthCheckError``.

Pass: both catch the hang within a few seconds of each other, and neither
fires on the slow step.
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

POLL_S = 2
CONFIRM_S = 20
FAULTS = {
    "hang": harness.LeaveCollective(rank=1, at_step=100),
    "slow_step": harness.SlowStep(rank=1, every=40, pause_s=CONFIRM_S / 2),
}
MERGED_ENV = {
    "RAY_TRAIN_ENABLE_NCCL_HANG_DETECTOR": "1",
    "RAY_TRAIN_NCCL_RAS_ACTION": "fail",
    "RAY_TRAIN_NCCL_RAS_MIN_POLL_INTERVAL_S": str(POLL_S),
    "RAY_TRAIN_NCCL_RAS_CONFIRM_DURATION_S": str(CONFIRM_S),
}


def make_train_func(fault, steps: int):
    def train_func():
        import time

        import torch
        import torch.distributed as dist

        import ray.train

        rank = ray.train.get_context().get_world_rank()
        tensor = torch.ones(512, 512, device=f"cuda:{torch.cuda.current_device()}")
        for step in range(steps):
            fault.maybe_inject(rank, step)
            dist.all_reduce(tensor)
            torch.cuda.synchronize()
            time.sleep(0.05)

    return train_func


def run(mode: str, fault_name: str, workers: int) -> dict:
    from ray.train import FailureConfig, RunConfig, ScalingConfig
    from ray.train.health import HealthConfig
    from ray.train.torch import TorchTrainer
    from ray.train.v2.api.exceptions import TrainingFailedError

    fault = FAULTS[fault_name]
    harness.banner(f"{mode}, {fault_name}", fault)
    for key, value in MERGED_ENV.items():
        if mode == "merged":
            os.environ[key] = value
        else:
            os.environ.pop(key, None)

    name = f"nccl_{fault_name}_{mode}_{int(time.time())}"
    storage = harness.storage_path()
    health_config = None
    if mode == "health":
        artifacts = (
            os.path.join(storage, name, "health_diagnostics") if storage else None
        )
        health_config = HealthConfig(
            preflight_checks=[nccl_ras_health.nccl_ras_preflight_check()],
            inflight_checks=[
                nccl_ras_health.nccl_hang_check(POLL_S, CONFIRM_S, "fail", artifacts)
            ],
        )

    events = harness.start_events()
    trainer = TorchTrainer(
        make_train_func(fault, steps=100_000 if fault_name == "hang" else 400),
        scaling_config=ScalingConfig(num_workers=workers, use_gpu=True),
        run_config=RunConfig(
            name=name,
            storage_path=storage,
            health_config=health_config,
            callbacks=[harness.Timeline()],
            failure_config=FailureConfig(max_failures=0),
        ),
    )
    error = None
    try:
        trainer.fit()
    except TrainingFailedError as e:
        error = e
    timeline = ray.get(events.get.remote())
    ray.kill(events)

    harness.print_timeline(timeline, "timeline (seconds after the fault fired):")
    if storage:
        print("\n  artifacts:")
        harness.list_artifacts(os.path.join(storage, name))
    detected = {"DIAGNOSE", "SHUTTINGDOWN", "RESTARTING", "REATTEMPT"}
    return {
        "error": type(error).__name__ if error else None,
        "detected_s": harness.seconds_to(timeline, detected),
    }


def main() -> int:
    args = sys.argv[1:]
    modes = [a for a in args if a in ("merged", "health")] or ["merged", "health"]
    faults = [a for a in args if a in FAULTS] or ["hang"]
    workers, _ = harness.connect(
        workers=harness.workers_from_env(), runtime_env=harness.nccl_runtime_env()
    )
    results = {(m, f): run(m, f, workers) for f in faults for m in modes}

    print("\n" + "=" * 78)
    ok = True
    for (mode, fault), r in results.items():
        seconds = f"{r['detected_s']:.1f}s" if r["detected_s"] is not None else "never"
        print(
            f"  {mode:<7} {fault:<9} detected after {seconds:<8} ended with {r['error']}"
        )
        if fault == "hang":
            ok &= r["error"] is not None and r["detected_s"] is not None
        else:
            ok &= r["error"] is None
    hang = [results.get((m, "hang")) for m in ("merged", "health")]
    if all(hang) and all(r["detected_s"] is not None for r in hang):
        gap = hang[1]["detected_s"] - hang[0]["detected_s"]
        print(f"  health minus merged: {gap:+.1f}s")
        ok &= abs(gap) <= 2 * POLL_S + 2
    print(f"\n  {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
