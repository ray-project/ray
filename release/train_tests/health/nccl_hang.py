#!/usr/bin/env python
"""One NCCL hang, caught by the NCCL hang detector on ``ray.train.health`` and
by the ``NCCLRASCallback`` it replaces, on the same job.

    python nccl_hang.py                    # callback, then health, compared
    python nccl_hang.py health             # one of them
    python nccl_hang.py health slow_step   # a slow step, which must not fire

Both runs train the same job with the same fault and timing: RAS polled every
2s, and a hang confirmed after 20s without progress (the defaults are 15s and
600s), with ``RAY_TRAIN_NCCL_RAS_ACTION=fail``.

  health    ``RAY_TRAIN_ENABLE_NCCL_HANG_DETECTOR=1``: the controller adds the
            detector's check to the run. A confirmed hang returns a
            ``Diagnose`` that uploads the diagnostics to ``hang_detector/``,
            then a ``Reattempt``, which with ``max_failures=0`` ends the run
            with ``HealthCheckError``.
  callback  ``NCCLRASCallback``, which the env var no longer turns on, passed
            in ``RunConfig(callbacks=...)``. On a confirmed hang it uploads the
            same diagnostics, one after another, and raises ``NCCLHangError``.

Pass: both end the run with their error within a few seconds of each other,
both upload the four diagnostics, and neither fires on the slow step.
"""
import os
import sys
import time
from pathlib import Path
from typing import List, Optional

import ray

sys.path.insert(0, str(Path(__file__).parent))
import gpu_health  # noqa: E402
import harness  # noqa: E402

harness.ship_by_value(gpu_health, harness)

POLL_S = 2
CONFIRM_S = 20
FAULTS = {
    "hang": harness.LeaveCollective(rank=1, at_step=100),
    "slow_step": harness.SlowStep(rank=1, every=40, pause_s=CONFIRM_S / 2),
}
ENABLE_ENV_VAR = "RAY_TRAIN_ENABLE_NCCL_HANG_DETECTOR"
DETECTOR_ENV = {
    "RAY_TRAIN_NCCL_RAS_ACTION": "fail",
    "RAY_TRAIN_NCCL_RAS_MIN_POLL_INTERVAL_S": str(POLL_S),
    "RAY_TRAIN_NCCL_RAS_CONFIRM_DURATION_S": str(CONFIRM_S),
}
EXPECTED_ERROR = {"health": "HealthCheckError", "callback": "NCCLHangError"}
# Both upload to `<storage>/<run name>/hang_detector/<tool>/`.
DIAGNOSTICS = ("flight_recorder", "nvidia_smi", "nccl_ras", "stack_traces")


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


def missing_diagnostics(run_dir: str) -> List[str]:
    """The diagnostics with no file under ``run_dir/hang_detector/``."""
    root = os.path.join(run_dir, "hang_detector")
    return [
        tool
        for tool in DIAGNOSTICS
        if not any(files for _, _, files in os.walk(os.path.join(root, tool)))
    ]


def run(mode: str, fault_name: str, workers: int) -> dict:
    from ray.train import FailureConfig, RunConfig, ScalingConfig
    from ray.train.torch import TorchTrainer
    from ray.train.v2._internal.callbacks.nccl_ras import NCCLRASCallback
    from ray.train.v2.api.exceptions import TrainingFailedError

    fault = FAULTS[fault_name]
    harness.banner(f"{mode}, {fault_name}", fault)
    # The controller reads these, as Ray Train propagates them from the driver.
    os.environ.update(DETECTOR_ENV)
    if mode == "health":
        os.environ[ENABLE_ENV_VAR] = "1"
    else:
        os.environ.pop(ENABLE_ENV_VAR, None)
    callbacks = [harness.Timeline()]
    if mode == "callback":
        # Reads DETECTOR_ENV here, on the driver.
        callbacks.append(NCCLRASCallback())

    name = f"nccl_{fault_name}_{mode}_{int(time.time())}"
    storage = harness.storage_path()
    events = harness.start_events()
    trainer = TorchTrainer(
        make_train_func(fault, steps=100_000 if fault_name == "hang" else 400),
        scaling_config=ScalingConfig(num_workers=workers, use_gpu=True),
        run_config=RunConfig(
            name=name,
            storage_path=storage,
            callbacks=callbacks,
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
    missing: Optional[List[str]] = None
    if storage:
        run_dir = os.path.join(storage, name)
        print("\n  artifacts:")
        harness.list_artifacts(run_dir)
        missing = missing_diagnostics(run_dir)
    # The health detector records its decisions; the callback only the error
    # it raises, which shuts the run down.
    return {
        "error": type(error).__name__ if error else None,
        "confirmed_s": harness.seconds_to(
            timeline, {"DIAGNOSE"}, prefix="NCCL hang confirmed"
        ),
        "ended_s": harness.seconds_to(
            timeline, {"REATTEMPT"} if mode == "health" else {"SHUTTINGDOWN", "ERRORED"}
        ),
        "missing": missing,
    }


def main() -> int:
    args = sys.argv[1:]
    modes = [a for a in args if a in ("callback", "health")] or ["callback", "health"]
    faults = [a for a in args if a in FAULTS] or ["hang"]
    workers, _ = harness.connect(
        workers=harness.workers_from_env(), runtime_env=harness.nccl_runtime_env()
    )
    results = {(m, f): run(m, f, workers) for f in faults for m in modes}

    def seconds(value: Optional[float]) -> str:
        return f"{value:.1f}s" if value is not None else "never"

    print("\n" + "=" * 78)
    ok = True
    for (mode, fault), r in results.items():
        print(
            f"  {mode:<8} {fault:<9} confirmed {seconds(r['confirmed_s']):<7} "
            f"ended {seconds(r['ended_s']):<7} with {r['error']}"
        )
        if fault == "hang":
            ok &= r["error"] == EXPECTED_ERROR[mode] and r["ended_s"] is not None
            if mode == "health":
                ok &= r["confirmed_s"] is not None
            if r["missing"] is None:
                print("           no shared storage: diagnostics not checked")
            elif r["missing"]:
                print(f"           missing diagnostics: {r['missing']}")
                ok = False
        else:
            ok &= r["error"] is None and r["confirmed_s"] is None
    hang = [results.get((m, "hang")) for m in ("callback", "health")]
    if all(hang) and all(r["ended_s"] is not None for r in hang):
        gap = hang[1]["ended_s"] - hang[0]["ended_s"]
        print(f"  health minus callback, to the end of the run: {gap:+.1f}s")
        ok &= abs(gap) <= 2 * POLL_S + 4
    print(f"\n  {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
