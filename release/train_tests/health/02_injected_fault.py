#!/usr/bin/env python
"""Inject a real hang and check the health loop reacts to it.

    cd release/train_tests/health
    python 02_injected_fault.py            # A: the merged detector
    python 02_injected_fault.py --ported   # B: through ray.train.health

Step 1 proves RAS produces data. This proves something acts on it.

  A. *the merged detector* (ray-project/ray#64928), with no new code. It raises
     NCCLHangError on a confirmed hang. Run this first: if it does not fire,
     the problem is the cluster, not our change.

  B. *the same detection written as a user of ``ray.train.health``*. The
     policy in ``nccl_ras_health.py`` is passed through
     ``RunConfig(health_config=...)`` and run by Ray Train's HealthManager in
     the controller. Expect, in order: "[Health] pre-flight passed on N
     node(s)" before the workers start, a DIAGNOSE (stack dumps, nvidia-smi,
     the RAS text report, at the stalled ranks), then a REATTEMPT that fails
     the run with HealthDecisionError.

The fault is a real one: one rank stops calling the collective while staying
alive, so its op count falls a step behind and every peer blocks inside the
all-reduce.
"""
import argparse
import os
import sys
import time
from pathlib import Path

import ray

sys.path.insert(0, str(Path(__file__).parent))
import nccl_ras_health  # noqa: E402

ray.cloudpickle.register_pickle_by_value(nccl_ras_health)

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

    rank = ray.train.get_context().get_world_rank()
    device = torch.device(f"cuda:{torch.cuda.current_device()}")
    tensor = torch.ones(512, 512, device=device)

    for step in range(config["steps"]):
        if rank == config["hang_rank"] and step >= config["hang_step"]:
            print(
                f"[fault-injection] rank {rank} left the collective at step "
                f"{step} at t={time.time():.3f}",
                flush=True,
            )
            time.sleep(3600)  # alive, so RAS can still see it
        dist.all_reduce(tensor)
        torch.cuda.synchronize()
        if step % 25 == 0:
            print(f"rank {rank} step {step}", flush=True)
        time.sleep(0.05)


def _trainer(args, run_config):
    from ray.train import ScalingConfig
    from ray.train.torch import TorchTrainer

    return TorchTrainer(
        train_func,
        train_loop_config={
            "steps": args.steps,
            "hang_rank": args.hang_rank,
            "hang_step": args.hang_step,
        },
        scaling_config=ScalingConfig(num_workers=args.workers, use_gpu=True),
        run_config=run_config,
    )


# ----------------------------------------------------------------------
# A: the merged detector, as the control
# ----------------------------------------------------------------------
def run_merged_detector(args) -> int:
    from ray.train import FailureConfig, NCCLHangError, RunConfig

    print("=" * 70)
    print("A. merged NCCL RAS detector (#64928) -- no new code")
    print("=" * 70)
    for k, v in RAS_ENV.items():
        os.environ.setdefault(k, v)
        print(f"  {k}={os.environ[k]}")

    trainer = _trainer(
        args,
        RunConfig(
            worker_runtime_env={"env_vars": {"NCCL_RAS_ENABLE": "1"}},
            failure_config=FailureConfig(max_failures=0),
        ),
    )
    started = time.monotonic()
    try:
        trainer.fit()
    except NCCLHangError:
        print(f"\n  PASS -- NCCLHangError after {time.monotonic() - started:.1f}s")
        return 0
    except Exception as e:  # noqa: BLE001
        print(f"\n  FAIL -- {type(e).__name__}: {str(e)[:800]}")
        return 1
    print("\n  FAIL -- training completed; the detector never confirmed.")
    return 1


# ----------------------------------------------------------------------
# B: the same detection, brought by the user through ray.train.health
# ----------------------------------------------------------------------
def run_through_health_config(args) -> int:
    import ray.train.health as health
    from ray.train import FailureConfig, RunConfig, UserCallback

    class PrintDecisions(UserCallback):
        """The REP's after_health_decision hook. Runs on the controller."""

        def after_health_decision(self, run_context, health_decision):
            print(
                f"[after_health_decision] t={time.time():.3f} "
                f"{health_decision.action.name} ({health_decision.cause.name}): "
                f"{health_decision.reason}",
                flush=True,
            )

    print("=" * 70)
    print("B. NCCL RAS as a user-provided HealthPolicy, via RunConfig")
    print("=" * 70)
    print(f"  probe every {args.poll_s:.0f}s, confirm after {args.confirm_s:.0f}s")

    policy = nccl_ras_health.nccl_ras_policy(
        confirm_duration_s=args.confirm_s, interval_s=args.poll_s
    )
    trainer = _trainer(
        args,
        RunConfig(
            health_config=health.HealthConfig(policies=[policy]),
            callbacks=[PrintDecisions()],
            worker_runtime_env={"env_vars": {"NCCL_RAS_ENABLE": "1"}},
            failure_config=FailureConfig(max_failures=0),
        ),
    )
    started = time.monotonic()
    try:
        trainer.fit()
    except health.HealthDecisionError as e:
        decision = e.decision
        print(f"\n  run ended after {time.monotonic() - started:.1f}s")
        print(f"  final decision: {decision.action.name} ({decision.cause.name})")
        print(f"  {decision.reason}")
        print(
            "\n  PASS -- the health loop confirmed the hang and acted on it.\n"
            "  Check the controller log for the DIAGNOSE that preceded it and the\n"
            "  `[Health] diagnostic output:` paths it wrote."
        )
        return 0
    except Exception as e:  # noqa: BLE001
        print(f"\n  FAIL -- {type(e).__name__}: {str(e)[:800]}")
        return 1
    print("\n  FAIL -- training completed; the health loop never decided.")
    return 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--steps", type=int, default=4000)
    parser.add_argument("--hang-rank", type=int, default=1)
    parser.add_argument("--hang-step", type=int, default=100)
    parser.add_argument("--poll-s", type=float, default=5.0)
    parser.add_argument("--confirm-s", type=float, default=20.0)
    parser.add_argument(
        "--ported",
        action="store_true",
        help="Run B (through ray.train.health) instead of A (the merged detector).",
    )
    args = parser.parse_args()

    ray.init(address="auto")
    if not 0 < args.hang_rank < args.workers:
        print("--hang-rank must be in [1, --workers)", file=sys.stderr)
        return 2
    return run_through_health_config(args) if args.ported else run_merged_detector(args)


if __name__ == "__main__":
    sys.exit(main())
