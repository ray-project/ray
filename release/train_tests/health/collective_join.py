#!/usr/bin/env python
"""NCCL RAS joined with what the training loop reports: a slow step is not a hang.

    python release/train_tests/health/collective_join.py                  # all three
    python release/train_tests/health/collective_join.py slow_step        # one of them

RAS flags a communicator that is mismatched and not advancing. A hang looks
like that, and so does a legitimately slow step: one rank pauses before the
collective (a checkpoint save, a GC pause) while its peers wait inside the
all-reduce. A fixed confirm window cannot tell them apart; it is right for one
job's step time and wrong for another's.

The job builds real TP and PP subgroups over 4 ranks and reports, through
``ray.train.health.report()``, its TP/PP coordinates and its normal step time.
``CollectiveHangEvaluator`` joins those with the RAS probe:

- the stall threshold is ``STALL_FACTOR x`` the reported step time (30s here),
- a frozen communicator is named by the ranks' reported coordinates,
- a confirmed hang is diagnosed on its nodes (stacks, ``nvidia-smi``) before
  anything is decided.

Scenarios:

  slow_step         SlowStep, our policy: must stay silent, though RAS sees
                    frozen communicators on every pause.
  slow_step_merged  the same SlowStep under the merged detector with a fixed
                    10s window: fires. The false positive the join avoids.
  wedge             LeaveCollective, our policy: must fire, and must name the
                    TP group.
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
STEP_S = 6.0
STALL_FACTOR = 5.0
THRESHOLD_S = STEP_S * STALL_FACTOR
MERGED_WINDOW_S = 10

SCENARIOS = {
    "slow_step": (harness.SlowStep(rank=1, every=5, pause_s=18), 16, "health"),
    "slow_step_merged": (harness.SlowStep(rank=1, every=5, pause_s=18), 16, "merged"),
    "wedge": (harness.LeaveCollective(rank=1, at_step=6), 60, "health"),
}


def train_func(config):
    import time

    import torch
    import torch.distributed as dist

    import ray.train
    import ray.train.health as health

    fault = config["fault"]
    ctx = ray.train.get_context()
    rank, world = ctx.get_world_rank(), ctx.get_world_size()
    tp_rank, pp_rank = rank % 2, rank // 2

    # tp=2, pp=2: TP groups [0,1] [2,3], PP groups [0,2] [1,3]. Every rank
    # creates every group, in the same order.
    tp_groups = [dist.new_group([p * 2, p * 2 + 1]) for p in range(world // 2)]
    pp_groups = [dist.new_group(list(range(t, world, 2))) for t in range(2)]
    # Real jobs touch the default group; this creates the world communicator
    # the RAS probe translates subgroup ranks against.
    dist.barrier()
    tensor = torch.ones(256, 256, device=f"cuda:{torch.cuda.current_device()}")

    for step in range(config["steps"]):
        fault.maybe_inject(rank, step)
        dist.all_reduce(tensor, group=tp_groups[pp_rank])
        dist.all_reduce(tensor, group=pp_groups[tp_rank])
        torch.cuda.synchronize()
        time.sleep(STEP_S)
        health.report(
            {"tp_rank": tp_rank, "pp_rank": pp_rank, "step_time_s": STEP_S},
            step=step,
        )


def run(name: str) -> bool:
    import ray.train.health as health
    from ray.train import FailureConfig, NCCLHangError, RunConfig, ScalingConfig
    from ray.train.torch import TorchTrainer

    fault, steps, mode = SCENARIOS[name]
    harness.banner(name, fault)
    print(f"  threshold: {STALL_FACTOR:g} x {STEP_S:.0f}s step = {THRESHOLD_S:.0f}s")

    class ObserveFrozen(health.Evaluator):
        """Records each frozen episode, so silence can be told from blindness."""

        def __init__(self):
            self._since = {}

        def evaluate(self, state):
            now = time.monotonic()
            results = state.results(nccl_ras_health.NcclRasProbe)
            frozen = {c: r for c, r in results.items() if "frozen" in r.events}
            for comm_id in list(self._since):
                if comm_id not in frozen:
                    ranks, since = self._since.pop(comm_id)
                    harness.record("FROZEN", f"ranks {ranks} for {now - since:.0f}s")
            for comm_id, result in frozen.items():
                ranks = sorted(int(r) for r in result.devices)
                self._since.setdefault(comm_id, (ranks, now))
            return []

    merged_env = {
        "RAY_TRAIN_ENABLE_NCCL_HANG_DETECTOR": "1",
        "RAY_TRAIN_NCCL_RAS_ACTION": "fail",
        "RAY_TRAIN_NCCL_RAS_MIN_POLL_INTERVAL_S": str(POLL_S),
        "RAY_TRAIN_NCCL_RAS_CONFIRM_DURATION_S": str(MERGED_WINDOW_S),
    }
    for key, value in merged_env.items():
        if mode == "merged":
            os.environ[key] = value
        else:
            os.environ.pop(key, None)

    health_config = None
    if mode == "health":
        policy = nccl_ras_health.collective_hang_policy(
            stall_factor=STALL_FACTOR, min_stall_s=2 * POLL_S, interval_s=POLL_S
        )
        observe = health.HealthPolicy(evaluator_creator=lambda: [ObserveFrozen()])
        health_config = health.HealthConfig(
            policies=[nccl_ras_health.nccl_ras_ready_policy(), policy, observe]
        )
    else:
        print(f"  merged detector with a fixed {MERGED_WINDOW_S}s window")

    events = harness.start_events()
    trainer = TorchTrainer(
        train_func,
        train_loop_config={"fault": fault, "steps": steps},
        scaling_config=ScalingConfig(num_workers=WORKERS, use_gpu=True),
        run_config=RunConfig(
            name=f"collective_join_{name}_{int(time.time())}",
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
    harness.print_timeline(timeline, "timeline (seconds after the first fault):")

    decisions = [d for _, kind, d in timeline if kind in ("DIAGNOSE", "REATTEMPT")]
    frozen = [d for _, kind, d in timeline if kind == "FROZEN"]
    if name == "slow_step":
        ok = error is None and not decisions and bool(frozen)
        why = (
            f"silent through {len(frozen)} frozen episodes"
            if ok
            else "fired on a slow step"
            if decisions or error
            else "RAS never saw a frozen communicator, so nothing was tested"
        )
    elif name == "slow_step_merged":
        ok = isinstance(error, NCCLHangError)
        why = "the fixed window called a slow step a hang" if ok else "did not fire"
    else:
        named = any("TP group, ranks [0, 1]" in d for d in decisions)
        ok = isinstance(error, health.HealthDecisionError) and named
        why = "fired and named TP group [0, 1]" if ok else "did not fire or name it"
    print(f"\n  {'PASS' if ok else 'FAIL'}: {why}")
    return ok


def main() -> int:
    chosen = sys.argv[1:] or list(SCENARIOS)
    if set(chosen) - set(SCENARIOS):
        print(f"usage: collective_join.py [{' | '.join(SCENARIOS)}]")
        return 2
    ray.init(address="auto")
    results = {name: run(name) for name in chosen}
    print("\n" + "=" * 78)
    for name, ok in results.items():
        print(f"  {name:<18} {'PASS' if ok else 'FAIL'}")
    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
