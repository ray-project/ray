#!/usr/bin/env python
"""A slow step is not a hang -- but only the job knows how slow is normal.

    cd release/train_tests/health
    python 03_collective_join.py --no-hang                   # phase 1
    python 03_collective_join.py                             # phases 1 + 2
    python 03_collective_join.py --no-hang --merged-control  # the control

RAS flags a communicator whose op counts are mismatched and not advancing.
That is what a hang looks like -- and also what a *legitimately slow step*
looks like: one rank pauses before the collective (a checkpoint save, a GC
pause, a slow data shard) and its peers wait inside the all-reduce until it
arrives. A single fixed confirm window cannot tell them apart; it is right for
one job's step time and wrong for another's.

The job builds real TP/PP subgroups over 4 ranks and reports its own layout and
step time through ``ray.train.health.report()``:

    tp_rank = rank % 2      pp_rank = rank // 2
    TP groups [0,1] [2,3]   all-reduced every step
    PP groups [0,2] [1,3]   all-reduced every step

  phase 1: every --pause-every steps, --pause-rank sleeps --pause-s before its
           TP all-reduce. RAS sees the TP group mismatched and frozen for the
           whole pause. The threshold is --stall-factor x the reported step
           time, so a pause shorter than that must NOT fire.
  phase 2: --hang-rank leaves its TP all-reduce for good. Must fire, and the
           decision must name the TP group.

``--merged-control`` runs the same job under the merged detector (#64928) with
a fixed confirm window shorter than the pause, instead of the health policy.
That is the false positive the join exists to avoid: it should fire in phase 1.
"""
import argparse
import os
import sys
from pathlib import Path

import ray

sys.path.insert(0, str(Path(__file__).parent))
import nccl_ras_health  # noqa: E402

ray.cloudpickle.register_pickle_by_value(nccl_ras_health)

TALLY = "health_03_tally"


def train_func(config):
    import time

    import torch
    import torch.distributed as dist

    import ray.train
    import ray.train.health as health

    ctx = ray.train.get_context()
    rank, world = ctx.get_world_rank(), ctx.get_world_size()
    tp_rank, pp_rank = rank % 2, rank // 2
    device = torch.device(f"cuda:{torch.cuda.current_device()}")

    # Every rank calls new_group in the same order, members or not.
    tp_groups = [
        dist.new_group([r for r in range(world) if r // 2 == p])
        for p in range(world // 2)
    ]
    pp_groups = [
        dist.new_group([r for r in range(world) if r % 2 == t]) for t in range(2)
    ]
    my_tp, my_pp = tp_groups[pp_rank], pp_groups[tp_rank]
    tensor = torch.ones(256, 256, device=device)

    for step in range(config["steps"]):
        started = time.perf_counter()

        hanging = rank == config["hang_rank"] and step >= config["hang_step"]
        if config["hang"] and hanging:
            print(f"[fault-injection] rank {rank} left its TP all-reduce at {step}")
            time.sleep(3600)  # alive, just never participating again

        pausing = (
            rank == config["pause_rank"]
            and step > 0
            and step % config["pause_every"] == 0
            and not (config["hang"] and step >= config["hang_step"])
        )
        if pausing:
            print(f"[slow-step] rank {rank} pausing {config['pause_s']}s at {step}")
            time.sleep(config["pause_s"])  # peers wait inside the all-reduce

        dist.all_reduce(tensor, group=my_tp)
        dist.all_reduce(tensor, group=my_pp)
        torch.cuda.synchronize()
        time.sleep(config["step_s"])

        health.report(
            {
                "tp_rank": tp_rank,
                "pp_rank": pp_rank,
                # The normal step time; the injected pause is excluded.
                "step_time_s": config["step_s"],
                "last_step_s": time.perf_counter() - started,
            },
            step=step,
        )
        if step % 5 == 0:
            print(f"rank {rank} step {step}", flush=True)


@ray.remote(num_cpus=0)
class Tally:
    """Lets controller-side code tell the driver what it saw."""

    def __init__(self):
        self.counts, self.decisions = {}, []

    def bump(self, key):
        self.counts[key] = self.counts.get(key, 0) + 1

    def record(self, line):
        self.decisions.append(line)

    def get(self):
        return self.counts, self.decisions


def main() -> int:
    import ray.train.health as health
    from ray.train import (
        FailureConfig,
        NCCLHangError,
        RunConfig,
        ScalingConfig,
        UserCallback,
    )
    from ray.train.torch import TorchTrainer

    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--step-s", type=float, default=6.0)
    parser.add_argument("--stall-factor", type=float, default=5.0)
    parser.add_argument("--pause-rank", type=int, default=1)
    parser.add_argument("--pause-every", type=int, default=5)
    parser.add_argument("--pause-s", type=float, default=18.0)
    parser.add_argument("--hang-rank", type=int, default=1)
    parser.add_argument("--hang-step", type=int, default=23)
    parser.add_argument("--poll-s", type=float, default=5.0)
    parser.add_argument("--timeout-s", type=float, default=300.0)
    parser.add_argument("--no-hang", action="store_true")
    parser.add_argument("--merged-control", action="store_true")
    args = parser.parse_args()

    threshold = args.step_s * args.stall_factor
    print(
        f"normal step {args.step_s:.0f}s -> threshold {threshold:.0f}s; "
        f"pause {args.pause_s:.0f}s every {args.pause_every} steps on rank "
        f"{args.pause_rank}"
    )
    if args.pause_s >= threshold:
        print("WARNING: the pause is longer than the threshold; phase 1 will fire.")
    if args.pause_s < 3 * args.poll_s:
        print("WARNING: the pause is too short for a poll to see it frozen.")

    ray.init(address="auto")
    if args.workers % 2:
        print("--workers must be even (tp=2)", file=sys.stderr)
        return 2
    tally = Tally.options(name=TALLY, lifetime="detached").remote()

    class ObserveFrozen(health.Evaluator):
        """Counts frozen-communicator observations, so silence can be trusted."""

        def evaluate(self, state):
            for result in state.results(nccl_ras_health.NcclRasProbe).values():
                if "frozen" in result.events:
                    ray.get_actor(TALLY).bump.remote("frozen")
            return []

    class RecordDecisions(UserCallback):
        def after_health_decision(self, run_context, health_decision):
            ray.get_actor(TALLY).record.remote(
                f"{health_decision.action.name}: {health_decision.reason}"
            )

    run_kwargs = dict(
        worker_runtime_env={"env_vars": {"NCCL_RAS_ENABLE": "1"}},
        failure_config=FailureConfig(max_failures=0),
    )
    if args.merged_control:
        # The merged detector, with a fixed window shorter than the pause.
        window = max(args.poll_s * 2, args.pause_s * 0.6)
        os.environ.update(
            {
                "RAY_TRAIN_ENABLE_NCCL_HANG_DETECTOR": "1",
                "RAY_TRAIN_NCCL_RAS_ACTION": "fail",
                "RAY_TRAIN_NCCL_RAS_MIN_POLL_INTERVAL_S": str(int(args.poll_s)),
                "RAY_TRAIN_NCCL_RAS_CONFIRM_DURATION_S": str(int(window)),
            }
        )
        print(f"CONTROL: merged detector with a fixed {window:.0f}s window")
        run_config = RunConfig(**run_kwargs)
    else:
        policy = nccl_ras_health.collective_hang_policy(
            stall_factor=args.stall_factor,
            min_stall_s=args.poll_s * 2,
            interval_s=args.poll_s,
        )
        observe = health.HealthPolicy(evaluator_creator=lambda: [ObserveFrozen()])
        run_config = RunConfig(
            health_config=health.HealthConfig(policies=[policy, observe]),
            callbacks=[RecordDecisions()],
            **run_kwargs,
        )

    trainer = TorchTrainer(
        train_func,
        train_loop_config={
            "steps": int(args.timeout_s / args.step_s),
            "step_s": args.step_s,
            "pause_rank": args.pause_rank,
            "pause_every": args.pause_every,
            "pause_s": args.pause_s,
            "hang": not args.no_hang,
            "hang_rank": args.hang_rank,
            "hang_step": args.hang_step,
        },
        scaling_config=ScalingConfig(num_workers=args.workers, use_gpu=True),
        run_config=run_config,
    )

    error = None
    try:
        trainer.fit()
    except (health.HealthDecisionError, NCCLHangError) as e:
        error = e
    counts, decisions = ray.get(tally.get.remote())
    ray.kill(tally)
    for line in decisions:
        print(f"  decision: {line}")

    if args.merged_control:
        if isinstance(error, NCCLHangError):
            print(
                "\n  CONTROL CONFIRMED -- the fixed-window detector called a slow\n"
                "  step a hang. That is the false positive the join avoids."
            )
            return 0
        print("\n  CONTROL DID NOT FIRE -- lengthen --pause-s.")
        return 1

    frozen = counts.get("frozen", 0)
    print(f"\nfrozen-communicator observations: {frozen}")
    if args.no_hang:
        if error is not None:
            print("\n  FAIL -- fired on a slow step. That is a false positive.")
            return 1
        if not frozen:
            print(
                "\n  INCONCLUSIVE -- no frozen communicator was ever observed, so the\n"
                "  job never got into the state this checks. Lengthen --pause-s."
            )
            return 1
        print(
            f"\n  PASS -- silent through {frozen} frozen observations: each was a\n"
            "  pause shorter than this job's own stall threshold."
        )
        return 0

    if error is None:
        print("\n  FAIL -- the wedge never produced a decision.")
        return 1
    reason = error.decision.reason
    if "TP group" not in reason:
        print(f"\n  FAIL -- decided, but did not name the TP group:\n  {reason}")
        return 1
    print(f"\n  PASS -- {error.decision.action.name}, naming the TP group.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
