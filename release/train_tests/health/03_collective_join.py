#!/usr/bin/env python
"""The RAS x health.report() join, on real communicators.

    python release/train_tests/health/03_collective_join.py

Steps 01 and 02 prove RAS emits data and that something reacts to a hang. This
proves the part a RAS-only detector cannot do: telling a communicator that
froze *because the job is stuck* from one that froze *because it idles by
design*.

The job builds real NCCL subgroups over 4 ranks:

    tp_rank = rank % 2      pp_rank = rank // 2

    "TP" groups  [0,1] [2,3]   all-reduced every step
    "PP" groups  [0,2] [1,3]   all-reduced every --pp-every steps

So for most of the run the PP communicators genuinely have frozen op counts
while the job is perfectly healthy. That is the false positive a fixed-timeout
detector produces, reproduced rather than simulated. Then one rank walks away
from its TP all-reduce, and the job actually stops.

Expected: no decision during phase 1 despite frozen PP communicators, and a
decision within seconds of the TP wedge in phase 2.
"""
import argparse
import subprocess
import sys
import threading
import time

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


def train_func(config):
    import time

    import torch
    import torch.distributed as dist

    import ray.train
    from ray.train.v2._internal.execution.health.report import report

    ctx = ray.train.get_context()
    rank, world = ctx.get_world_rank(), ctx.get_world_size()
    tp_rank, pp_rank = rank % 2, rank // 2
    device = torch.device(f"cuda:{torch.cuda.current_device()}")

    # Every rank must call new_group in the same order, including ranks that
    # are not members -- that is the collective contract, not a nicety.
    tp_groups = [
        dist.new_group([r for r in range(world) if r // 2 == p])
        for p in range(world // 2)
    ]
    pp_groups = [
        dist.new_group([r for r in range(world) if r % 2 == t]) for t in range(2)
    ]
    my_tp, my_pp = tp_groups[pp_rank], pp_groups[tp_rank]

    print(
        f"rank {rank}: tp_rank={tp_rank} pp_rank={pp_rank} "
        f"(TP peers {[r for r in range(world) if r // 2 == pp_rank]}, "
        f"PP peers {[r for r in range(world) if r % 2 == tp_rank]})",
        flush=True,
    )

    tensor = torch.ones(256, 256, device=device)
    wedged = False

    for step in range(config["steps"]):
        started = time.perf_counter()

        wedge_me = rank == config["hang_rank"] and step >= config["hang_step"]
        if config["hang"] and wedge_me:
            if not wedged:
                wedged = True
                print(
                    f"[fault-injection] rank {rank} leaving its TP all-reduce at "
                    f"step {step}, t={time.time():.3f}",
                    flush=True,
                )
            # Stay alive so RAS can still see this rank; just stop participating.
            time.sleep(3600)

        dist.all_reduce(tensor, group=my_tp)

        # The PP group is only used occasionally, so its op counts sit frozen
        # in between -- which is exactly what a bubble looks like to RAS.
        if step % config["pp_every"] == 0:
            dist.all_reduce(tensor, group=my_pp)

        torch.cuda.synchronize()
        dt = time.perf_counter() - started

        report(
            {
                "step": step,
                "step_time_s": dt,
                "tp_rank": tp_rank,
                "pp_rank": pp_rank,
            },
            step=step,
        )
        if step % 25 == 0:
            print(f"rank {rank} step {step} ({dt * 1000:.0f}ms)", flush=True)
        time.sleep(config["step_sleep_s"])


@ray.remote(num_cpus=0)
def _ras_json():
    p = subprocess.run(
        "ncclras -f json -t 5", shell=True, capture_output=True, text=True
    )
    return p.returncode, p.stdout


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--steps", type=int, default=4000)
    parser.add_argument("--step-sleep-s", type=float, default=0.2)
    parser.add_argument("--pp-every", type=int, default=10)
    parser.add_argument("--hang-rank", type=int, default=1)
    parser.add_argument("--hang-step", type=int, default=150)
    parser.add_argument("--poll-s", type=float, default=5.0)
    parser.add_argument("--min-stall-s", type=float, default=20.0)
    parser.add_argument("--stall-factor", type=float, default=5.0)
    parser.add_argument("--timeout-s", type=float, default=900.0)
    parser.add_argument(
        "--no-hang",
        action="store_true",
        help="Run only phase 1: prove the frozen PP groups do not fire.",
    )
    args = parser.parse_args()

    from ray.train import RunConfig, ScalingConfig
    from ray.train.torch import TorchTrainer
    from ray.train.v2._internal.callbacks.nccl_ras import parse_ras_schema
    from ray.train.v2._internal.execution.health import HealthManager, WorkerHealth
    from ray.train.v2._internal.execution.health.adapters.collective_join import (
        CollectiveHangEvaluator,
        parallelism_groups,
    )
    from ray.train.v2._internal.execution.health.adapters.nccl_ras_policy import (
        NcclRasProbe,
        nccl_ras_policy,
    )

    ray.init(address="auto")
    if args.workers % 2:
        print("--workers must be even (tp=2)", file=sys.stderr)
        return 2

    trainer = TorchTrainer(
        train_func,
        train_loop_config={
            "steps": args.steps,
            "step_sleep_s": args.step_sleep_s,
            "pp_every": args.pp_every,
            "hang": not args.no_hang,
            "hang_rank": args.hang_rank,
            "hang_step": args.hang_step,
        },
        scaling_config=ScalingConfig(num_workers=args.workers, use_gpu=True),
        run_config=RunConfig(
            worker_runtime_env={"env_vars": {"NCCL_RAS_ENABLE": "1"}}
        ),
    )
    threading.Thread(target=trainer.fit, daemon=True).start()

    gpu_nodes = [
        n for n in ray.nodes() if n["Alive"] and n["Resources"].get("GPU", 0) > 0
    ]
    node_ids = [n["NodeID"] for n in gpu_nodes]

    def query():
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

    # The policy the port ships; the evaluator is the one under test.
    policy = nccl_ras_policy(query=query)
    policy.evaluator_creator = lambda: [
        CollectiveHangEvaluator(
            stall_factor=args.stall_factor, min_stall_s=args.min_stall_s
        )
    ]
    manager = HealthManager([policy])

    print(
        f"\npolling every {args.poll_s:.0f}s; a stall confirms after "
        f"max({args.min_stall_s:.0f}s, {args.stall_factor:.0f} x median step time)\n"
    )

    started = time.monotonic()
    step_seen = 0
    phase1_false_positives = 0
    frozen_pp_seen = False

    while time.monotonic() - started < args.timeout_s:
        # Feed the manager what a worker would report. Until the controller
        # wiring lands, the script plays that role.
        for rank in range(args.workers):
            node_id = node_ids[rank % len(node_ids)]
            manager.ingest_worker_health(
                WorkerHealth(
                    rank,
                    node_id,
                    time.time(),
                    step=step_seen,
                    reported={
                        "tp_rank": rank % 2,
                        "pp_rank": rank // 2,
                        "step_time_s": args.step_sleep_s,
                        "step": step_seen,
                    },
                )
            )

        manager.run_cluster_probes(node_ids)
        state = manager.build_state()
        comms = state.results(NcclRasProbe)
        groups = parallelism_groups(state.reported)

        elapsed = time.monotonic() - started
        if comms:
            named = []
            for comm_id, result in sorted(comms.items()):
                ranks = frozenset(int(r) for r in result.devices)
                axis = groups.get(ranks, "?")
                frozen = "frozen" in result.events
                if frozen and axis == "pp_rank":
                    frozen_pp_seen = True
                named.append(
                    f"{comm_id[:8]}[{axis.replace('_rank', '') if axis != '?' else '?'}"
                    f"{sorted(ranks)}]{'=FROZEN' if frozen else ''}"
                )
            print(f"  t={elapsed:6.1f}s  {' '.join(named)}")

        decision = manager.poll_decision()
        if decision is not None:
            phase = "2 (wedged)" if step_seen >= args.hang_step else "1 (healthy)"
            print(f"\n  t={elapsed:6.1f}s  DECISION in phase {phase}")
            print(f"  {decision.action.name} ({decision.cause.name})")
            print(f"  {decision.reason}\n")
            if step_seen < args.hang_step:
                phase1_false_positives += 1
                print("  FAIL -- fired while the job was healthy.")
                return 1
            print("  PASS -- fired on the real hang, not on the idle PP groups.")
            print(f"  frozen PP communicators seen beforehand: {frozen_pp_seen}")
            if not frozen_pp_seen:
                print(
                    "  NOTE: no frozen PP communicator was ever observed, so the\n"
                    "        false-positive half was not actually exercised. Raise\n"
                    "        --pp-every or lower --poll-s."
                )
            return 0

        # Track the job's progress the way the controller would.
        step_seen = int(elapsed / max(args.step_sleep_s, 1e-3))
        if not args.no_hang and step_seen >= args.hang_step:
            step_seen = args.hang_step  # the job has stopped advancing

        time.sleep(args.poll_s)

    if args.no_hang:
        print(
            f"\n  PASS -- {args.timeout_s:.0f}s with no decision on a healthy job.\n"
            f"  frozen PP communicators seen: {frozen_pp_seen}"
        )
        return 0 if frozen_pp_seen else 1
    print(f"\n  FAIL -- no decision within {args.timeout_s:.0f}s")
    return 1


if __name__ == "__main__":
    sys.exit(main())
