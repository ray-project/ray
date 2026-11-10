#!/usr/bin/env python
"""GR00T 2's straggler detection and recovery, on ``ray.train.health``.

    python gr00t_recovery.py                 # every scenario, on the running cluster
    python gr00t_recovery.py stall           # one of them
    python gr00t_recovery.py --local stall   # a local CPU cluster: 4 nodes, gloo

Every scenario runs the same health config, the one GR00T would run:

  pre-flight  ``gpu_health.all_reduce_preflight_check()``, at the start of
              each attempt with training stopped.
  in flight   ``progress_health.stall_check()`` and ``straggler_check()``, on
              each rank's ``report_progress(step, phase, loss, straggler_score)``.

The job trains 120 steps, saves a checkpoint every 40, and resumes from the
latest one when it restarts. ``FailureConfig(max_failures=1)``. Each scenario
injects one fault, in the first attempt only:

  stall                rank 1 stops after step 50, alive. Its step stops
                       advancing past the train phase's timeout: DIAGNOSE (stack
                       traces, GPU health), no GPU fault, REATTEMPT, resume from
                       step 40.
  checkpoint_pause     rank 0 takes 25s to save the step 40 checkpoint, longer
                       than the train phase's timeout but within the
                       checkpoint phase's: no decision.
  straggler            rank 1 reports a straggler score of 0.5 from step 50, on
                       healthy GPUs: DIAGNOSE, no GPU fault, training continues.
  straggler_gpu_fault  the last rank reports 0.5 from step 50, and its node gets
                       XID 48 (double-bit ECC): DIAGNOSE, EVICT, resume from
                       step 40.
  nan_loss             rank 0's loss is NaN from step 50: REATTEMPT, resume from
                       step 40.

Pass: each scenario's decisions and ending match, and a restarted run resumes
from a checkpoint: step 40, or 80 if training reached it while diagnosing.
"""
import argparse
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import ray

sys.path.insert(0, str(Path(__file__).parent))
import gpu_health  # noqa: E402
import harness  # noqa: E402
import progress_health  # noqa: E402

harness.ship_by_value(harness, gpu_health, progress_health)

STEPS = 120
CHECKPOINT_EVERY = 40
STEP_S = 0.1
INIT_S = 2.0
PHASE_TIMEOUTS_S = {
    progress_health.INIT: 60.0,
    progress_health.TRAIN: 10.0,
    progress_health.CHECKPOINT: 60.0,
}


@dataclass
class Fault:
    """``rank`` (``-1`` for the last) injects ``kind`` after ``at_step``, in
    the run's first attempt only."""

    kind: str
    rank: int
    at_step: int
    pause_s: float = 0.0

    def describe(self) -> str:
        rank = "the last rank" if self.rank == -1 else f"rank {self.rank}"
        return {
            "stall": f"{rank} stops after step {self.at_step}, alive",
            "checkpoint_pause": f"{rank} takes {self.pause_s:.0f}s to save the "
            f"step {self.at_step} checkpoint",
            "straggler": f"{rank} reports a straggler score of 0.5 from step "
            f"{self.at_step}",
            "straggler_gpu_fault": f"{rank} reports a straggler score of 0.5 "
            f"from step {self.at_step}, and its node gets XID 48",
            "nan_loss": f"{rank}'s loss is NaN from step {self.at_step}",
        }[self.kind]


SCENARIOS = {
    "stall": dict(
        fault=Fault("stall", rank=1, at_step=50),
        decisions=["DIAGNOSE", "REATTEMPT"],
        resumes=True,
    ),
    "checkpoint_pause": dict(
        fault=Fault("checkpoint_pause", rank=0, at_step=40, pause_s=25.0),
        decisions=[],
        resumes=False,
    ),
    "straggler": dict(
        fault=Fault("straggler", rank=1, at_step=50),
        decisions=["DIAGNOSE"],
        resumes=False,
    ),
    "straggler_gpu_fault": dict(
        fault=Fault("straggler_gpu_fault", rank=-1, at_step=50),
        decisions=["DIAGNOSE", "EVICT"],
        resumes=True,
    ),
    "nan_loss": dict(
        fault=Fault("nan_loss", rank=0, at_step=50),
        decisions=["REATTEMPT"],
        resumes=True,
    ),
}


def make_train_func(fault: Fault):
    def train_func():
        import json
        import os
        import tempfile
        import time

        import torch
        import torch.distributed as dist

        import ray.train
        from ray.train import Checkpoint

        context = ray.train.get_context()
        rank, world_size = context.get_world_rank(), context.get_world_size()
        node_id = ray.get_runtime_context().get_node_id()
        armed = rank == fault.rank % world_size and not harness.fault_fired()

        start = 0
        checkpoint = ray.train.get_checkpoint()
        if checkpoint:
            with checkpoint.as_directory() as d:
                with open(os.path.join(d, "state.json")) as f:
                    start = json.load(f)["step"]
        progress_health.report_progress(start, progress_health.INIT, loss=0.0)
        nodes = [None] * world_size
        dist.all_gather_object(nodes, node_id)
        if rank == 0:
            short = sorted({n[:8] for n in nodes})
            harness.record("START", f"from step {start}, on nodes {short}")
        time.sleep(INIT_S)

        device = "cuda" if torch.cuda.is_available() else "cpu"
        tensor = torch.ones(256, 256, device=device)
        score, nan = 1.0, False
        for step in range(start + 1, STEPS + 1):
            dist.all_reduce(tensor)
            if device == "cuda":
                torch.cuda.synchronize()
            time.sleep(STEP_S)
            fire = armed and step == fault.at_step
            if fire:
                harness.record("INJECT", fault.describe())
                nan = fault.kind == "nan_loss"
                if fault.kind.startswith("straggler"):
                    score = 0.5
                if fault.kind == "straggler_gpu_fault":
                    harness.inject_gpu_event(node_id, gpu_health.xid_event(48, gpu=0))
            loss = float("nan") if nan else 1.0 / step
            progress_health.report_progress(
                step, progress_health.TRAIN, loss, straggler_score=score
            )
            if fire and fault.kind == "stall":
                time.sleep(3600)
            if step % CHECKPOINT_EVERY == 0:
                progress_health.report_progress(
                    step, progress_health.CHECKPOINT, loss, straggler_score=score
                )
                if fire and fault.kind == "checkpoint_pause":
                    time.sleep(fault.pause_s)
                with tempfile.TemporaryDirectory() as d:
                    checkpoint = None
                    if rank == 0:
                        with open(os.path.join(d, "state.json"), "w") as f:
                            json.dump({"step": step}, f)
                        checkpoint = Checkpoint.from_directory(d)
                    ray.train.report({"step": step}, checkpoint=checkpoint)

    return train_func


def _resume_step(timeline) -> Optional[int]:
    """The step the second attempt resumed from, if there was one."""
    starts = [detail for _, kind, detail in timeline if kind == "START"]
    if len(starts) < 2:
        return None
    return int(starts[1].split()[2].rstrip(","))


def run(name: str, workers: int, use_gpu: bool) -> bool:
    from ray.train import FailureConfig, RunConfig, ScalingConfig
    from ray.train.health import HealthConfig
    from ray.train.torch import TorchTrainer
    from ray.train.v2.api.exceptions import TrainingFailedError

    scenario = SCENARIOS[name]
    fault = scenario["fault"]
    harness.banner(name, fault)
    harness.clear_gpu_events()
    events = harness.start_events()
    run_name = f"gr00t_{name}_{int(time.time())}"
    storage = harness.storage_path()
    # Ray Train's default storage when there's no shared storage.
    root = storage or os.path.expanduser("~/ray_results")
    artifacts = os.path.join(root, run_name, "health_diagnostics")
    trainer = TorchTrainer(
        make_train_func(fault),
        scaling_config=ScalingConfig(num_workers=workers, use_gpu=use_gpu),
        run_config=RunConfig(
            name=run_name,
            storage_path=storage,
            health_config=HealthConfig(
                preflight_checks=[gpu_health.all_reduce_preflight_check()],
                inflight_checks=[
                    progress_health.stall_check(
                        PHASE_TIMEOUTS_S, artifacts_dir=artifacts
                    ),
                    progress_health.straggler_check(
                        on_healthy=lambda message: harness.record("NO FAULT", message)
                    ),
                ],
            ),
            callbacks=[harness.Timeline()],
            failure_config=FailureConfig(max_failures=1),
        ),
    )
    error = None
    try:
        trainer.fit()
    except TrainingFailedError as e:
        error = e
    timeline = ray.get(events.get.remote())
    ray.kill(events)
    harness.clear_gpu_events()

    harness.print_timeline(timeline, "timeline (seconds after the fault fired):")
    if os.path.isdir(artifacts):
        print("\n  artifacts:")
        harness.list_artifacts(artifacts)
    decisions = [
        kind for _, kind, _ in timeline if kind in ("DIAGNOSE", "REATTEMPT", "EVICT")
    ]
    resume_step = _resume_step(timeline)
    ok = (
        decisions == scenario["decisions"]
        and error is None
        and (resume_step is not None) == scenario["resumes"]
        and (resume_step or 0) % CHECKPOINT_EVERY == 0
    )
    if name == "straggler":
        ok &= any(kind == "NO FAULT" for _, kind, _ in timeline)
    ended = type(error).__name__ if error else None
    print(
        f"  decisions {decisions}, resumed from step {resume_step}, ended with "
        f"{ended}: {'PASS' if ok else 'FAIL'}"
    )
    return ok


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("scenarios", nargs="*", help=f"some of {list(SCENARIOS)}")
    parser.add_argument(
        "--local", action="store_true", help="a local cluster of 4 single-CPU nodes"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=harness.workers_from_env(),
        help="how many workers to train; by default one per GPU, or per local node",
    )
    args = parser.parse_args()
    unknown = set(args.scenarios) - set(SCENARIOS)
    if unknown:
        parser.error(f"unknown scenarios {sorted(unknown)}")

    workers, use_gpu = harness.connect(args.local, args.workers)

    results = {
        name: run(name, workers, use_gpu) for name in args.scenarios or SCENARIOS
    }
    # Disconnect before the local cluster's exit hook stops its nodes.
    ray.shutdown()
    print("\n" + "=" * 78)
    for name, ok in results.items():
        print(f"  {name:<20} {'PASS' if ok else 'FAIL'}")
    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
