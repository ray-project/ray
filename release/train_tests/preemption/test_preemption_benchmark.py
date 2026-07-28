"""Benchmark: how much work does preemption handling save?

Runs the same training workload twice, under the same injected preemption
schedule, and compares how much work each arm has to redo:

* ``baseline``: periodic checkpoints only. The training function never looks at
  ``ray.train.get_preemption_info()``, so on a preemption it resumes from the
  last periodic checkpoint and replays everything since.
* ``jit``: periodic checkpoints plus a just-in-time checkpoint taken when a
  preemption is first observed (the checkpoint-and-continue pattern). On a
  preemption it resumes from that just-in-time checkpoint, replaying ~nothing.

The headline metrics are ``wasted_steps`` -- steps executed beyond the target,
i.e. pure recompute -- and ``wasted_gpu_seconds``, which scales that by the
worker count because a single preempted node forces the whole group to restart
and redo the same work. Both are measured by counting every step executed
across all attempts and subtracting the target, so they are immune to how long
the autoscaler took to replace an instance. End-to-end wall-clock time is
reported alongside them, but it is dominated by node replacement and is the
noisier signal.

A preemption lands at a random point in the checkpoint interval, so over
several preemptions the baseline loses ``checkpoint_interval / 2`` steps each
on average, while the ``jit`` arm only loses the steps taken during the drain
grace window.

Preemptions are injected with ``EC2InstanceTerminatorWithGracePeriod``, which
drains a node through the GCS with ``DRAIN_NODE_REASON_PREEMPTION`` and a real
deadline, waits out the grace period, then stops the instance. That drives the
whole production path: the preemption watcher, the fan-out to every worker,
``get_preemption_info()``, ``PreemptingState``, and a real
``RayActorError.preempted`` death.
"""

import argparse
import logging
import os
import tempfile
import time
from typing import Dict, List, Optional

import torch
import torch.nn as nn

import ray
import ray.train
import ray.train.torch
from ray._private.test_utils import (
    EC2InstanceTerminatorWithGracePeriod,
    get_and_run_resource_killer,
    safe_write_to_results_json,
)
from ray.train import CheckpointConfig, FailureConfig, RunConfig, ScalingConfig
from ray.train.torch import TorchTrainer

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

STORAGE_PATH = os.environ.get("ANYSCALE_ARTIFACT_STORAGE", "/mnt/cluster_storage")

# Mirrors the hardcoded `grace_period_s` default of
# `EC2InstanceTerminatorWithGracePeriod`; we cannot currently override it (see
# the TODO in `run_arm`). Recorded in the results so a run's numbers can be read
# against the window they were measured with.
#
# This is the hard budget for the just-in-time checkpoint: the watcher poll
# (~5s) plus the checkpoint save+upload must fit inside it, or the node is gone
# before the checkpoint commits and the `jit` arm degrades to the baseline.
# `vit_b_16` is ~86M params (~0.35GB), which uploads in a few seconds; going
# much larger (e.g. `vit_l_16` at ~1.2GB) eats most of the window.
GRACE_PERIOD_S = 30

# Image size for the synthetic batches. Matches what the torchvision ImageNet
# classifiers expect.
IMAGE_SIZE = 224

# The `jit` arm should never redo more work than the baseline. Allow a small
# slack for the step that was in flight when the node went away.
WASTED_STEPS_SLACK = 5


# ==== Workload ====


def create_model(model_name: str) -> nn.Module:
    """Build a torchvision classifier.

    A real architecture (rather than a stack of Linear layers) makes each step
    represent genuine training work, so `wasted_steps` translates into GPU-time
    that a user would actually lose.
    """
    import torchvision.models as tv_models

    if not hasattr(tv_models, model_name):
        raise ValueError(f"Unknown torchvision model: {model_name}")
    # `weights=None` -- we measure recompute, not accuracy, so random init is
    # fine and avoids downloading weights onto every worker.
    return getattr(tv_models, model_name)(weights=None)


@ray.remote(num_cpus=0)
class StepTracker:
    """Counts work across worker-group restarts.

    Lives on the driver so it survives the restarts that a preemption causes.
    Only rank 0 reports, so counts are per-run rather than per-worker.
    """

    def __init__(self):
        self._executed_steps = 0
        self._attempts: List[Dict] = []
        self._jit_checkpoints = 0

    def record_attempt(self, resumed_from_step: int) -> None:
        now = time.time()
        self._attempts.append(
            {
                "resumed_from_step": resumed_from_step,
                "started_at": now,
                "last_step_at": now,
            }
        )

    def record_step(self) -> None:
        self._executed_steps += 1
        if self._attempts:
            self._attempts[-1]["last_step_at"] = time.time()

    def record_jit_checkpoint(self, step: int) -> None:
        self._jit_checkpoints += 1
        logger.info("Just-in-time checkpoint saved at step %d", step)

    def _mean_step_time_s(self) -> float:
        """Mean seconds per step, excluding time spent restarting.

        Summing each attempt's own span (rather than dividing end-to-end time)
        leaves out the minutes spent waiting for the autoscaler to replace a
        preempted node, so this reflects training throughput only.
        """
        if not self._executed_steps:
            return 0.0
        training_s = sum(a["last_step_at"] - a["started_at"] for a in self._attempts)
        return training_s / self._executed_steps

    def summary(self) -> Dict:
        return {
            "executed_steps": self._executed_steps,
            "num_attempts": len(self._attempts),
            "resumed_from_steps": [a["resumed_from_step"] for a in self._attempts],
            "jit_checkpoints": self._jit_checkpoints,
            "mean_step_time_s": self._mean_step_time_s(),
        }


def train_func(config: Dict):
    """Train to `target_steps`, checkpointing periodically.

    In the `jit` arm, also save a single checkpoint the first time a preemption
    is observed, then keep training. `get_preemption_info()` stays set for the
    whole grace window, so the `saved_on_preemption` guard keeps this to one
    extra checkpoint instead of one per step.

    The watcher fans the signal out to *every* worker, not just the ones on the
    doomed node -- but as independent RPCs, so ranks can observe it on different
    steps. Since `report` is an all-rank barrier, the decision is all-reduced
    (see `any_rank_preempted`) so the ranks stay in lockstep.
    """
    target_steps = config["target_steps"]
    checkpoint_interval = config["checkpoint_interval"]
    use_jit_checkpoint = config["use_jit_checkpoint"]
    tracker = ray.get_actor(config["tracker_name"])
    is_rank_0 = ray.train.get_context().get_world_rank() == 0

    model = create_model(config["model"])
    model = ray.train.torch.prepare_model(model)
    optimizer = torch.optim.SGD(model.parameters(), lr=0.01, momentum=0.9)
    loss_fn = nn.CrossEntropyLoss()

    start_step = 0
    checkpoint = ray.train.get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as checkpoint_dir:
            state = torch.load(
                os.path.join(checkpoint_dir, "state.pt"), map_location="cpu"
            )
        model.load_state_dict(state["model"])
        optimizer.load_state_dict(state["optimizer"])
        start_step = state["step"] + 1

    if is_rank_0:
        ray.get(tracker.record_attempt.remote(start_step))
    logger.info("Attempt starting at step %d (target %d)", start_step, target_steps)

    def save_checkpoint(step: int, metrics: Dict):
        """Report a checkpoint from rank 0 only.

        `report` is an all-rank barrier, so every rank must call it the same
        number of times -- but only rank 0 needs to upload. Under DDP all ranks
        hold identical weights, so uploading from every rank would multiply the
        checkpoint traffic by `num_workers` for no benefit, and the just-in-time
        save has to fit inside the drain grace period.
        """
        if not is_rank_0:
            ray.train.report(metrics, checkpoint=None)
            return
        with tempfile.TemporaryDirectory() as tmpdir:
            torch.save(
                {
                    "model": model.state_dict(),
                    "optimizer": optimizer.state_dict(),
                    "step": step,
                },
                os.path.join(tmpdir, "state.pt"),
            )
            ray.train.report(
                metrics, checkpoint=ray.train.Checkpoint.from_directory(tmpdir)
            )

    # Synthetic batches: this benchmark measures recomputed work, not accuracy,
    # so fixed random tensors keep step time deterministic and avoid a data
    # dependency. The forward/backward is the real model's, which is what makes
    # a "wasted step" cost real GPU time.
    device = ray.train.torch.get_device()
    batch = torch.randn(config["batch_size"], 3, IMAGE_SIZE, IMAGE_SIZE, device=device)
    target = torch.randint(0, 1000, (config["batch_size"],), device=device)

    preempt_flag = torch.zeros(1, device=device)

    def any_rank_preempted() -> bool:
        """Whether *any* rank has observed the preemption yet.

        `get_preemption_info()` is per-worker state and the watcher fans the
        signal out with independent RPCs, so ranks can observe it on different
        steps. Since `report` is an all-rank barrier, the decision to take a
        just-in-time checkpoint has to be collective -- otherwise one rank
        reports and the others don't, and the barrier deadlocks. All-reduce the
        flag so every rank agrees on the same step. (This mirrors what a real
        coordinated emergency checkpoint has to do.)
        """
        local = ray.train.get_preemption_info() is not None
        if (
            not torch.distributed.is_available()
            or not torch.distributed.is_initialized()
        ):
            return local
        preempt_flag[0] = 1.0 if local else 0.0
        torch.distributed.all_reduce(preempt_flag, op=torch.distributed.ReduceOp.MAX)
        return preempt_flag.item() > 0

    saved_on_preemption = False
    for step in range(start_step, target_steps):
        # One unit of work.
        optimizer.zero_grad()
        loss = loss_fn(model(batch), target)
        loss.backward()
        optimizer.step()

        if is_rank_0:
            ray.get(tracker.record_step.remote())

        # Just-in-time checkpoint on the first observed preemption, then keep
        # training until the node is actually reclaimed. Every rank evaluates
        # this identically, so they stay in lockstep on the `report` barrier.
        if use_jit_checkpoint and not saved_on_preemption and any_rank_preempted():
            saved_on_preemption = True
            save_checkpoint(step, {"step": step, "jit": True})
            if is_rank_0:
                ray.get(tracker.record_jit_checkpoint.remote(step))
            continue

        # Only checkpoint steps report. Train V2 does not persist metrics that
        # aren't attached to a checkpoint, so a metrics-only `report` would be a
        # no-op that still pays for the all-rank barrier and the controller
        # draining a size-1 result queue (~2s/step in an earlier run of this
        # benchmark). Progress is visible from the checkpoint reports, and the
        # benchmark's own counters live in `StepTracker`, not `Result.metrics`.
        if step % checkpoint_interval == 0 or step == target_steps - 1:
            save_checkpoint(step, {"step": step, "jit": False})


# ==== Harness ====


def wait_for_worker_nodes(num_nodes: int, timeout_s: int = 900) -> None:
    """Wait until `num_nodes` worker nodes are alive.

    Terminated instances have to be replaced between arms so both arms train on
    the same cluster size.
    """
    head_node_id = ray.get_runtime_context().get_node_id()
    deadline = time.time() + timeout_s
    num_alive = 0
    while time.time() < deadline:
        num_alive = len(
            [
                node
                for node in ray.nodes()
                if node["Alive"] and node["NodeID"] != head_node_id
            ]
        )
        if num_alive >= num_nodes:
            return
        logger.info("Waiting for worker nodes: %d/%d alive", num_alive, num_nodes)
        time.sleep(10)
    raise TimeoutError(
        f"Only {num_alive} of {num_nodes} worker nodes came back within {timeout_s}s."
    )


def run_arm(arm: str, use_jit_checkpoint: bool, args: argparse.Namespace) -> Dict:
    """Run one arm end to end and return its metrics.

    Both arms share this path deliberately: the preemption schedule, failure
    budgets, model, and step target must be identical for the comparison to mean
    anything, so `use_jit_checkpoint` is the only thing that differs between
    them. It selects the checkpoint-and-continue branch inside `train_func`.
    """
    logger.info(
        "=== Running arm '%s' (use_jit_checkpoint=%s) ===", arm, use_jit_checkpoint
    )
    wait_for_worker_nodes(args.num_workers)

    tracker_name = f"step_tracker_{arm}"
    tracker = StepTracker.options(name=tracker_name).remote()

    # Inject the preemption schedule. `kill_delay_s` lets the run get far enough
    # past a periodic checkpoint that the baseline has real work to lose.
    #
    # The killer drains the node with a reclaim deadline of
    # `now + grace_period_s`, waits that long, then force-stops Ray on it. That
    # deadline is what reaches the UDF as `get_preemption_info().deadline_ms`,
    # so it bounds how long a rank has to write its just-in-time checkpoint.
    #
    # TODO: make the grace period sweepable. `grace_period_s` is a ctor kwarg of
    # `EC2InstanceTerminatorWithGracePeriod` (default 30s), but
    # `get_and_run_resource_killer` does not forward extra kwargs, so we are
    # pinned to the default. Either add a **kwargs passthrough there, or
    # construct the killer actor directly here, so the benchmark can measure how
    # the savings degrade once the window is too short for the JIT save.
    resource_killer = get_and_run_resource_killer(
        EC2InstanceTerminatorWithGracePeriod,
        kill_interval_s=args.kill_interval_s,
        max_to_kill=args.num_preemptions,
        kill_delay_s=args.kill_delay_s,
    )

    trainer = TorchTrainer(
        train_func,
        train_loop_config={
            "target_steps": args.target_steps,
            "checkpoint_interval": args.checkpoint_interval,
            "model": args.model,
            "batch_size": args.batch_size,
            "use_jit_checkpoint": use_jit_checkpoint,
            "tracker_name": tracker_name,
        },
        scaling_config=ScalingConfig(num_workers=args.num_workers, use_gpu=True),
        run_config=RunConfig(
            name=f"preemption_benchmark_{arm}_{int(time.time())}",
            storage_path=STORAGE_PATH,
            # Preemptions consume `max_preemption_failures`, not `max_failures`,
            # so a spot run isn't hard-failed by planned interruptions.
            failure_config=FailureConfig(
                max_failures=0,
                max_preemption_failures=args.num_preemptions + 2,
            ),
            checkpoint_config=CheckpointConfig(num_to_keep=2),
        ),
    )

    start_time = time.time()
    error: Optional[str] = None
    try:
        result = trainer.fit()
        completed = result.error is None
    except Exception as e:  # noqa: BLE001 - report the failure as a metric
        logger.exception("Arm '%s' failed", arm)
        completed = False
        error = repr(e)
    e2e_time = time.time() - start_time

    summary = ray.get(tracker.summary.remote())
    ray.kill(resource_killer)
    ray.kill(tracker)

    wasted_steps = max(0, summary["executed_steps"] - args.target_steps)
    metrics = {
        "arm": arm,
        "completed": completed,
        "error": error,
        "e2e_time": e2e_time,
        # Steps executed beyond the target == work redone after a preemption.
        "wasted_steps": wasted_steps,
        # A preempted node forces *every* worker to restart and redo the same
        # steps, so the cost to the cluster scales with the worker count.
        "wasted_gpu_seconds": (
            wasted_steps * summary["mean_step_time_s"] * args.num_workers
        ),
        **summary,
    }
    logger.info("Arm '%s' metrics: %s", arm, metrics)
    return metrics


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-workers", type=int, default=8)
    parser.add_argument(
        "--model",
        type=str,
        default="vit_b_16",
        help=(
            "torchvision classifier to train. `vit_b_16` (~86M params, ~0.35GB "
            "checkpoint) keeps the just-in-time save well inside the drain "
            "grace period. `vit_l_16` (~304M) stresses that window and is much "
            "slower on g4dn, where the gradient all-reduce dominates step time."
        ),
    )
    parser.add_argument(
        "--target-steps",
        type=int,
        default=1600,
        help=(
            "Sized for a ~1.8h two-arm run at the 1.13s/step measured for "
            "vit_b_16 on 8xT4. The budget is linear in step time, so re-measure "
            "with `--num-preemptions 0` before changing the model or worker "
            "count, and keep `--checkpoint-interval` at about a quarter of this."
        ),
    )
    parser.add_argument(
        "--checkpoint-interval",
        type=int,
        default=400,
        help=(
            "Periodic checkpoint interval in steps. A preemption lands at a "
            "random point in the interval, so the baseline loses this/2 on "
            "average per preemption while the jit arm loses only the steps "
            "taken during the grace window. The gap grows with this value."
        ),
    )
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--num-preemptions", type=int, default=4)
    parser.add_argument(
        "--kill-delay-s",
        type=int,
        default=300,
        help="Wait before the first preemption so the run builds up work to lose.",
    )
    parser.add_argument(
        "--kill-interval-s",
        type=int,
        default=600,
        help="Spacing between preemptions; spreads them across the run.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    ray.init()

    # Run the baseline first so both arms see a cluster that has already been
    # through node replacement.
    baseline = run_arm("baseline", use_jit_checkpoint=False, args=args)
    jit = run_arm("jit", use_jit_checkpoint=True, args=args)

    steps_saved = baseline["wasted_steps"] - jit["wasted_steps"]
    time_saved = baseline["e2e_time"] - jit["e2e_time"]
    results = {
        "baseline": baseline,
        "jit": jit,
        "steps_saved": steps_saved,
        "steps_saved_pct": (
            100.0 * steps_saved / baseline["wasted_steps"]
            if baseline["wasted_steps"]
            else 0.0
        ),
        "time_saved_s": time_saved,
        "time_saved_pct": (
            100.0 * time_saved / baseline["e2e_time"] if baseline["e2e_time"] else 0.0
        ),
        # Recompute avoided, in GPU-seconds across the whole worker group. This
        # is the headline cost number: unlike `time_saved_s` it isn't polluted
        # by how long the autoscaler took to replace the preempted instances.
        "gpu_seconds_saved": (
            baseline["wasted_gpu_seconds"] - jit["wasted_gpu_seconds"]
        ),
        "estimated_time_saved_s": steps_saved * baseline["mean_step_time_s"],
        "config": dict(vars(args), grace_period_s=GRACE_PERIOD_S),
    }
    logger.info("Preemption benchmark results: %s", results)
    safe_write_to_results_json(results)

    assert jit["completed"], f"jit arm did not finish: {jit['error']}"
    assert baseline["completed"], f"baseline arm did not finish: {baseline['error']}"

    if not args.num_preemptions:
        # Calibration run: nothing was preempted, so there is nothing to compare.
        # `mean_step_time_s` above is the number to size `--target-steps` with.
        logger.info(
            "No preemptions were injected; skipping the comparison assertions. "
            "Measured %.2fs/step.",
            baseline["mean_step_time_s"],
        )
        return

    # The just-in-time checkpoint has to actually have been taken, otherwise the
    # arms are identical and the comparison is meaningless.
    assert jit["jit_checkpoints"] > 0, (
        "No just-in-time checkpoint was taken -- the preemption was probably "
        "never observed by the training function."
    )

    # The core claim: checkpoint-and-continue redoes less work.
    assert jit["wasted_steps"] <= baseline["wasted_steps"] + WASTED_STEPS_SLACK, (
        f"jit arm redid more work than the baseline: "
        f"{jit['wasted_steps']=} vs {baseline['wasted_steps']=}"
    )


if __name__ == "__main__":
    main()
