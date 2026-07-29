import argparse
import logging
import os
import tempfile
import time
from typing import Dict, List, Optional

import torch
import torch.nn as nn
import torchvision.models as tv_models

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

GRACE_PERIOD_S = 30

# What the torchvision ImageNet classifiers expect.
IMAGE_SIZE = 224

# Slack for the step that was in flight when the node went away.
WASTED_STEPS_SLACK = 5


def create_model(model_name: str) -> nn.Module:
    if not hasattr(tv_models, model_name):
        raise ValueError(f"Unknown torchvision model: {model_name}")
    return getattr(tv_models, model_name)(weights=None)


@ray.remote(num_cpus=0)
class StepTracker:
    """Counts work across worker-group restarts."""

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
        """Mean seconds per step, excluding time spent restarting."""
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

    device = ray.train.torch.get_device()
    batch = torch.randn(config["batch_size"], 3, IMAGE_SIZE, IMAGE_SIZE, device=device)
    target = torch.randint(0, 1000, (config["batch_size"],), device=device)

    preempt_flag = torch.zeros(1, device=device)

    def any_rank_preempted() -> bool:
        """Whether *any* rank has observed the preemption yet.

        The watcher fans the signal out with independent RPCs, so ranks might
        observe it on different steps. Every rank has to take the branch below on
        the same step: a rank that enters `report` while the others run another
        `backward` deadlocks the group, because `report` and DDP's gradient
        all-reduce are both collectives and neither side can yield to the other.

        TODO(lehui): Ray Train should own this agreement instead of the UDF. JAX exposes
        `reached_preemption_sync_point(step)`, which all-reduces the max step
        across hosts and returns True at max+1, so Orbax users never write a
        collective themselves.
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
        optimizer.zero_grad()
        loss = loss_fn(model(batch), target)
        loss.backward()
        optimizer.step()

        if is_rank_0:
            ray.get(tracker.record_step.remote())

        # Checkpoint on the first observed preemption, then keep training until
        # the node is actually reclaimed.
        if use_jit_checkpoint and not saved_on_preemption and any_rank_preempted():
            saved_on_preemption = True
            save_checkpoint(step, {"step": step, "jit": True})
            if is_rank_0:
                ray.get(tracker.record_jit_checkpoint.remote(step))
            continue

        if step % checkpoint_interval == 0 or step == target_steps - 1:
            save_checkpoint(step, {"step": step, "jit": False})


def wait_for_worker_nodes(num_nodes: int, timeout_s: int = 900) -> None:
    """Wait until `num_nodes` worker nodes are alive.

    Terminated instances have to be replaced between experiments so both train
    on the same cluster size.
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


def run_experiment(use_jit_checkpoint: bool, args: argparse.Namespace) -> Dict:
    """Run one experiment end to end and return its metrics.

    Both experiments share this path deliberately: the preemption schedule,
    failure budgets, model, and step target have to be identical for the
    comparison to mean anything, so `use_jit_checkpoint` is the only difference.
    """
    label = "jit" if use_jit_checkpoint else "baseline"
    logger.info("=== Running %s experiment ===", label)
    wait_for_worker_nodes(args.num_workers)

    tracker_name = f"step_tracker_{label}"
    tracker = StepTracker.options(name=tracker_name).remote()

    # TODO: make the grace period configurable.
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
            name=f"preemption_benchmark_{label}_{int(time.time())}",
            storage_path=STORAGE_PATH,
            failure_config=FailureConfig(
                max_failures=0,
                max_preemption_failures=args.num_preemptions + 1,
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
        logger.exception("%s experiment failed", label)
        completed = False
        error = repr(e)
    e2e_time = time.time() - start_time

    summary = ray.get(tracker.summary.remote())
    ray.kill(resource_killer)
    ray.kill(tracker)

    wasted_steps = max(0, summary["executed_steps"] - args.target_steps)
    metrics = {
        "completed": completed,
        "error": error,
        "e2e_time": e2e_time,
        "wasted_steps": wasted_steps,
        "wasted_gpu_seconds": (
            wasted_steps * summary["mean_step_time_s"] * args.num_workers
        ),
        **summary,
    }
    logger.info("%s experiment metrics: %s", label, metrics)
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
            "Sized for a ~1.8h run of both experiments at the 1.13s/step measured "
            "for vit_b_16 on 8xT4. The budget is linear in step time, so re-measure "
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
            "average per preemption, while the jit experiment loses only the steps "
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

    baseline = run_experiment(use_jit_checkpoint=False, args=args)
    jit = run_experiment(use_jit_checkpoint=True, args=args)

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
        "gpu_seconds_saved": (
            baseline["wasted_gpu_seconds"] - jit["wasted_gpu_seconds"]
        ),
        "estimated_time_saved_s": steps_saved * baseline["mean_step_time_s"],
        "config": dict(vars(args), grace_period_s=GRACE_PERIOD_S),
    }
    logger.info("Preemption benchmark results: %s", results)
    safe_write_to_results_json(results)

    for label, metrics in [("baseline", baseline), ("jit", jit)]:
        assert metrics["completed"], f"{label} did not finish: {metrics['error']}"

    if not args.num_preemptions:
        logger.info(
            "No preemptions were injected; skipping the comparison assertions. "
            "Measured %.2fs/step.",
            baseline["mean_step_time_s"],
        )
        return

    assert jit["jit_checkpoints"] > 0, (
        "No just-in-time checkpoint was taken -- the preemption was probably "
        "never observed by the training function."
    )

    assert jit["wasted_steps"] <= baseline["wasted_steps"] + WASTED_STEPS_SLACK, (
        f"jit experiment redid more work than the baseline: "
        f"{jit['wasted_steps']=} vs {baseline['wasted_steps']=}"
    )


if __name__ == "__main__":
    main()
