"""Benchmark: how much work does preemption handling save?

Runs the same training workload twice, under the same injected preemption
schedule, and compares how much work each arm has to redo:

* ``baseline``: periodic checkpoints only. The training function never looks at
  ``ray.train.get_preemption_info()``, so on a preemption it resumes from the
  last periodic checkpoint and replays everything since.
* ``jit``: periodic checkpoints plus a just-in-time checkpoint taken when a
  preemption is first observed (the checkpoint-and-continue pattern). On a
  preemption it resumes from that just-in-time checkpoint, replaying ~nothing.

The headline metric is ``wasted_steps`` -- steps executed beyond the target,
i.e. pure recompute. It is measured by counting every step executed across all
attempts and subtracting the target, so it is immune to cluster/network noise.
End-to-end wall-clock time is reported alongside it.

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
# against the window they were measured with. The just-in-time checkpoint has to
# finish inside this window, so keep the model small enough that a save fits
# comfortably alongside the watcher's poll interval.
GRACE_PERIOD_S = 30

# The `jit` arm should never redo more work than the baseline. Allow a small
# slack for the step that was in flight when the node went away.
WASTED_STEPS_SLACK = 5


# ==== Workload ====


def create_model(hidden_dim: int, num_layers: int) -> nn.Module:
    layers: List[nn.Module] = [nn.Linear(hidden_dim, hidden_dim), nn.ReLU()]
    for _ in range(num_layers - 1):
        layers += [nn.Linear(hidden_dim, hidden_dim), nn.ReLU()]
    layers.append(nn.Linear(hidden_dim, 10))
    return nn.Sequential(*layers)


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
        self._attempts.append(
            {"resumed_from_step": resumed_from_step, "started_at": time.time()}
        )

    def record_step(self) -> None:
        self._executed_steps += 1

    def record_jit_checkpoint(self, step: int) -> None:
        self._jit_checkpoints += 1
        logger.info("Just-in-time checkpoint saved at step %d", step)

    def summary(self) -> Dict:
        return {
            "executed_steps": self._executed_steps,
            "num_attempts": len(self._attempts),
            "resumed_from_steps": [a["resumed_from_step"] for a in self._attempts],
            "jit_checkpoints": self._jit_checkpoints,
        }


def train_func(config: Dict):
    """Train to `target_steps`, checkpointing periodically.

    In the `jit` arm, also save a single checkpoint the first time a preemption
    is observed, then keep training. `get_preemption_info()` stays set for the
    whole grace window, so the `saved_on_preemption` guard keeps this to one
    extra checkpoint instead of one per step.

    Note that `report` is a barrier across all ranks. The preemption watcher
    fans the signal out to *every* worker, so all ranks reach this report and
    the just-in-time checkpoint commits even though only some nodes are being
    preempted.
    """
    target_steps = config["target_steps"]
    checkpoint_interval = config["checkpoint_interval"]
    step_duration_s = config["step_duration_s"]
    use_jit_checkpoint = config["use_jit_checkpoint"]
    tracker = ray.get_actor(config["tracker_name"])

    model = create_model(config["hidden_dim"], config["num_layers"])
    model = ray.train.torch.prepare_model(model)
    optimizer = torch.optim.SGD(model.parameters(), lr=0.01)
    loss_fn = nn.MSELoss()

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

    if ray.train.get_context().get_world_rank() == 0:
        ray.get(tracker.record_attempt.remote(start_step))
    logger.info("Attempt starting at step %d (target %d)", start_step, target_steps)

    def save_checkpoint(step: int, metrics: Dict):
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
    batch = torch.randn(config["batch_size"], config["hidden_dim"], device=device)
    target = torch.randn(config["batch_size"], 10, device=device)

    saved_on_preemption = False
    for step in range(start_step, target_steps):
        # One unit of work.
        optimizer.zero_grad()
        loss = loss_fn(model(batch), target)
        loss.backward()
        optimizer.step()
        time.sleep(step_duration_s)

        if ray.train.get_context().get_world_rank() == 0:
            ray.get(tracker.record_step.remote())

        # Just-in-time checkpoint on the first observed preemption, then keep
        # training until the node is actually reclaimed.
        if (
            use_jit_checkpoint
            and not saved_on_preemption
            and ray.train.get_preemption_info() is not None
        ):
            saved_on_preemption = True
            save_checkpoint(step, {"step": step, "jit": True})
            if ray.train.get_context().get_world_rank() == 0:
                ray.get(tracker.record_jit_checkpoint.remote(step))
            continue

        if step % checkpoint_interval == 0 or step == target_steps - 1:
            save_checkpoint(step, {"step": step, "jit": False})
        else:
            ray.train.report({"step": step, "jit": False})


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


def run_arm(arm: str, args: argparse.Namespace) -> Dict:
    """Run one arm end to end and return its metrics."""
    logger.info("=== Running arm '%s' ===", arm)
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
            "step_duration_s": args.step_duration_s,
            "hidden_dim": args.hidden_dim,
            "num_layers": args.num_layers,
            "batch_size": args.batch_size,
            "use_jit_checkpoint": arm == "jit",
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

    metrics = {
        "arm": arm,
        "completed": completed,
        "error": error,
        "e2e_time": e2e_time,
        # Steps executed beyond the target == work redone after a preemption.
        "wasted_steps": max(0, summary["executed_steps"] - args.target_steps),
        **summary,
    }
    logger.info("Arm '%s' metrics: %s", arm, metrics)
    return metrics


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-workers", type=int, default=2)
    parser.add_argument("--target-steps", type=int, default=600)
    parser.add_argument(
        "--checkpoint-interval",
        type=int,
        default=100,
        help=(
            "Periodic checkpoint interval in steps. The baseline loses up to "
            "this much work per preemption; the jit arm loses ~none, so the "
            "gap grows with this value."
        ),
    )
    parser.add_argument("--step-duration-s", type=float, default=0.2)
    parser.add_argument("--hidden-dim", type=int, default=1024)
    parser.add_argument("--num-layers", type=int, default=8)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--num-preemptions", type=int, default=2)
    parser.add_argument(
        "--kill-delay-s",
        type=int,
        default=90,
        help="Wait before the first preemption so the run builds up work to lose.",
    )
    parser.add_argument("--kill-interval-s", type=int, default=120)
    return parser.parse_args()


def main():
    args = parse_args()
    ray.init()

    # Run the baseline first so both arms see a cluster that has already been
    # through node replacement.
    baseline = run_arm("baseline", args)
    jit = run_arm("jit", args)

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
        "estimated_time_saved_s": steps_saved * args.step_duration_s,
        "config": dict(vars(args), grace_period_s=GRACE_PERIOD_S),
    }
    logger.info("Preemption benchmark results: %s", results)
    safe_write_to_results_json(results)

    assert jit["completed"], f"jit arm did not finish: {jit['error']}"
    assert baseline["completed"], f"baseline arm did not finish: {baseline['error']}"

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
