"""Local integration demo for preemption handling.

Spins up a real Ray cluster (2 TorchTrainer workers using torch's gloo
backend), simulates a drain event by writing to a fake-drains JSON file,
and observes the controller transition through PreemptingState + restart
with a JIT checkpoint.

Run:
    /path/to/python python/ray/train/v2/tests/preemption_integration_demo.py
"""

import json
import os
import tempfile
import threading
import time

import ray
import ray.train
from ray.train import Checkpoint, FailureConfig, RunConfig, ScalingConfig
from ray.train.torch import TorchTrainer


# Path to the fake-drains JSON file. The PreemptionCallback reads this when
# the RAY_TRAIN_PREEMPTION_FAKE_DRAINS_FILE env var is set. The file path
# crosses the driver / controller-actor process boundary cleanly because
# both processes are on the same node and /tmp is shared.
FAKE_DRAINS_FILE = "/tmp/__ray_train_v2_demo_fake_drains__.json"


def write_fake_drains(drains: dict):
    """Atomically write {node_id_hex: deadline_ms} to the fake-drains file."""
    tmp = FAKE_DRAINS_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(drains, f)
    os.replace(tmp, FAKE_DRAINS_FILE)


def clear_fake_drains():
    if os.path.exists(FAKE_DRAINS_FILE):
        os.remove(FAKE_DRAINS_FILE)


# ─── The user training function ─────────────────────────────────────────────


def train_func(config):
    rank = ray.train.get_context().get_world_rank()
    state = {"step": 0, "loss": 0.0}

    # Resume from previous checkpoint, if any.
    ckpt = ray.train.get_checkpoint()
    if ckpt:
        with ckpt.as_directory() as d:
            with open(os.path.join(d, "state.json")) as f:
                state = json.load(f)
        print(f"[rank {rank}] RESUMED from step {state['step']}", flush=True)
    else:
        print(f"[rank {rank}] starting fresh", flush=True)

    total_steps = config.get("total_steps", 200)
    for step in range(state["step"], total_steps):
        # JIT preemption check at the top of every step.
        info = ray.train.preemption_status()
        if info is not None:
            print(
                f"[rank {rank}] PREEMPT signal: this={info.this_worker_preempted} "
                f"ranks={info.preempted_ranks} "
                f"sec_left={info.seconds_remaining:.1f}",
                flush=True,
            )
            # In standard DDP every rank holds the same model+optimizer state,
            # so any of them can write a valid checkpoint. Writing from every
            # rank (rather than just the survivor) means:
            #   - single-node local tests work (no "survivor" exists when the
            #     one node is draining)
            #   - redundancy if one rank's write fails before deadline
            # ``ray.train.report`` serializes per-step writes via
            # report_call_index so duplicate writes are handled correctly.
            #
            # Power-user variant: gate on ``not info.this_worker_preempted`` to
            # have only survivors write (useful in multi-node clusters where
            # the surviving DC has more headroom before its own deadline).
            with tempfile.TemporaryDirectory() as d:
                with open(os.path.join(d, "state.json"), "w") as f:
                    json.dump(state, f)
                ray.train.report(
                    {"loss": state["loss"], "step": state["step"]},
                    checkpoint=Checkpoint.from_directory(d),
                )
            print(
                f"[rank {rank}] wrote JIT checkpoint at step {state['step']} "
                f"({'preempted' if info.this_worker_preempted else 'survivor'})",
                flush=True,
            )
            return

        # Fake training step.
        time.sleep(0.2)
        state["step"] = step
        state["loss"] = 1.0 / (step + 1)

        # Periodic async checkpoint (every 50 steps).
        if step > 0 and step % 50 == 0:
            with tempfile.TemporaryDirectory() as d:
                with open(os.path.join(d, "state.json"), "w") as f:
                    json.dump(state, f)
                ray.train.report(
                    {"loss": state["loss"], "step": step},
                    checkpoint=Checkpoint.from_directory(d),
                )
            print(f"[rank {rank}] periodic checkpoint at step {step}", flush=True)


# ─── Main ──────────────────────────────────────────────────────────────────


def main():
    # 1. Configure preemption handling via env vars. Setting them BEFORE
    #    constructing the trainer is important so the controller actor
    #    inherits them via get_env_vars_to_propagate().
    clear_fake_drains()
    os.environ["RAY_TRAIN_PREEMPTION_FAKE_DRAINS_FILE"] = FAKE_DRAINS_FILE
    os.environ["RAY_TRAIN_PREEMPTION_POLL_INTERVAL_S"] = "1"
    os.environ["RAY_TRAIN_PREEMPTION_FAILURE_DOMAIN"] = "node"

    storage_dir = tempfile.mkdtemp(prefix="ray_train_preempt_demo_")
    print(f"storage_dir: {storage_dir}")
    print(f"fake_drains_file: {FAKE_DRAINS_FILE}")

    # 2. Schedule a drain injection.
    #    Wait long enough that the worker group is up. Then atomically
    #    write the file. The controller's PreemptionCallback polls every
    #    1s and will pick up the change on the next tick.
    #
    #    Two modes:
    #    (a) If RAY_TRAIN_PREEMPTION_FAKE_NODE_MAP is set (e.g.,
    #        "0:nodeA,1:nodeB"), we drain one of the FAKE node IDs so
    #        only specific ranks become preempted -- survivor branch
    #        is exercised.
    #    (b) Otherwise we drain the REAL node ID of the head node,
    #        which on a single-node cluster preempts ALL ranks.
    def inject_drain():
        time.sleep(15)  # workers usually up by t=10s; give margin

        fake_node_map_env = os.environ.get("RAY_TRAIN_PREEMPTION_FAKE_NODE_MAP")
        if fake_node_map_env:
            # Survivor-mode: parse fake map, drain the LAST node id so the
            # earlier ranks stay as survivors. With "0:nodeA,1:nodeB" this
            # drains nodeB → rank 1 preempted, rank 0 survives.
            fake_node_ids = []
            for entry in fake_node_map_env.split(","):
                entry = entry.strip()
                if not entry:
                    continue
                fake_node_ids.append(entry.split(":", 1)[1].strip())
            unique_ids = list(dict.fromkeys(fake_node_ids))  # preserve order, dedup
            if not unique_ids:
                print("[injector] empty fake map; aborting", flush=True)
                return
            target = unique_ids[-1]
            survivors = unique_ids[:-1]
            mode_desc = (
                f"survivor-mode (fake): drain={target}, "
                f"survivors={survivors}"
            )
        else:
            # All-preempted mode: drain the real head node.
            nodes = [n for n in ray.nodes() if n.get("Alive")]
            if not nodes:
                print("[injector] no live nodes!", flush=True)
                return
            target = nodes[0]["NodeID"]
            mode_desc = f"all-preempted (real node): drain={target[:8]}..."

        deadline_ms = int((time.time() + 30) * 1000)  # 30s deadline
        write_fake_drains({target: deadline_ms})
        print(f"[injector] {mode_desc}, deadline=+30s", flush=True)
        # Clear after 10s so the restart attempt can train normally instead
        # of immediately preempting again.
        time.sleep(10)
        clear_fake_drains()
        print("[injector] cleared fake drain (so restart can progress)", flush=True)

    injector = threading.Thread(target=inject_drain, daemon=True)
    injector.start()

    # 3. Run the trainer.
    trainer = TorchTrainer(
        train_func,
        train_loop_config={"total_steps": 500},
        scaling_config=ScalingConfig(num_workers=2, use_gpu=False),
        run_config=RunConfig(
            name="preempt_demo",
            storage_path=storage_dir,
            failure_config=FailureConfig(
                max_failures=0,
                max_preemption_failures=1,  # allow ONE retry on preemption
            ),
        ),
    )
    try:
        result = trainer.fit()
        print("\n=== RESULT ===")
        print(result)
        print("\n=== FINAL CHECKPOINT ===")
        print(result.checkpoint)
    finally:
        clear_fake_drains()


if __name__ == "__main__":
    main()
