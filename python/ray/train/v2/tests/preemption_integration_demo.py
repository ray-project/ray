"""Local integration demo for preemption handling.

Sets up a multi-node local Ray cluster (head + 3 worker nodes) via
``ray.cluster_utils.Cluster``, starts a 2-worker training run that uses
2 of the 3 worker nodes, then drains one of those worker nodes via the
real Ray Core ``drain_node`` GCS RPC. The remaining 2 worker nodes
(the original survivor's node + the standby) carry the restart, so the
run completes end-to-end.

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
from ray.cluster_utils import Cluster
from ray.train import Checkpoint, FailureConfig, RunConfig, ScalingConfig
from ray.train.torch import TorchTrainer


def train_func(config):
    rank = ray.train.get_context().get_world_rank()
    state = {"step": 0, "loss": 0.0}

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
        info = ray.train.preemption_status()
        if info is not None:
            is_preempted = rank in info.preempted_ranks
            print(
                f"[rank {rank}] PREEMPT signal: "
                f"preempted_ranks={info.preempted_ranks} "
                f"sec_left={info.seconds_remaining:.1f} "
                f"is_me={is_preempted}",
                flush=True,
            )
            # Differentiated branches so the log clearly shows each path:
            #   - preempted rank: local cleanup only, NO checkpoint
            #     (the survivor will write a valid checkpoint, and in
            #     standard DDP every rank holds the same state anyway)
            #   - survivor rank: write a JIT checkpoint via train.report,
            #     which becomes the resume point for the restarted WG
            if is_preempted:
                print(
                    f"[rank {rank}] PREEMPTED branch: doing local cleanup",
                    flush=True,
                )
                time.sleep(0.5)  # simulate user cleanup work
                print(
                    f"[rank {rank}] PREEMPTED branch: cleanup done, exiting "
                    f"(no checkpoint written by this rank)",
                    flush=True,
                )
            else:
                print(
                    f"[rank {rank}] SURVIVOR branch: writing JIT checkpoint "
                    f"at step {state['step']}",
                    flush=True,
                )
                with tempfile.TemporaryDirectory() as d:
                    with open(os.path.join(d, "state.json"), "w") as f:
                        json.dump(state, f)
                    ray.train.report(
                        {"loss": state["loss"], "step": state["step"]},
                        checkpoint=Checkpoint.from_directory(d),
                    )
                print(
                    f"[rank {rank}] SURVIVOR branch: JIT checkpoint written",
                    flush=True,
                )
            return

        time.sleep(0.2)
        state["step"] = step
        state["loss"] = 1.0 / (step + 1)

        if step > 0 and step % 50 == 0:
            with tempfile.TemporaryDirectory() as d:
                with open(os.path.join(d, "state.json"), "w") as f:
                    json.dump(state, f)
                ray.train.report(
                    {"loss": state["loss"], "step": step},
                    checkpoint=Checkpoint.from_directory(d),
                )
            print(f"[rank {rank}] periodic checkpoint at step {step}", flush=True)


def drain_node_via_gcs(node_id: str, deadline_seconds_from_now: int = 30) -> None:
    """Call Ray Core's drain_node GCS RPC — the same path GKE preemption uses."""
    from ray._private import services
    from ray.core.generated import autoscaler_pb2

    address = services.canonicalize_bootstrap_address_or_die(addr="auto")
    gcs_client = ray._raylet.GcsClient(address=address)
    deadline_ms = int((time.time() + deadline_seconds_from_now) * 1000)
    is_accepted, msg = gcs_client.drain_node(
        node_id,
        autoscaler_pb2.DrainNodeReason.Value("DRAIN_NODE_REASON_PREEMPTION"),
        "preemption_integration_demo: simulating node drain",
        deadline_ms,
    )
    if not is_accepted:
        raise RuntimeError(f"drain_node rejected for {node_id}: {msg}")
    print(
        f"[drain] node={node_id[:8]}... drained via GCS, "
        f"deadline=+{deadline_seconds_from_now}s",
        flush=True,
    )


def main():
    # 1. Cluster: head + 3 worker nodes. Training uses 2 (SPREAD across nodes).
    #    The 3rd worker node is a standby so the post-drain restart can find
    #    a feasible placement once one of the two used nodes is drained out.
    cluster = Cluster(
        initialize_head=True,
        connect=True,
        head_node_args={"num_cpus": 4, "resources": {"head_node_marker": 1}},
    )
    worker_nodes = [
        cluster.add_node(num_cpus=2, resources={"worker_node": 1})
        for _ in range(3)
    ]
    cluster.wait_for_nodes()

    alive = [n for n in ray.nodes() if n.get("Alive")]
    print(f"cluster: {len(alive)} alive nodes")
    for n in alive:
        labels = {k: v for k, v in n["Resources"].items() if not k.startswith("memory") and not k.startswith("object_store") and not k.startswith("node:")}
        print(f"  - {n['NodeID'][:8]}... {labels}")

    storage_dir = tempfile.mkdtemp(prefix="ray_train_preempt_demo_")
    print(f"storage_dir: {storage_dir}")

    # 2. Schedule the drain. After 15s (workers should be up by then), drain
    #    the FIRST worker node in our pool. SPREAD will have placed a rank
    #    there; the 3rd node stays as a standby for the restart.
    target_node_id = worker_nodes[0].node_id
    standby_node_id = worker_nodes[2].node_id
    print(
        f"drain target  : {target_node_id[:8]}...  (will be drained at t≈15s)\n"
        f"standby node  : {standby_node_id[:8]}...  (available for restart)",
        flush=True,
    )

    def inject_drain():
        time.sleep(15)
        try:
            drain_node_via_gcs(target_node_id, deadline_seconds_from_now=30)
        except Exception as e:
            print(f"[drain] failed: {e}", flush=True)

    threading.Thread(target=inject_drain, daemon=True).start()

    # 3. Run the trainer. 2 workers, each requiring worker_node:1, SPREAD.
    trainer = TorchTrainer(
        train_func,
        train_loop_config={"total_steps": 200},
        scaling_config=ScalingConfig(
            num_workers=2,
            use_gpu=False,
            resources_per_worker={"worker_node": 1},
            placement_strategy="SPREAD",
        ),
        run_config=RunConfig(
            name="preempt_demo",
            storage_path=storage_dir,
            failure_config=FailureConfig(
                max_failures=0,
                max_preemption_failures=1,
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
        cluster.shutdown()


if __name__ == "__main__":
    main()
