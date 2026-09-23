"""Run a real NCCL job and capture what `ncclras` actually reports.

    python release/train_tests/health/01_nccl_ras.py

This is Step 1 of CLUSTER_SETUP.md, and the highest-value thing to do on a GPU
cluster first: every RAS fixture in our tests is hand-written, so one real
report from your hardware is worth more than another unit test.

Why a bare `ncclras` on an idle node fails: RAS is not a daemon. Its monitoring
threads live inside the NCCL processes, so the service on 127.0.0.1:28028 only
exists while a job with initialized communicators is running, and only on nodes
hosting a rank of it. Querying an idle node gets:

    Connecting to 127.0.0.1:28028: Connection refused
    Failed to connect to the NCCL RAS service!

So this script starts a training job, waits for the communicators to exist,
queries every GPU node until one answers, and writes the output next to a
matching `nvidia-smi -q`.

Pass --hang to have one rank stop calling the collective partway through, so
the captured report is of a *wedged* communicator rather than a healthy one.
That is the input the detector actually has to parse.
"""
import argparse
import json
import subprocess
import sys
import threading
import time
from pathlib import Path

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

DEFAULT_OUT = Path("ras_fixtures")
RAS_PORT_HINT = "NCCL RAS listens on 127.0.0.1:28028 inside each rank's process."


def train_func(config):
    """A minimal all-reduce loop, long enough to be queried while it runs."""
    import time

    import torch
    import torch.distributed as dist

    import ray.train
    from ray.train.v2._internal.execution.health.testing import symptoms

    rank = ray.train.get_context().get_world_rank()
    device = torch.device(f"cuda:{torch.cuda.current_device()}")
    tensor = torch.ones(1024, 1024, device=device)

    # One rank stops participating at `hang_step`, leaving its collective count
    # one behind while every peer blocks inside the all-reduce. That is the
    # exact shape `mismatched_comms` keys on.
    desync = symptoms.CollectiveDesync(
        rank=config["hang_rank"], start_step=config["hang_step"]
    )

    for step in range(config["steps"]):
        if config["hang"] and desync.maybe_skip(step):
            # Sit out the collective, and keep the process alive so RAS can
            # still report on it.
            time.sleep(config["steps"])
            continue
        dist.all_reduce(tensor)
        torch.cuda.synchronize()
        if step % 20 == 0:
            print(f"rank {rank} step {step}", flush=True)
        time.sleep(0.05)


@ray.remote(num_cpus=0)
def _run(cmd: str):
    import socket

    proc = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return socket.gethostname(), proc.returncode, proc.stdout, proc.stderr


def on_node(node_id: str, cmd: str, timeout: float = 120.0):
    ref = _run.options(
        scheduling_strategy=NodeAffinitySchedulingStrategy(node_id=node_id, soft=False)
    ).remote(cmd)
    return ray.get(ref, timeout=timeout)


def gpu_nodes():
    return [n for n in ray.nodes() if n["Alive"] and n["Resources"].get("GPU", 0) > 0]


def capture(out_dir: Path, wait_s: float) -> int:
    print(f"waiting {wait_s:.0f}s for communicators to come up ...", flush=True)
    time.sleep(wait_s)

    nodes = gpu_nodes()
    print(f"querying {len(nodes)} GPU node(s). {RAS_PORT_HINT}", flush=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    captured = 0
    for node in nodes:
        node_id = node["NodeID"]
        ip = node.get("NodeManagerAddress", node_id[:12])

        host, rc, stdout, stderr = on_node(node_id, "ncclras -f json -t 5")
        if rc != 0 or not stdout.strip():
            # Expected on any node not hosting a rank of this job.
            reason = (stderr or "").strip().splitlines()[:1]
            print(f"  {host}: no RAS service {reason}")
            continue

        try:
            parsed = json.loads(stdout)
            n_comms = len(parsed.get("communicators", []))
        except json.JSONDecodeError:
            n_comms = "unparseable"

        (out_dir / f"ncclras_{ip}.json").write_text(stdout)
        print(f"  {host}: captured RAS report, {n_comms} communicator(s)")

        # The human-readable form, which a Diagnose pushes at hang time.
        _, _, text, _ = on_node(node_id, "ncclras -f text -t 5")
        if text.strip():
            (out_dir / f"ncclras_{ip}.txt").write_text(text)

        # And the GPU snapshot, so `_parse_nvidia_smi` can be checked against
        # real output instead of the invented text it was written against.
        _, smi_rc, smi, _ = on_node(node_id, "nvidia-smi -q")
        if smi_rc == 0 and smi.strip():
            (out_dir / f"nvidia-smi_{ip}.txt").write_text(smi)

        captured += 1

    if not captured:
        print(
            "\nNo node answered. Either the job had not created its "
            "communicators yet (raise --wait), or the workers are not where "
            "you think (check `ray.train` logs for the worker node ips), or "
            "NCCL_RAS_ENABLE is not set in the worker environment.",
            file=sys.stderr,
        )
        return 1

    print(f"\nwrote {captured} node(s) into {out_dir.resolve()}")
    print("Keep these. Pin them as test fixtures.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--steps", type=int, default=4000)
    parser.add_argument("--wait", type=float, default=90.0)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument(
        "--hang",
        action="store_true",
        help="Wedge one rank, so the captured report is of a stalled "
        "communicator rather than a healthy one.",
    )
    parser.add_argument("--hang-rank", type=int, default=1)
    parser.add_argument("--hang-step", type=int, default=100)
    args = parser.parse_args()

    from ray.train import RunConfig, ScalingConfig
    from ray.train.torch import TorchTrainer

    ray.init(address="auto")

    trainer = TorchTrainer(
        train_func,
        train_loop_config={
            "steps": args.steps,
            "hang": args.hang,
            "hang_rank": args.hang_rank,
            "hang_step": args.hang_step,
        },
        scaling_config=ScalingConfig(num_workers=args.workers, use_gpu=True),
        run_config=RunConfig(
            # RAS is what we are here to query; make sure it is on.
            worker_runtime_env={"env_vars": {"NCCL_RAS_ENABLE": "1"}},
        ),
    )

    # fit() blocks, so run it alongside the capture. The daemon thread dies
    # with the process once we have what we came for.
    threading.Thread(target=trainer.fit, daemon=True).start()
    return capture(args.out, args.wait)


if __name__ == "__main__":
    sys.exit(main())
