"""Ray Train release test: local mode launched by torchrun.

Setup:
- 2 x g4dn.12xlarge (4 GPU)

Test owner: xinyuangui2

The test launches a ray cluster with 2 nodes, and launches a torchrun job on each node.
"""
import os
import ray
import subprocess
import logging
from ray.air.util.node import _force_on_node
from pathlib import Path

logger = logging.getLogger(__name__)


@ray.remote
def _write(stream: bytes, path: str):
    Path(path).parent.mkdir(parents=True, exist_ok=True)

    with open(path, "wb") as f:
        f.write(stream)


@ray.remote
def _torch_run_launch(
    master_address: str,
    node_rank: int,
    absolute_path: str,
    n_nodes: int,
    n_processes_per_node: int,
    master_port: int,
):
    cmd = [
        "torchrun",
        f"--nnodes={n_nodes}",
        f"--nproc-per-node={n_processes_per_node}",
        f"--node_rank={node_rank}",
        "--rdzv_backend=c10d",
        f"--rdzv_endpoint={master_address}:{master_port}",
        "--rdzv_id=local_mode_job",
        absolute_path,
    ]

    env = os.environ.copy()
    env["RAY_TRAIN_V2_ENABLED"] = "1"

    subprocess.check_call(cmd, env=env)


def torch_run_launch_on_nodes():
    head_ip = ray.util.get_node_ip_address()
    node_id_ips = []
    for node in ray.nodes():
        if not node["Alive"]:
            continue

        node_ip = node["NodeManagerAddress"]

        if node_ip == head_ip:
            continue

        node_id = node["NodeID"]
        node_id_ips.append((node_id, node_ip))

    assert len(node_id_ips) == 2, f"Expected 2 nodes, got {len(node_id_ips)}"
    master_address = node_id_ips[0][1]
    futures = []
    absolute_path = os.path.abspath("torch_local_mode_test.py")
    with open(absolute_path, "rb") as f:
        stream = f.read()
    logger.info(f"Uploading file to all nodes: {absolute_path}")
    for i in range(len(node_id_ips)):
        futures.append(
            _force_on_node(node_id_ips[i][0], _write).remote(stream, absolute_path)
        )
    ray.get(futures)
    logger.info("Uploaded file to all nodes, starting torch run launch")
    futures = []
    for i in range(len(node_id_ips)):
        futures.append(
            _force_on_node(node_id_ips[i][0], _torch_run_launch).remote(
                master_address, i, absolute_path, len(node_id_ips), 4, 29500
            )
        )
    ray.get(futures)


if __name__ == "__main__":
    # https://docs.ray.io/en/latest/ray-core/scheduling/accelerators.html#using-accelerators-in-tasks-and-actors
    # we don't want actors to override CUDA_VISIBLE_DEVICES
    ray.init(
        "auto",
        runtime_env={"env_vars": {"RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES": "1"}},
    )
    torch_run_launch_on_nodes()
