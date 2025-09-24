import os
import ray
import subprocess
from ray.air.util.node import _force_on_node


@ray.remote
def _torch_run_launch(
    master_address: str,
    node_rank: int,
    n_nodes: int = 4,
    n_processes_per_node: int = 4,
    master_port: str = 29500,
):
    cmd_node1 = [
        "torchrun",
        f"--nnodes={n_nodes}",
        f"--nproc-per-node={n_processes_per_node}",
        f"--node_rank={node_rank}",
        f"--master_addr={master_address}",
        f"--master_port={master_port}",
        "torch_local_mode_test.py",
    ]

    env = os.environ.copy()
    env["RAY_TRAIN_V2_ENABLED"] = "1"

    subprocess.check_call(cmd_node1, env=env)


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

    assert len(node_id_ips) == 4, f"Expected 4 nodes, got {len(node_id_ips)}"
    master_address = node_id_ips[0][1]
    futures = []
    for i in range(len(node_id_ips)):
        futures.append(
            _force_on_node(node_id_ips[i][0], _torch_run_launch).remote(
                master_address, i
            )
        )
    ray.get(futures)


if __name__ == "__main__":
    ray.init("auto")
    torch_run_launch_on_nodes()
