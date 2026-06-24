"""Run the framework under raw torch.distributed, placed by Ray.

This is the "torchrun parity" baseline made runnable on a head/worker Ray
cluster *without* ssh-ing into GPU nodes. Ray is used only as the launcher:
it places one actor per GPU (borrowing its scheduler/placement), and we set the
standard torch.distributed env vars (RANK/WORLD_SIZE/MASTER_ADDR/...) ourselves,
exactly as torchrun would. Each actor then runs the same adapter via the
TorchrunContext path (vanilla `init_process_group("env://")`).

What this isolates: Ray Train's *orchestration* layer (controller, health
checks, worker-group management, checkpoint reporting) — held against the same
framework code on the same cluster, launch substrate (Ray) constant. For a
fully Ray-free number, use real torchrun via srun/ssh; this trades a small bit
of that purity for not needing node access.

Note on devices: Ray gives each actor one GPU via CUDA_VISIBLE_DEVICES, so every
process sees its GPU as cuda:0 and binds LOCAL_RANK=0 — distinct physical GPUs,
same as Ray Train does internally.
"""

import json
import logging
import os
from typing import Any, Dict, List

from core.experiment_config import ExperimentConfig
from core.train_context import default_metrics_path

logger = logging.getLogger(__name__)


def assign_topology(node_ips: List[str]) -> List[Dict[str, int]]:
    """Pure: per-worker distributed topology from each worker's node IP.

    ``node_ips`` is ordered by global rank. Returns, per worker, its
    ``rank``, ``node_rank``, node-local ``local_rank``, and ``local_world_size``.
    Nodes are ranked by first appearance (so rank 0's node is node_rank 0).
    """
    node_order: List[str] = []
    for ip in node_ips:
        if ip not in node_order:
            node_order.append(ip)
    per_node_total = {ip: node_ips.count(ip) for ip in node_order}

    seen: Dict[str, int] = {}
    topology = []
    for rank, ip in enumerate(node_ips):
        local_rank = seen.get(ip, 0)
        seen[ip] = local_rank + 1
        topology.append(
            {
                "rank": rank,
                "node_rank": node_order.index(ip),
                "local_rank": local_rank,
                "local_world_size": per_node_total[ip],
            }
        )
    return topology


def _extra_env_vars(cfg: ExperimentConfig) -> Dict[str, str]:
    env = {"PYTORCH_CUDA_ALLOC_CONF": "expandable_segments:True"}
    for key in ("HF_TOKEN", "HUGGING_FACE_HUB_TOKEN", "HF_HOME", "CUDA_HOME"):
        if os.environ.get(key):
            env[key] = os.environ[key]
    from data.text_dataset import shared_hf_cache

    cache = shared_hf_cache()
    if cache:
        env.setdefault("HF_HOME", cache)
    env.update(cfg.env_vars or {})
    return env


def run_with_torchrun_ray(cfg: ExperimentConfig) -> Dict[str, Any]:
    import ray
    from ray.util.placement_group import placement_group, remove_placement_group
    from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

    n = cfg.num_workers
    metrics_path = default_metrics_path(cfg.name)
    extra_env = _extra_env_vars(cfg)

    # One GPU bundle per worker; PACK keeps a single-node job on one node.
    pg = placement_group([{"GPU": 1, "CPU": 1} for _ in range(n)], strategy="PACK")
    ray.get(pg.ready())

    @ray.remote(num_gpus=1, num_cpus=1)
    class _TorchDistWorker:
        def node_ip(self) -> str:
            return ray.util.get_node_ip_address()

        def free_port(self) -> int:
            import socket

            s = socket.socket()
            s.bind(("", 0))
            port = s.getsockname()[1]
            s.close()
            return port

        def run(
            self,
            topology: Dict[str, int],
            world_size: int,
            master_addr: str,
            master_port: int,
            env: Dict[str, str],
        ) -> Dict[str, Any]:
            os.environ.update(
                {
                    "RANK": str(topology["rank"]),
                    "WORLD_SIZE": str(world_size),
                    # Ray restricts this actor to one visible GPU -> cuda:0.
                    "LOCAL_RANK": "0",
                    "LOCAL_WORLD_SIZE": str(topology["local_world_size"]),
                    "NODE_RANK": str(topology["node_rank"]),
                    "MASTER_ADDR": master_addr,
                    "MASTER_PORT": str(master_port),
                }
            )
            os.environ.update(env)
            from core.launchers.torchrun_launcher import run_with_torchrun

            return run_with_torchrun(cfg)

    workers = [
        _TorchDistWorker.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg, placement_group_bundle_index=i
            )
        ).remote()
        for i in range(n)
    ]

    try:
        node_ips = ray.get([w.node_ip.remote() for w in workers])
        topology = assign_topology(node_ips)
        master_addr = node_ips[0]
        master_port = ray.get(workers[0].free_port.remote())
        logger.info(
            f"torchrun_ray: world_size={n} master={master_addr}:{master_port} "
            f"nodes={sorted(set(node_ips))}"
        )

        results = ray.get(
            [
                workers[i].run.remote(
                    topology[i], n, master_addr, master_port, extra_env
                )
                for i in range(n)
            ]
        )
    finally:
        remove_placement_group(pg)

    # Only rank 0 returns populated metrics (others return {}).
    for result in results:
        if result:
            return result
    with open(metrics_path, "r") as f:
        return json.load(f)
