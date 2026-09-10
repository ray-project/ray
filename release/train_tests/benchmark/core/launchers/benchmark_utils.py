"""Reusable helpers for launching torch.distributed across Ray actors.

These are the placement / rendezvous primitives behind the
``ray_torch_distributed`` launcher — the same pattern the legacy ``air_benchmarks`` benchmark_util used to
stand up "vanilla torch": place one actor per GPU, elect rank 0 as the master,
and let each actor ``init_process_group("env://")``.
"""

import logging
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)


def assign_topology(node_ips: List[str]) -> List[Dict[str, int]]:
    """Pure: per-worker distributed topology from each worker's node IP.

    ``node_ips`` is ordered by global rank. Returns, per worker, its ``rank``,
    ``node_rank``, and ``local_world_size``. Nodes are ranked by first
    appearance (so rank 0's node is node_rank 0). No local_rank: each Ray
    actor sees exactly one GPU, so its local device is always cuda:0.
    """
    node_order: List[str] = []
    for ip in node_ips:
        if ip not in node_order:
            node_order.append(ip)
    per_node_total = {ip: node_ips.count(ip) for ip in node_order}

    return [
        {
            "rank": rank,
            "node_rank": node_order.index(ip),
            "local_world_size": per_node_total[ip],
        }
        for rank, ip in enumerate(node_ips)
    ]


def create_gpu_actor_group(
    actor_cls,
    num_workers: int,
    cpus_per_worker: int = 1,
    gpus_per_worker: int = 1,
    runtime_env: Dict | None = None,
):
    """One GPU bundle per worker via a PACK placement group; returns (actors, pg).

    PACK keeps a single-node job on one node; multi-node spills naturally.

    ``gpus_per_worker > 1`` reserves multiple GPUs per actor (the
    air_benchmarks benchmark_util pattern) — for node-level launchers that
    fork one subprocess per GPU. The in-process ray_torch_distributed launcher
    drives exactly one GPU per rank, so it keeps the default of 1.
    """
    import ray
    from ray.util.placement_group import placement_group
    from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

    pg = placement_group(
        [{"GPU": gpus_per_worker, "CPU": cpus_per_worker} for _ in range(num_workers)],
        strategy="PACK",
    )
    ray.get(pg.ready())
    actors = [
        actor_cls.options(
            # Match the bundle so the actor owns what the PG reserved — and so
            # Ray sets OMP_NUM_THREADS=num_cpus, keeping PyTorch's CPU thread
            # pool consistent with what the config requested (and symmetric
            # with the ray_train launcher's resources_per_worker).
            num_cpus=cpus_per_worker,
            num_gpus=gpus_per_worker,
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg, placement_group_bundle_index=i
            ),
            runtime_env=runtime_env,
        ).remote()
        for i in range(num_workers)
    ]
    return actors, pg


def elect_rendezvous(actors) -> Tuple[List[str], List[Dict[str, int]], str, int]:
    """Gather node IPs (rank order), build topology, pick rank-0 as master.

    The actor class must expose ``node_ip()`` and ``free_port()`` remote methods.
    Returns (node_ips, topology, master_addr, master_port).
    """
    import ray

    node_ips = ray.get([a.node_ip.remote() for a in actors])
    topology = assign_topology(node_ips)
    master_addr = node_ips[0]
    master_port = ray.get(actors[0].free_port.remote())
    return node_ips, topology, master_addr, master_port
