"""Reusable helpers for launching torch.distributed across Ray actors.

These are the placement / rendezvous primitives behind the ``torchrun_ray``
launcher — the same pattern the legacy ``air_benchmarks`` benchmark_util used to
stand up "vanilla torch": place one actor per GPU, elect rank 0 as the master,
and let each actor ``init_process_group("env://")``.
"""

import logging
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)


def assign_topology(node_ips: List[str]) -> List[Dict[str, int]]:
    """Pure: per-worker distributed topology from each worker's node IP.

    ``node_ips`` is ordered by global rank. Returns, per worker, its ``rank``,
    ``node_rank``, node-local ``local_rank``, and ``local_world_size``. Nodes
    are ranked by first appearance (so rank 0's node is node_rank 0).
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


def create_gpu_actor_group(actor_cls, num_workers: int, cpus_per_worker: int = 1):
    """One GPU bundle per worker via a PACK placement group; returns (actors, pg).

    The actor class must declare ``num_gpus=1``. PACK keeps a single-node job on
    one node; multi-node spills naturally.
    """
    import ray
    from ray.util.placement_group import placement_group
    from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

    pg = placement_group(
        [{"GPU": 1, "CPU": cpus_per_worker} for _ in range(num_workers)],
        strategy="PACK",
    )
    ray.get(pg.ready())
    actors = [
        actor_cls.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(
                placement_group=pg, placement_group_bundle_index=i
            )
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
