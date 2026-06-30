"""Run the framework under raw torch.distributed, placed by Ray.

The torch.distributed parity baseline made runnable on a head/worker Ray cluster
without ssh-ing into GPU nodes. Ray is the launcher only: it places one actor
per GPU (via its scheduler), and we set the standard torch.distributed env vars
(RANK/WORLD_SIZE/MASTER_ADDR/...) ourselves, exactly as torchrun would. Each
actor then runs the same adapter via ``init_process_group("env://")``.

This isolates Ray Train's *orchestration* layer (controller, health checks,
checkpoint reporting) while holding the launch substrate (Ray) constant — the
same approach the legacy air_benchmarks used to run "vanilla torch".

Devices: Ray gives each actor one GPU via CUDA_VISIBLE_DEVICES, so every process
sees its GPU as cuda:0 and binds LOCAL_RANK=0 (distinct physical GPUs).
"""

import logging
import os
from typing import Any, Dict

from core.experiment_config import ExperimentConfig
from core.launchers.ray_actor_utils import (
    assign_topology,  # noqa: F401 - re-exported for tests/back-compat
    create_gpu_actor_group,
    elect_rendezvous,
)
from core.registry import get_adapter_cls
from core.train_context import TorchrunContext

logger = logging.getLogger(__name__)


def _run_in_worker(
    cfg: ExperimentConfig,
    topology: Dict[str, int],
    world_size: int,
    master_addr: str,
    master_port: int,
    env: Dict[str, str],
) -> Dict[str, Any]:
    """Set the torch.distributed env, init the process group, run the adapter.

    Runs inside a Ray actor. Returns the adapter's metrics on rank 0, {} else.
    """
    import torch
    import torch.distributed as dist

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

    backend = "nccl" if torch.cuda.is_available() else "gloo"
    if not dist.is_initialized():
        dist.init_process_group(backend=backend)

    ctx = TorchrunContext(cfg.name)
    metrics = get_adapter_cls(cfg.adapter)(cfg, ctx).run()

    dist.barrier()
    dist.destroy_process_group()
    return metrics if ctx.world_rank == 0 else {}


def run_with_torchrun_ray(cfg: ExperimentConfig) -> Dict[str, Any]:
    import ray
    from ray.util.placement_group import remove_placement_group

    env = dict(cfg.env_vars or {})

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
            return _run_in_worker(
                cfg, topology, world_size, master_addr, master_port, env
            )

    actors, pg = create_gpu_actor_group(_TorchDistWorker, cfg.scaling.num_workers)
    try:
        node_ips, topology, master_addr, master_port = elect_rendezvous(actors)
        logger.info(
            f"torchrun_ray: world_size={cfg.scaling.num_workers} "
            f"master={master_addr}:{master_port} nodes={sorted(set(node_ips))}"
        )
        results = ray.get(
            [
                actors[i].run.remote(
                    topology[i], cfg.scaling.num_workers, master_addr, master_port, env
                )
                for i in range(cfg.scaling.num_workers)
            ]
        )
    finally:
        remove_placement_group(pg)

    for result in results:  # only rank 0 returns populated metrics
        if result:
            return result
    return {}
