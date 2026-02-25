import logging
import os
from typing import Any, Dict, List, Optional

import torch.distributed as dist

import ray
from ray.train._internal.base_worker_group import BaseWorkerGroup
from ray.train.torch.config import (
    TorchConfig,
    _get_backend,
    _set_torch_distributed_env_vars,
    _setup_torch_process_group_with_master,
    _TorchBackend,
)
from ray.train.v2._internal.constants import TORCHFT_LIGHTHOUSE_ENV_VAR
from ray.train.v2.backend import BeforeWorkerGroupStartMixin
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

logger = logging.getLogger(__name__)


class TorchftConfig(TorchConfig):
    """Configuration for torchft-based fault tolerant training.

    See https://github.com/meta-pytorch/torchft for more info.

    Args:
        lighthouse_kwargs: Keyword arguments to pass to the torchft.Lighthouse constructor.
        **kwargs: Additional keyword arguments to pass to the TorchConfig constructor.
    """

    def __init__(self, lighthouse_kwargs: Dict[str, Any], **kwargs):
        self.lighthouse_kwargs = lighthouse_kwargs
        super().__init__(**kwargs)

    @property
    def backend_cls(self):
        return _TorchftBackend


@ray.remote
class LighthouseServerActor:
    """Actor that runs the torchft.Lighthouse server.

    ray.remote(LighthouseServer) does not work because it is a PyO3 type.
    """

    def __init__(self, lighthouse_kwargs: Dict[str, Any]):
        from torchft.coordination import LighthouseServer

        self.lighthouse = LighthouseServer(**lighthouse_kwargs)

    def address(self) -> str:
        return self.lighthouse.address()


class _TorchftBackend(_TorchBackend, BeforeWorkerGroupStartMixin):
    """Backend for torchft-based fault-tolerant training with replica groups.

    Creates a separate TCPStore and process group per replica group,
    matching the torchrun model.
    """

    def __init__(self):
        super().__init__()
        self.lighthouse_actor = None

    def before_worker_group_start(self, backend_config: TorchftConfig):
        if self.lighthouse_actor is not None:
            return

        # Let the OS pick a free port by default
        if "bind" in backend_config.lighthouse_kwargs:
            lighthouse_kwargs = backend_config.lighthouse_kwargs
        else:
            lighthouse_kwargs = {"bind": "[::]:0"} | backend_config.lighthouse_kwargs

        # Store reference so the actor lives as long as the backend/controller.
        self.lighthouse_actor = LighthouseServerActor.options(
            # Schedule lightweight lighthouse actor on head node
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=ray.get_runtime_context().get_node_id(),
                soft=False,
            )
        ).remote(lighthouse_kwargs=lighthouse_kwargs)
        lighthouse_address = ray.get(self.lighthouse_actor.address.remote())
        logger.info(f"Created torchft lighthouse at {lighthouse_address}")
        os.environ[TORCHFT_LIGHTHOUSE_ENV_VAR] = lighthouse_address

    def on_start(
        self,
        worker_group: BaseWorkerGroup,
        backend_config: TorchftConfig,
        workers_subset: Optional[List[int]] = None,
    ):
        if not dist.is_available():
            raise RuntimeError("Distributed torch is not available.")

        backend = _get_backend(worker_group, backend_config)

        num_workers = len(worker_group)
        # TODO: when we support model parallelism, we can get device mesh config from the
        # worker_group object.
        num_replica_groups = num_workers
        num_workers_per_replica_group = 1

        # Determine which groups need initialization based on the subset
        if workers_subset is not None:
            group_ids = set()
            for rank in workers_subset:
                group_ids.add(rank // num_workers_per_replica_group)
        else:
            group_ids = set(range(num_replica_groups))

        for group_id in group_ids:
            group_ranks = list(
                range(
                    group_id * num_workers_per_replica_group,
                    (group_id + 1) * num_workers_per_replica_group,
                )
            )
            _setup_torch_process_group_with_master(
                worker_group, backend_config, backend, group_ranks
            )

    def on_training_start(
        self,
        worker_group: BaseWorkerGroup,
        backend_config: TorchftConfig,
        workers_subset: Optional[List[int]] = None,
    ):
        if workers_subset is not None:
            ranks_to_init = workers_subset
        else:
            ranks_to_init = list(range(len(worker_group)))

        for global_rank in ranks_to_init:
            worker_group.execute_single(
                global_rank,
                _set_torch_distributed_env_vars,
            )
