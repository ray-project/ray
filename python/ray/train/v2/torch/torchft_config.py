import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict

import ray
from ray.train.torch.config import (
    TorchConfig,
    _TorchBackend,
)
from ray.train.v2._internal.constants import TORCHFT_LIGHTHOUSE_ENV_VAR
from ray.train.v2._internal.execution.worker_group import WorkerGroup
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

    @property
    def has_replica_groups(self):
        return True


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

    Creates a separate process group per replica group by calling
    the parent _TorchBackend.on_start() once per replica group.
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

    def on_start(self, worker_group, backend_config: TorchftConfig):
        assert isinstance(worker_group, WorkerGroup)
        replica_groups = worker_group.get_replica_groups()

        # Bind super() eagerly — the zero-arg form relies on a __class__ cell
        # that doesn't transfer correctly into ThreadPoolExecutor submissions.
        parent_on_start = super().on_start
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(parent_on_start, rg, backend_config)
                for rg in replica_groups
            ]
            for f in futures:
                f.result()
