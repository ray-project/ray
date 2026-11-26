import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict

import ray
from ray.train.torch.config import (
    TorchConfig,
    _TorchBackend,
)
from ray.train.v2._internal.constants import TORCHFT_LIGHTHOUSE_ADDR_ENV_VAR
from ray.train.v2._internal.execution.worker_group import ReplicaGroup, WorkerGroup
from ray.util.annotations import PublicAPI
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class TorchftConfig(TorchConfig):
    """Configuration for fault-tolerant training with torchft.

    Pass this as the ``torch_config`` of a
    :class:`~ray.train.torch.TorchTrainer` to train with
    `torchft <https://github.com/meta-pytorch/torchft>`_. Ray Train then
    starts the torchft Lighthouse server as an actor on the node that runs
    the Ray Train controller, which is usually the head node. It sets the
    ``TORCHFT_LIGHTHOUSE`` environment variable on every training worker
    and creates one process group per replica group instead of one
    process group across all workers. Each Ray Train worker is its own
    replica group. When a worker fails, Ray Train replaces only that
    replica group instead of restarting the whole worker group, and the
    replacement recovers model and optimizer state from a live peer
    through torchft.

    Your training function must also create a ``torchft.Manager`` and
    wrap the model and optimizer with the torchft primitives. See
    :ref:`train-torchft` for a complete example.

    Example:

        .. testcode::
            :skipif: True

            from ray.train import FailureConfig, RunConfig, ScalingConfig
            from ray.train.torch import TorchftConfig, TorchTrainer

            trainer = TorchTrainer(
                train_func,
                scaling_config=ScalingConfig(num_workers=2),
                torch_config=TorchftConfig(
                    lighthouse_kwargs={"min_replicas": 1},
                    backend="gloo",
                ),
                run_config=RunConfig(
                    failure_config=FailureConfig(max_failures=3),
                ),
            )

    Args:
        lighthouse_kwargs: Keyword arguments for the
            ``torchft.coordination.LighthouseServer`` constructor.
            ``min_replicas`` is required: the minimum number of replica
            groups that must be alive to form a quorum and keep training.
            ``bind`` defaults to ``"[::]:0"``, which binds the Lighthouse
            to a free port on the Lighthouse's node. The torchft version you
            install defines the remaining keys, such as
            ``join_timeout_ms``, ``quorum_tick_ms``, and
            ``heartbeat_timeout_ms``.
        **kwargs: Additional keyword arguments for the
            :class:`~ray.train.torch.TorchConfig` constructor, such as
            ``backend="gloo"`` or ``backend="nccl"``.
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


class _TorchftBackend(_TorchBackend):
    """Backend for torchft-based fault-tolerant training with replica groups.

    Creates a separate process group per replica group by calling
    the parent _TorchBackend.on_start() once per replica group.
    """

    has_replica_groups: bool = True

    def __init__(self):
        super().__init__()
        self.lighthouse_actor = None

    def _maybe_create_lighthouse_actor(self, backend_config: TorchftConfig) -> str:
        """Create lighthouse actor if it doesn't exist and return its address."""
        if self.lighthouse_actor is not None:
            # Intentionally read address from actor in case it was restarted.
            return ray.get(self.lighthouse_actor.address.remote())

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
        return lighthouse_address

    def on_start(self, worker_group, backend_config: TorchftConfig):
        lighthouse_address = self._maybe_create_lighthouse_actor(backend_config)

        # Push the lighthouse address to all workers in this group.
        # Necessary because workers were already started before on_start runs.
        def _set_lighthouse_address(addr: str):
            os.environ[TORCHFT_LIGHTHOUSE_ADDR_ENV_VAR] = addr

        worker_group.execute(_set_lighthouse_address, lighthouse_address)

        # Bind super() eagerly — the zero-arg form relies on a __class__ cell
        # that doesn't transfer correctly into ThreadPoolExecutor submissions.
        parent_on_start = super().on_start

        if isinstance(worker_group, ReplicaGroup):
            # Single replica group replacement — just start this one.
            parent_on_start(worker_group, backend_config)
        else:
            # Full worker group startup — start all replica groups in parallel.
            assert isinstance(worker_group, WorkerGroup)
            replica_groups = worker_group.get_replica_groups()
            with ThreadPoolExecutor() as executor:
                futures = [
                    executor.submit(parent_on_start, rg, backend_config)
                    for rg in replica_groups
                ]
                for f in futures:
                    f.result()
