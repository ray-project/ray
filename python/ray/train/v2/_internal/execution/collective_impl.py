import logging
from typing import Any, List, Optional, TypeVar

import ray
import ray.cloudpickle as pickle
from ray.train.v2._internal.execution.checkpoint.sync_actor import (
    SynchronizationBarrierResetError,
)
from ray.train.v2._internal.execution.context import get_train_context

# For reference, {1:1} is 19 bytes, {"1":"1"} is 21 bytes,
# and {"12345": "12345"} is 25 bytes.
_MAX_BROADCAST_SIZE_BYTES = 1000


logger = logging.getLogger(__name__)
T = TypeVar("T")


def barrier() -> None:
    """
    Create a barrier across all training workers.
    """
    train_context = get_train_context()
    sync_actor = train_context.get_synchronization_actor()
    return ray.get(
        sync_actor.broadcast_from_rank_zero.remote(
            world_rank=train_context.get_world_rank(),
            world_size=train_context.get_world_size(),
            data=None,
            caller_method_name="ray.train.collective.barrier",
        )
    )


def broadcast_from_rank_zero(data: Any) -> Any:
    """Broadcast data from the rank 0 worker to all other workers.

    This method is used by the public API function :func:`ray.train.collective.broadcast_from_rank_zero`.
    Users should typically call ``ray.train.collective.broadcast_from_rank_zero()`` instead of calling this method directly.
    """
    # Validate data.
    if data is not None:
        data_bytes = len(pickle.dumps(data))
        if data_bytes > _MAX_BROADCAST_SIZE_BYTES:
            logger.warning(
                f"Data size {data_bytes} bytes exceeds the maximum broadcast "
                f"size of {_MAX_BROADCAST_SIZE_BYTES} bytes"
            )

    train_context = get_train_context()
    sync_actor = train_context.get_synchronization_actor()
    return ray.get(
        sync_actor.broadcast_from_rank_zero.remote(
            world_rank=train_context.get_world_rank(),
            world_size=train_context.get_world_size(),
            data=data,
            caller_method_name="ray.train.collective.broadcast_from_rank_zero",
        )
    )


def collective_all_gather(data: T, *, caller_method_name: str) -> Optional[List[T]]:
    """Gather one value from every training worker, ordered by world rank.

    Returns ``None`` if the barrier was reset while waiting, which happens when a
    worker dies and the replica group is replaced. Callers should fall back to
    their local value in that case rather than failing the training function.
    """
    train_context = get_train_context()
    sync_actor = train_context.get_synchronization_actor()
    try:
        return ray.get(
            sync_actor.collective_all_gather.remote(
                world_rank=train_context.get_world_rank(),
                world_size=train_context.get_world_size(),
                data=data,
                caller_method_name=caller_method_name,
            )
        )
    except ray.exceptions.RayTaskError as e:
        if not isinstance(e.cause, SynchronizationBarrierResetError):
            raise
        logger.warning(
            f"Synchronization barrier was reset during {caller_method_name} "
            "(likely due to a worker failure). Falling back to this worker's "
            "local value."
        )
        return None
