import logging
from typing import Any

import ray
import ray.cloudpickle as pickle
from ray.train.v2._internal.execution.context import get_train_context

# For reference, {1:1} is 19 bytes, {"1":"1"} is 21 bytes,
# and {"12345": "12345"} is 25 bytes.
_MAX_BROADCAST_SIZE_BYTES = 1000


logger = logging.getLogger(__name__)


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
