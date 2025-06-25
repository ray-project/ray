import pickle
from typing import Any, Dict, Optional

import ray
from ray.train.v2._internal.execution.context import get_train_context
from ray.util.annotations import PublicAPI

# For reference, {1:1} is 19 bytes, {"1":"1"} is 21 bytes,
# and {"12345": "12345"} is 25 bytes.
_MAX_BROADCAST_SIZE_BYTES = 1000


@PublicAPI(stability="beta")
def broadcast_from_rank_zero(
    data: Optional[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """Broadcast small (<1kb) data from the rank 0 worker to all other workers.

    Serves as a barrier, meaning that all workers must call this method before
    the training function can continue.

    Example:

        .. testcode:
            :skipif: True

            from ray.train import get_context
            from ray.train.collectives import broadcast_from_rank_zero
            from ray.train.torch import TorchTrainer

            def train_func():
                ...
                if get_context().get_world_rank() == 0:
                    data = {"some_key": "some_value"}
                else:
                    data = None
                data = broadcast_from_rank_zero(data)
                ...

            trainer = TorchTrainer(train_func)
            trainer.fit()

    Args:
        data: The small (1kb) data to broadcast from the rank 0 worker to all
            other workers.

    Returns:
        The data broadcasted from the rank 0 worker.

    Raises:
        ValueError: If the data is too big.
        pickle.PicklingError: If the data is not pickleable.
        TypeError: If the data is not a dictionary or pickleable.
    """
    # Validate data.
    if data is not None:
        if not isinstance(data, dict):
            raise TypeError("data must be of type Dict[str, Any], got {type(data)}")
        for key in data.keys():
            if not isinstance(key, str):
                raise TypeError(
                    f"data must be of type Dict[str, Any], but the key {key} is of "
                    f"type {type(key)}"
                )
    data_bytes = pickle.dumps(data)
    if len(data_bytes) > _MAX_BROADCAST_SIZE_BYTES:
        raise ValueError(
            f"Data size {len(data_bytes)} bytes exceeds the maximum broadcast "
            f"size of {_MAX_BROADCAST_SIZE_BYTES} bytes"
        )

    # Send data to all workers.
    train_context = get_train_context()
    sync_actor = train_context.get_synchronization_actor()
    return ray.get(
        sync_actor.broadcast_from_rank_zero.remote(
            world_rank=train_context.get_world_rank(),
            world_size=train_context.get_world_size(),
            data=data,
            barrier_method="ray.train.collectives.broadcast_from_rank_zero",
        )
    )


@PublicAPI(stability="beta")
def barrier() -> None:
    """Create a barrier across all workers.

    All workers must call this method before the training function can continue.

    Example:

        .. testcode:
            :skipif: True

            from ray.train import get_context
            from ray.train.collectives import barrier
            from ray.train.torch import TorchTrainer

            def train_func():
                ...
                print(f"Rank {get_context().get_world_rank()} is waiting at the barrier.")
                barrier()
                print(f"Rank {get_context().get_world_rank()} has passed the barrier.")
                ...

            trainer = TorchTrainer(train_func)
            trainer.fit()
    """
    train_context = get_train_context()
    sync_actor = train_context.get_synchronization_actor()
    return ray.get(
        sync_actor.broadcast_from_rank_zero.remote(
            world_rank=train_context.get_world_rank(),
            world_size=train_context.get_world_size(),
            data=None,
            barrier_method="ray.train.collectives.barrier",
            has_data=False,
        )
    )
