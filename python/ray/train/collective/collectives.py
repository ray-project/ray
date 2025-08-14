import logging
from typing import Optional, TypeVar

import ray
import ray.cloudpickle as pickle
from ray.train.v2._internal.execution.context import (
    get_train_context as get_internal_train_context,
)
from ray.train.v2._internal.execution.train_fn_utils import get_train_fn_utils
from ray.util.annotations import PublicAPI

# For reference, {1:1} is 19 bytes, {"1":"1"} is 21 bytes,
# and {"12345": "12345"} is 25 bytes.
_MAX_BROADCAST_SIZE_BYTES = 1000

T = TypeVar("T", bound=Optional[object])


logger = logging.getLogger(__file__)


@PublicAPI(stability="alpha")
def broadcast_from_rank_zero(data: T) -> T:
    """Broadcast small (<1kb) data from the rank 0 worker to all other workers.

    Serves as a barrier, meaning that all workers must call this method before
    the training function can continue.

    Example:

        .. testcode:
            :skipif: True

            from ray.train import get_context
            from ray.train.collective import broadcast_from_rank_zero
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
        TypeError: If the data is not pickleable.
    """
    # Validate data.
    if data is not None:
        data_bytes = len(pickle.dumps(data))
        if data_bytes > _MAX_BROADCAST_SIZE_BYTES:
            logger.warning(
                f"Data size {data_bytes} bytes exceeds the maximum broadcast "
                f"size of {_MAX_BROADCAST_SIZE_BYTES} bytes"
            )

    # Send data to all workers.
    # TODO (xgui): We should not expose get_synchronization_actor() from internal_context here.
    # Maybe create one public barrier API inside `TrainFnUtils`
    sync_actor = get_internal_train_context().get_synchronization_actor()
    train_context = get_train_fn_utils().get_context()

    return ray.get(
        sync_actor.broadcast_from_rank_zero.remote(
            world_rank=train_context.get_world_rank(),
            world_size=train_context.get_world_size(),
            data=data,
            caller_method_name="ray.train.collective.broadcast_from_rank_zero",
        )
    )


@PublicAPI(stability="alpha")
def barrier() -> None:
    """Create a barrier across all workers.

    All workers must call this method before the training function can continue.

    Example:

        .. testcode:
            :skipif: True

            from ray.train import get_context
            from ray.train.collective import barrier
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
    train_context = get_train_fn_utils().get_context()
    sync_actor = get_internal_train_context().get_synchronization_actor()
    return ray.get(
        sync_actor.broadcast_from_rank_zero.remote(
            world_rank=train_context.get_world_rank(),
            world_size=train_context.get_world_size(),
            data=None,
            caller_method_name="ray.train.collective.barrier",
        )
    )
