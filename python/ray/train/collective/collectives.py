import logging
from typing import Optional, TypeVar

from ray.train.v2._internal.execution.train_fn_utils import get_train_fn_utils
from ray.train.v2._internal.util import requires_train_worker
from ray.util.annotations import PublicAPI

T = TypeVar("T", bound=Optional[object])


logger = logging.getLogger(__file__)


@PublicAPI(stability="alpha")
@requires_train_worker()
def broadcast_from_rank_zero(data: T) -> T:
    """Broadcast small (<1kb) data from the rank 0 worker to all other workers.

    Serves as a barrier, meaning that all workers must call this method before
    the training function can continue.

    Example:

        .. testcode:

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
    return get_train_fn_utils().broadcast_from_rank_zero(data)


@PublicAPI(stability="alpha")
@requires_train_worker()
def barrier() -> None:
    """Create a barrier across all workers.

    All workers must call this method before the training function can continue.

    Example:

        .. testcode:

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
    return get_train_fn_utils().barrier()
