from typing import Dict, Optional, TYPE_CHECKING

from ray.air.checkpoint import Checkpoint
from ray.air.session import Session

if TYPE_CHECKING:
    # avoid circular import
    from ray.train._internal.session import _TrainSession


class TrainSession(Session):
    """Session client that "per worker train loop" can interact with.

    Notice that each worker will automatically switch to its working
    directory on entering the train loop. This is to ensure that
    each worker can safely write to a local directory without racing
    and overwriting each other."""

    def __init__(self, session: "_TrainSession"):
        self._session = session

    def report(self, metrics: Dict, checkpoint: Optional[Checkpoint] = None) -> None:
        self._session.report(metrics, checkpoint)

    @property
    def loaded_checkpoint(self) -> Optional[Checkpoint]:
        return self._session.loaded_checkpoint

    @property
    def world_size(self) -> int:
        """Get the current world size (i.e. total number of workers) for this run.

        .. code-block:: python

            import time
            from ray.air.session import get_session

            def train_func():
                assert get_session().world_size == 4

            trainer = Trainer(backend="torch", num_workers=4)
            trainer.start()
            trainer.run(train_func)
            trainer.shutdown()
        """
        return self._session.world_size

    @property
    def world_rank(self) -> int:
        """Get the world rank of this worker.

        .. code-block:: python

            import time
            from ray.air.session import get_session

            def train_func():
                for iter in range(100):
                    time.sleep(1)
                    if get_session().world_rank == 0:
                        print("Worker 0")

            trainer = Trainer(backend="torch")
            trainer.start()
            trainer.run(train_func)
            trainer.shutdown()
        """
        return self._session.world_rank

    @property
    def local_rank(self) -> int:
        """Get the local rank of this worker (rank of the worker on its node).

        .. code-block:: python

            import time
            from ray.air.session import get_session

            def train_func():
                if torch.cuda.is_available():
                    torch.cuda.set_device(get_session().local_rank)
                ...

            trainer = Trainer(backend="torch", use_gpu=True)
            trainer.start()
            trainer.run(train_func)
            trainer.shutdown()
        """
        return self._session.local_rank
