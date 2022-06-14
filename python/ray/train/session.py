import warnings
from typing import Dict, Optional, TYPE_CHECKING, Union

from ray.air.checkpoint import Checkpoint
from ray.air.session import Session

if TYPE_CHECKING:
    # avoid circular import
    from ray.train._internal.session import _TrainSession
    from ray.data import Dataset, DatasetPipeline


class TrainSession(Session):
    """Session client that "per worker train loop" can interact with.

    Notice that each worker will automatically switch to its working
    directory on entering the train loop. This is to ensure that
    each worker can safely write to a local directory without racing
    and overwriting each other."""

    def __init__(self, session: "_TrainSession"):
        self._session = session

    def report(self, metrics: Dict, *, checkpoint: Optional[Checkpoint] = None) -> None:
        self._session.report(metrics, checkpoint)

    @property
    def loaded_checkpoint(self) -> Optional[Checkpoint]:
        return self._session.loaded_checkpoint

    @property
    def trial_name(self) -> str:
        return self._session.trial_info.name

    @property
    def trial_id(self) -> str:
        return self._session.trial_info.id

    @property
    def trial_resources(self) -> Dict[str, float]:
        return self._session.trial_info.resources

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

    def get_dataset_shard(self,
        dataset_name: Optional[str] = None,
    ) -> Optional[Union["Dataset", "DatasetPipeline"]]:
        """Returns the Ray Dataset or DatasetPipeline shard for this worker.

        You should call ``to_torch()`` or ``to_tf()`` on this shard to convert
        it to the appropriate framework-specific Dataset.

        .. code-block:: python

            import ray
            from ray import train

            def train_func():
                model = Net()
                for iter in range(100):
                    session = get_session()
                    data_shard = session.get_dataset_shard().to_torch()
                    model.train(data_shard)
                return model

            dataset = ray.data.read_csv("train.csv")
            dataset.filter(...).repeat().random_shuffle()

            trainer = Trainer(backend="torch")
            trainer.start()

            # Trainer will automatically handle sharding.
            train_model = trainer.run(train_func, dataset=dataset)
            trainer.shutdown()

        Args:
            dataset_name: If a Dictionary of Datasets was passed to ``Trainer``, then
                specifies which dataset shard to return.

        Returns:
            The ``Dataset`` or ``DatasetPipeline`` shard to use for this worker.
            If no dataset is passed into Trainer, then return None.
        """
        shard = self._session.dataset_shard
        if shard is None:
            warnings.warn(
                "No dataset passed in. Returning None. Make sure to "
                "pass in a Ray Dataset to Trainer.run to use this "
                "function."
            )
        elif isinstance(shard, dict):
            if not dataset_name:
                raise RuntimeError(
                    "Multiple datasets were passed into ``Trainer``, "
                    "but no ``dataset_name`` is passed into "
                    "``get_dataset_shard``. Please specify which "
                    "dataset shard to retrieve."
                )
            return shard.get(dataset_name)
        return shard
