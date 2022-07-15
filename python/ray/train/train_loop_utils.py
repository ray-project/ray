import warnings
from typing import TYPE_CHECKING, Dict, Optional, Union

from ray.train._internal.session import get_session
from ray.train.constants import SESSION_MISUSE_LOG_ONCE_KEY
from ray.util import PublicAPI, log_once

if TYPE_CHECKING:
    from ray.data import Dataset, DatasetPipeline


def _warn_session_misuse(fn_name: str):
    """Logs warning message on provided fn being used outside of session.

    Args:
        fn_name: The name of the function to warn about.
    """

    if log_once(f"{SESSION_MISUSE_LOG_ONCE_KEY}-{fn_name}"):
        warnings.warn(
            f"`train.{fn_name}()` is meant to only be "
            f"called "
            "inside a training function that is executed by "
            "`Trainer.run`. Returning None."
        )


@PublicAPI(stability="beta")
def get_dataset_shard(
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
                data_shard = train.get_dataset_shard().to_torch()
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
    session = get_session()
    if session is None:
        _warn_session_misuse(get_dataset_shard.__name__)
        return
    shard = session.dataset_shard
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


@PublicAPI(stability="beta")
def report(**kwargs) -> None:
    """Reports all keyword arguments to Train as intermediate results.

    .. code-block:: python

        import time
        from ray import train

        def train_func():
            for iter in range(100):
                time.sleep(1)
                train.report(hello="world")

        trainer = Trainer(backend="torch")
        trainer.start()
        trainer.run(train_func)
        trainer.shutdown()

    Args:
        **kwargs: Any key value pair to be reported by Train.
            If callbacks are provided, they are executed on these
            intermediate results.
    """
    session = get_session()
    if session is None:
        _warn_session_misuse(report.__name__)
        return
    session._report_legacy(**kwargs)


@PublicAPI(stability="beta")
def world_rank() -> int:
    """Get the world rank of this worker.

    .. code-block:: python

        import time
        from ray import train

        def train_func():
            for iter in range(100):
                time.sleep(1)
                if train.world_rank() == 0:
                    print("Worker 0")

        trainer = Trainer(backend="torch")
        trainer.start()
        trainer.run(train_func)
        trainer.shutdown()

    """
    session = get_session()
    if session is None:
        return 0
    return session.world_rank


@PublicAPI(stability="beta")
def local_rank() -> int:
    """Get the local rank of this worker (rank of the worker on its node).

    .. code-block:: python

        import time
        from ray import train

        def train_func():
            if torch.cuda.is_available():
                torch.cuda.set_device(train.local_rank())
            ...

        trainer = Trainer(backend="torch", use_gpu=True)
        trainer.start()
        trainer.run(train_func)
        trainer.shutdown()

    """
    session = get_session()
    if session is None:
        return 0
    return session.local_rank


@PublicAPI(stability="beta")
def load_checkpoint() -> Optional[Dict]:
    """Loads checkpoint data onto the worker.

    .. code-block:: python

        from ray import train

        def train_func():
            checkpoint = train.load_checkpoint()
            for iter in range(checkpoint["epoch"], 5):
                print(iter)

        trainer = Trainer(backend="torch")
        trainer.start()
        trainer.run(train_func, checkpoint={"epoch": 3})
        # 3
        # 4
        trainer.shutdown()

    Args:
        **kwargs: Any key value pair to be checkpointed by Train.
    Returns:
        The most recently saved checkpoint if ``train.save_checkpoint()``
        has been called. Otherwise, the checkpoint that the session was
        originally initialized with. ``None`` if neither exist.
    """
    session = get_session()
    if session is None:
        _warn_session_misuse(load_checkpoint.__name__)
        return
    return session.loaded_checkpoint


@PublicAPI(stability="beta")
def save_checkpoint(**kwargs) -> None:
    """Checkpoints all keyword arguments to Train as restorable state.

    .. code-block:: python

        import time
        from ray import train

        def train_func():
            for iter in range(100):
                time.sleep(1)
                train.save_checkpoint(epoch=iter)

        trainer = Trainer(backend="torch")
        trainer.start()
        trainer.run(train_func)
        trainer.shutdown()

    Args:
        **kwargs: Any key value pair to be checkpointed by Train.
    """
    session = get_session()
    if session is None:
        _warn_session_misuse(save_checkpoint.__name__)
        return
    session.checkpoint(**kwargs)


@PublicAPI(stability="beta")
def world_size() -> int:
    """Get the current world size (i.e. total number of workers) for this run.

    .. code-block:: python

        import time
        from ray import train

        def train_func():
            assert train.world_size() == 4

        trainer = Trainer(backend="torch", num_workers=4)
        trainer.start()
        trainer.run(train_func)
        trainer.shutdown()
    """
    session = get_session()
    if session is None:
        return 1
    return session.world_size
