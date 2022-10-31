from typing import TYPE_CHECKING, Dict, Optional, Union

from ray.util.annotations import Deprecated

if TYPE_CHECKING:
    from ray.data import Dataset, DatasetPipeline


def _get_deprecation_msg(is_docstring: bool, fn_name: Optional[str] = None):
    if is_docstring:
        session_api_link = ":ref:`ray.air.session <air-session-ref>`"
    else:
        session_api_link = (
            "`ray.air.session` ( "
            "https://docs.ray.io/en/latest/ray-air/package-ref.html"
            "#module-ray.air.session"
            ") ."
        )

    deprecation_msg = (
        f"The `train.{fn_name}` APIs are deprecated in Ray "
        f"2.1, and is replaced by {session_api_link}"
        "The `ray.air.session` APIs provide the same functionality, "
        "but in a unified manner across Ray Train and Ray Tune."
    )
    return deprecation_msg


@Deprecated(message=_get_deprecation_msg(is_docstring=True))
def get_dataset_shard(
    dataset_name: Optional[str] = None,
) -> Optional[Union["Dataset", "DatasetPipeline"]]:
    """Returns the Ray Dataset or DatasetPipeline shard for this worker.

    Call :meth:`~ray.data.Dataset.iter_torch_batches` or
    :meth:`~ray.data.Dataset.to_tf` on this shard to convert it to the appropriate
    framework-specific data type.

    .. code-block:: python

        import ray
        from ray import train

        def train_func():
            model = Net()
            for iter in range(100):
                data_shard = session.get_dataset_shard("train")
                for batch in data_shard.iter_torch_batches():
                    # ...
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
    raise DeprecationWarning(
        _get_deprecation_msg(is_docstring=False, fn_name=get_dataset_shard.__name__),
    )


@Deprecated(message=_get_deprecation_msg(is_docstring=True))
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
    raise DeprecationWarning(
        _get_deprecation_msg(is_docstring=False, fn_name=report.__name__),
    )


@Deprecated(message=_get_deprecation_msg(is_docstring=True))
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
    raise DeprecationWarning(
        _get_deprecation_msg(is_docstring=False, fn_name=world_rank.__name__),
    )


@Deprecated(message=_get_deprecation_msg(is_docstring=True))
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
    raise DeprecationWarning(
        _get_deprecation_msg(is_docstring=False, fn_name=local_rank.__name__),
    )


@Deprecated(message=_get_deprecation_msg(is_docstring=True))
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
    raise DeprecationWarning(
        _get_deprecation_msg(is_docstring=False, fn_name=load_checkpoint.__name__),
    )


@Deprecated(message=_get_deprecation_msg(is_docstring=True))
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
    raise DeprecationWarning(
        _get_deprecation_msg(is_docstring=False, fn_name=save_checkpoint.__name__),
    )


@Deprecated(message=_get_deprecation_msg(is_docstring=True))
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
    raise DeprecationWarning(
        _get_deprecation_msg(is_docstring=False, fn_name=world_size.__name__),
    )
