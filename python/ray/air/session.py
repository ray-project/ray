from typing import TYPE_CHECKING, Dict, Optional, Union

from ray.air._internal.session import _get_session
from ray.air.checkpoint import Checkpoint
from ray.train.session import _TrainSessionImpl

if TYPE_CHECKING:
    from ray.data import Dataset, DatasetPipeline
    from ray.tune.execution.placement_groups import PlacementGroupFactory


def report(metrics: Dict, *, checkpoint: Optional[Checkpoint] = None) -> None:
    """Report metrics and optionally save a checkpoint.

    Each invocation of this method will automatically increment the underlying
    iteration number. The physical meaning of this "iteration" is defined by
    user (or more specifically the way they call ``report``).
    It does not necessarily map to one epoch.

    This API is the canonical way to report metrics from Tune and Train, and
    replaces the legacy ``tune.report``, ``with tune.checkpoint_dir``,
    ``train.report`` and ``train.save_checkpoint`` calls.

    Note on directory checkpoints: AIR will take ownership of checkpoints passed
    to ``report()`` by moving them to a new path. The original directory will no
    longer be accessible to the caller after the report call.

    Example:
        .. code-block: python

            from ray.air import session
            from ray.air.checkpoint import Checkpoint
            from ray.air.config import ScalingConfig

            ######## Using it in the *per worker* train loop (TrainSession) #######
            def train_func():
                model = build_model()
                model.save("my_model", overwrite=True)
                session.report(
                    metrics={"foo": "bar"},
                    checkpoint=Checkpoint.from_directory(temp_dir.name)
                )
                # Air guarantees by this point, you can safely write new stuff to
                # "my_model" directory.

            scaling_config = ScalingConfig(num_workers=2)
            trainer = TensorflowTrainer(
                train_loop_per_worker=train_func, scaling_config=scaling_config
            )
            result = trainer.fit()
            # If you navigate to result.checkpoint's path, you will find the
            content of ``model.save()`` under it.
            # If you have `SyncConfig` configured, the content should also
            # show up in the corresponding cloud storage path.

    Args:
        metrics: The metrics you want to report.
        checkpoint: The optional checkpoint you want to report.
    """

    _get_session().report(metrics, checkpoint=checkpoint)


def get_checkpoint() -> Optional[Checkpoint]:
    """Access the session's last checkpoint to resume from if applicable.

    Returns:
        Checkpoint object if the session is currently being resumed.
            Otherwise, return None.

    Example:
        .. code-block: python

            ######## Using it in the *per worker* train loop (TrainSession) ######
            from ray.air import session
            from ray.air.checkpoint import Checkpoint
            from ray.air.config import ScalingConfig
            def train_func():
                ckpt = session.get_checkpoint()
                if ckpt:
                    with ckpt.as_directory() as loaded_checkpoint_dir:
                        import tensorflow as tf

                        model = tf.keras.models.load_model(loaded_checkpoint_dir)
                else:
                    model = build_model()

                model.save("my_model", overwrite=True)
                session.report(
                    metrics={"iter": 1},
                    checkpoint=Checkpoint.from_directory("my_model")
                )

            scaling_config = ScalingConfig(num_workers=2)
            trainer = TensorflowTrainer(
                train_loop_per_worker=train_func, scaling_config=scaling_config
            )
            result = trainer.fit()

            # trainer2 will pick up from the checkpoint saved by trainer1.
            trainer2 = TensorflowTrainer(
                train_loop_per_worker=train_func,
                scaling_config=scaling_config,
                # this is ultimately what is accessed through
                # ``Session.get_checkpoint()``
                resume_from_checkpoint=result.checkpoint,
            )
            result2 = trainer2.fit()
    """

    return _get_session().loaded_checkpoint


def get_trial_name() -> str:
    """Trial name for the corresponding trial."""
    return _get_session().trial_name


def get_trial_id() -> str:
    """Trial id for the corresponding trial."""
    return _get_session().trial_id


def get_trial_resources() -> "PlacementGroupFactory":
    """Trial resources for the corresponding trial."""
    return _get_session().trial_resources


def get_world_size() -> int:
    """Get the current world size (i.e. total number of workers) for this run.

    .. code-block:: python

        import time
        from ray.air import session
        from ray.air.config import ScalingConfig

        def train_loop_per_worker(config):
            assert session.get_world_size() == 4

        train_dataset = ray.data.from_items(
            [{"x": x, "y": x + 1} for x in range(32)])
        trainer = TensorflowTrainer(train_loop_per_worker,
            scaling_config=ScalingConfig(num_workers=1),
            datasets={"train": train_dataset})
        trainer.fit()
    """
    session = _get_session()
    if not isinstance(session, _TrainSessionImpl):
        raise RuntimeError(
            "`get_world_size` can only be called for TrainSession! "
            "Make sure you only use that in `train_loop_per_worker` function"
            "that is passed into `DataParallelTrainer`."
        )
    return session.world_size


def get_world_rank() -> int:
    """Get the world rank of this worker.

    .. code-block:: python

        import time
        from ray.air import session
        from ray.air.config import ScalingConfig

        def train_loop_per_worker():
            for iter in range(100):
                time.sleep(1)
                if session.get_world_rank() == 0:
                    print("Worker 0")

        train_dataset = ray.data.from_items(
            [{"x": x, "y": x + 1} for x in range(32)])
        trainer = TensorflowTrainer(train_loop_per_worker,
            scaling_config=ScalingConfig(num_workers=1),
            datasets={"train": train_dataset})
        trainer.fit()
    """
    session = _get_session()
    if not isinstance(session, _TrainSessionImpl):
        raise RuntimeError(
            "`get_world_rank` can only be called for TrainSession! "
            "Make sure you only use that in `train_loop_per_worker` function"
            "that is passed into `DataParallelTrainer`."
        )
    return session.world_rank


def get_local_rank() -> int:
    """Get the local rank of this worker (rank of the worker on its node).

    .. code-block:: python

        import time
        from ray.air import session
        from ray.air.config import ScalingConfig

        def train_loop_per_worker():
            if torch.cuda.is_available():
                torch.cuda.set_device(session.get_local_rank())
            ...

        train_dataset = ray.data.from_items(
            [{"x": x, "y": x + 1} for x in range(32)])
        trainer = TensorflowTrainer(train_loop_per_worker,
            scaling_config=ScalingConfig(num_workers=1),
            datasets={"train": train_dataset})
        trainer.fit()
    """
    session = _get_session()
    if not isinstance(session, _TrainSessionImpl):
        raise RuntimeError(
            "`get_local_rank` can only be called for TrainSession! "
            "Make sure you only use that in `train_loop_per_worker` function"
            "that is passed into `DataParallelTrainer`."
        )
    return session.local_rank


def get_dataset_shard(
    dataset_name: Optional[str] = None,
) -> Optional[Union["Dataset", "DatasetPipeline"]]:
    """Returns the Ray Dataset or DatasetPipeline shard for this worker.

    You should call ``iter_torch_batches()`` or ``iter_tf_batches()``
    on this shard to convert it to the appropriate
    framework-specific data type.

    .. code-block:: python

        import ray
        from ray import train
        from ray.air import session
        from ray.air.config import ScalingConfig

        def train_loop_per_worker():
            model = Net()
            for iter in range(100):
                # Trainer will automatically handle sharding.
                data_shard = session.get_dataset_shard("train")
                for batch in data_shard.iter_torch_batches():
                    # ...
            return model

        train_dataset = ray.data.from_items(
            [{"x": x, "y": x + 1} for x in range(32)])
        trainer = TorchTrainer(train_loop_per_worker,
            scaling_config=ScalingConfig(num_workers=2),
            datasets={"train": train_dataset})
        trainer.fit()

    Args:
        dataset_name: If a Dictionary of Datasets was passed to ``Trainer``, then
            specifies which dataset shard to return.

    Returns:
        The ``Dataset`` or ``DatasetPipeline`` shard to use for this worker.
        If no dataset is passed into Trainer, then return None.
    """
    session = _get_session()
    if not isinstance(session, _TrainSessionImpl):
        raise RuntimeError(
            "`get_dataset_shard` can only be called for TrainSession! "
            "Make sure you only use that in `train_loop_per_worker` function"
            "that is passed into `DataParallelTrainer`."
        )
    return session.get_dataset_shard(dataset_name)
