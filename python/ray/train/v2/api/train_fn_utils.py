from typing import TYPE_CHECKING, Any, Dict, Optional
from ray.util.annotations import PublicAPI

from ray.train import Checkpoint
from ray.train.v2._internal.execution.context import get_train_context
from ray.train.v2.api.context import TrainContext

if TYPE_CHECKING:
    from ray.data import DataIterator


@PublicAPI(stability="stable")
def report(
    metrics: Dict[str, Any],
    checkpoint: Optional[Checkpoint] = None,
    checkpoint_dir_name: Optional[str] = None,
):
    """Report metrics and optionally save a checkpoint.

    If a checkpoint is provided, it will be
    :ref:`persisted to storage <persistent-storage-guide>`.

    If this is called in multiple distributed training workers:

    - Only the metrics reported by the rank 0 worker will be attached to the checkpoint.
    - A checkpoint will be registered as long as one or more workers reports
      checkpoint that is not None.
      See the :ref:`checkpointing guide <train-dl-saving-checkpoints>`.
    - Checkpoints from multiple workers will be merged into one directory
      in persistent storage.
      See :ref:`the distributed checkpointing guide <train-distributed-checkpointing>`.


    .. warning::

        All workers must call `ray.train.report` the same number of times
        so that Ray Train can properly synchronize the training state across
        workers. This method acts as a barrier across all workers, so be sure
        that every worker reaches this method.

    Example:

        .. testcode::

            import tempfile

            from ray import train
            from ray.train import Checkpoint
            from ray.train.torch import TorchTrainer


            def train_func(config):
                start_epoch = 0
                checkpoint = train.get_checkpoint()
                if checkpoint:
                    with checkpoint.as_directory() as checkpoint_dir:
                        # Load back training state
                        ...

                for epoch in range(start_epoch, config.get("num_epochs", 10)):
                    # Do training...

                    metrics = {"loss": ...}

                    with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
                       # Save the checkpoint...
                       # torch.save(...)

                        checkpoint = Checkpoint.from_directory(temp_checkpoint_dir)

                        # Example: Only the rank 0 worker uploads the checkpoint.
                        if ray.train.get_context().get_world_rank() == 0:
                            train.report(metrics, checkpoint=checkpoint)
                        else:
                            train.report(metrics, checkpoint=None)

            trainer = TorchTrainer(
                train_func, scaling_config=train.ScalingConfig(num_workers=2)
            )

    Args:
        metrics: The metrics you want to report.
        checkpoint: The optional checkpoint you want to report.
    """

    get_train_context().report(
        metrics=metrics, checkpoint=checkpoint, checkpoint_dir_name=checkpoint_dir_name
    )


@PublicAPI(stability="stable")
def get_context() -> TrainContext:
    """Get or create a singleton training context.

    The context is only available within a function passed to Ray Train.

    See the :class:`~ray.train.TrainContext` API reference to see available methods.
    """
    # TODO: Return a dummy train context on the controller and driver process
    # instead of raising an exception the the train context does not exist.
    return TrainContext()


@PublicAPI(stability="stable")
def get_checkpoint() -> Optional[Checkpoint]:
    """Access the latest reported checkpoint to resume from if one exists.

    Example:

        .. testcode::

            import tempfile

            from ray import train
            from ray.train import Checkpoint
            from ray.train.torch import TorchTrainer


            def train_func(config):
                start_epoch = 0
                checkpoint = train.get_checkpoint()
                if checkpoint:
                    with checkpoint.as_directory() as checkpoint_dir:
                        # Load back training state
                        ...

                for epoch in range(start_epoch, config.get("num_epochs", 10)):
                    # Do training...

                    metrics = {"loss": ...}

                    with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
                       # Save the checkpoint...

                        checkpoint = Checkpoint.from_directory(temp_checkpoint_dir)
                        train.report(metrics, checkpoint=checkpoint)

            trainer = TorchTrainer(
                train_func, scaling_config=train.ScalingConfig(num_workers=2)
            )

    Returns:
        Checkpoint object if the session is currently being resumed.
            Otherwise, return None.
    """
    return get_train_context().get_checkpoint()


@PublicAPI(stability="stable")
def get_dataset_shard(dataset_name: Optional[str] = None) -> Optional["DataIterator"]:
    """Returns the :class:`ray.data.DataIterator` shard for this worker.

    Call :meth:`~ray.data.DataIterator.iter_torch_batches` or
    :meth:`~ray.data.DataIterator.to_tf` on this shard to convert it to the
    appropriate framework-specific data type.

    .. testcode::

        import ray
        from ray import train
        from ray.train import ScalingConfig
        from ray.train.torch import TorchTrainer

        def train_loop_per_worker(config):
            ...
            for epoch in range(2):
                # Trainer will automatically handle sharding.
                data_shard = train.get_dataset_shard("train")
                for batch in data_shard.iter_torch_batches():
                    ...

        train_dataset = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")
        trainer = TorchTrainer(
            train_loop_per_worker,
            scaling_config=ScalingConfig(num_workers=2),
            datasets={"train": train_dataset}
        )
        trainer.fit()

    .. testoutput::
        :hide:

        ...

    Args:
        dataset_name: If a Dictionary of Datasets was passed to ``Trainer``, then
            specifies which dataset shard to return.

    Returns:
        The ``DataIterator`` shard to use for this worker.
        If no dataset is passed into Trainer, then return None.
    """
    return get_train_context().get_dataset_shard(dataset_name)
