from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

from ray.train.v2._internal.data_integration.interfaces import DatasetShardMetadata
from ray.train.v2._internal.execution.train_fn_utils import get_train_fn_utils
from ray.train.v2._internal.util import requires_train_worker
from ray.train.v2.api.context import TrainContext
from ray.train.v2.api.report_config import (
    CheckpointConsistencyMode,
    CheckpointUploadMode,
)
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.data import DataIterator
    from ray.train import Checkpoint
    from ray.train.v2.api.reported_checkpoint import ReportedCheckpoint


@PublicAPI(stability="stable")
@requires_train_worker(raise_in_tune_session=True)
def report(
    metrics: Dict[str, Any],
    checkpoint: Optional["Checkpoint"] = None,
    checkpoint_dir_name: Optional[str] = None,
    checkpoint_upload_mode: CheckpointUploadMode = CheckpointUploadMode.SYNC,
    delete_local_checkpoint_after_upload: Optional[bool] = None,
    checkpoint_upload_fn: Optional[Callable[["Checkpoint", str], "Checkpoint"]] = None,
    validate_fn: Optional[Callable[["Checkpoint", Optional[Dict]], Dict]] = None,
    validate_config: Optional[Dict] = None,
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
            :skipif: True

            import tempfile

            import ray.train
            from ray.train.torch import TorchTrainer


            def train_func(config):
                start_epoch = 0

                for epoch in range(start_epoch, config.get("num_epochs", 10)):
                    # Do training...

                    metrics = {"loss": ...}

                    with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
                       # Save the checkpoint...
                       # torch.save(...)

                        checkpoint = ray.train.Checkpoint.from_directory(temp_checkpoint_dir)

                        # Example: Only the rank 0 worker uploads the checkpoint.
                        if ray.train.get_context().get_world_rank() == 0:
                            ray.train.report(metrics, checkpoint=checkpoint)
                        else:
                            ray.train.report(metrics, checkpoint=None)

            trainer = TorchTrainer(
                train_func, scaling_config=ray.train.ScalingConfig(num_workers=2)
            )

    Args:
        metrics: The metrics you want to report.
        checkpoint: The optional checkpoint you want to report.
        checkpoint_dir_name: Custom name for the checkpoint directory.
            If not provided, a unique directory name will be automatically generated.
            If provided, it must be unique across all checkpoints per worker to avoid
            naming collisions. Consider including identifiers such as the epoch or batch
            index in the name.
        checkpoint_upload_mode: The manner in which we want to upload the checkpoint.
            Defaults to uploading the checkpoint synchronously.
            This works when no checkpoint is provided but is not useful in that case.
        delete_local_checkpoint_after_upload: Whether to delete the checkpoint after it is uploaded.
        checkpoint_upload_fn: A user defined function that will be called with the
            checkpoint to upload it. If not provided, defaults to using the `pyarrow.fs.copy_files`
            utility for copying to the destination `storage_path`.
        validate_fn: If provided, Ray Train will validate the checkpoint using
            this function.
        validate_config: Configuration passed to the validate_fn. Can contain info
            like the validation dataset.
    """
    if delete_local_checkpoint_after_upload is None:
        delete_local_checkpoint_after_upload = (
            checkpoint_upload_mode._default_delete_local_checkpoint_after_upload()
        )

    # TODO: figure out how to validate validate_fn itself
    if validate_config and not validate_fn:
        raise ValueError("validate_fn must be provided together with validate_config")

    get_train_fn_utils().report(
        metrics=metrics,
        checkpoint=checkpoint,
        checkpoint_dir_name=checkpoint_dir_name,
        checkpoint_upload_mode=checkpoint_upload_mode,
        delete_local_checkpoint_after_upload=delete_local_checkpoint_after_upload,
        checkpoint_upload_fn=checkpoint_upload_fn,
        validate_fn=validate_fn,
        validate_config=validate_config or {},
    )


@PublicAPI(stability="stable")
@requires_train_worker(raise_in_tune_session=True)
def get_context() -> TrainContext:
    """Get or create a singleton training context.

    The context is only available within a function passed to Ray Train.

    See the :class:`~ray.train.TrainContext` API reference to see available methods.
    """
    return get_train_fn_utils().get_context()


@PublicAPI(stability="stable")
@requires_train_worker(raise_in_tune_session=True)
def get_checkpoint() -> Optional["Checkpoint"]:
    """Access the latest reported checkpoint to resume from if one exists.

    See :ref:`the checkpoint loading guide <train-dl-loading-checkpoints>` for more details.

    Example:

        .. testcode::
            :skipif: True

            import tempfile

            import ray.train
            from ray.train.torch import TorchTrainer


            def train_func(config):
                start_epoch = 0
                checkpoint = ray.train.get_checkpoint()
                if checkpoint:
                    with checkpoint.as_directory() as checkpoint_dir:
                        # Load back training state
                        ...

                for epoch in range(start_epoch, config.get("num_epochs", 10)):
                    # Do training...

                    metrics = {"loss": ...}

                    with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
                       # Save the checkpoint...

                        checkpoint = ray.train.Checkpoint.from_directory(temp_checkpoint_dir)
                        ray.train.report(metrics, checkpoint=checkpoint)

            trainer = TorchTrainer(
                train_func, scaling_config=ray.train.ScalingConfig(num_workers=2)
            )

    Returns:
        Checkpoint object if the session is currently being resumed.
            Otherwise, return None.
    """
    return get_train_fn_utils().get_checkpoint()


@PublicAPI(stability="alpha")
@requires_train_worker()
def get_all_reported_checkpoints(
    consistency_mode: CheckpointConsistencyMode = CheckpointConsistencyMode.VALIDATED,
) -> List["ReportedCheckpoint"]:
    """Get all the reported checkpoints so far.

    Blocks until Ray Train has finished processing every in-flight `ray.train.report` call.

    Example:

        .. testcode::

            import tempfile

            import ray.train
            from ray.train.torch import TorchTrainer


            def train_func(config):
                start_epoch = 0

                for epoch in range(start_epoch, config.get("num_epochs", 2)):
                    # Do training...

                    metrics = {"loss": 0.1}

                    with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
                       # Save the checkpoint...

                        checkpoint = ray.train.Checkpoint.from_directory(temp_checkpoint_dir)
                        ray.train.report(metrics, checkpoint=checkpoint)

                reported_checkpoints = ray.train.get_all_reported_checkpoints()
                # Report artifacts/metrics to experiment tracking framework...

            trainer = TorchTrainer(
                train_func, scaling_config=ray.train.ScalingConfig(num_workers=2)
            )
            trainer.fit()

    Args:
        consistency_mode: Read semantics for checkpoint retrieval during an ongoing run.
            Defaults to CheckpointConsistencyMode.VALIDATED.
            See :class:`~ray.train.CheckpointConsistencyMode` for more details.

    Returns:
        List of ReportedCheckpoint objects that represent the checkpoints and
        corresponding metrics reported by the workers.
    """
    return get_train_fn_utils().get_all_reported_checkpoints(
        consistency_mode=consistency_mode
    )


@PublicAPI(stability="stable")
@requires_train_worker()
def get_dataset_shard(dataset_name: Optional[str] = None) -> Optional["DataIterator"]:
    """Returns the :class:`ray.data.DataIterator` shard for this worker.

    Call :meth:`~ray.data.DataIterator.iter_torch_batches` or
    :meth:`~ray.data.DataIterator.to_tf` on this shard to convert it to the
    appropriate framework-specific data type.

    .. testcode::

        import ray.train
        from ray.train.torch import TorchTrainer

        def train_fn_per_worker(config):
            ...
            for epoch in range(2):
                # Trainer will automatically handle sharding.
                data_shard = ray.train.get_dataset_shard("train")
                for batch in data_shard.iter_torch_batches():
                    ...

        train_dataset = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")
        trainer = TorchTrainer(
            train_fn_per_worker,
            scaling_config=ray.train.ScalingConfig(num_workers=2),
            datasets={"train": train_dataset}
        )
        trainer.fit()

    Args:
        dataset_name: If a Dictionary of Datasets was passed to ``Trainer``, then
            specifies which dataset shard to return.

    Returns:
        The ``DataIterator`` shard to use for this worker.
        If no dataset is passed into Trainer, then return None.
    """
    return get_train_fn_utils().get_dataset_shard(
        DatasetShardMetadata(dataset_name=dataset_name)
    )
