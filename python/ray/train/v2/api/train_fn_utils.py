import json
import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Literal, Optional, Union

from ray._common.usage.usage_lib import TagKey, record_extra_usage_tag
from ray.train.v2._internal.constants import (
    TRAIN_ANNOTATION_RAY_TRAIN_ANNOTATE,
    TRAIN_ANNOTATION_RAY_TRAIN_REPORT,
)
from ray.train.v2._internal.data_integration.interfaces import DatasetShardMetadata
from ray.train.v2._internal.execution.context import get_train_context
from ray.train.v2._internal.execution.train_fn_utils import get_train_fn_utils
from ray.train.v2._internal.util import requires_train_worker
from ray.train.v2.api.context import TrainContext
from ray.train.v2.api.report_config import (
    CheckpointConsistencyMode,
    CheckpointUploadMode,
)
from ray.train.v2.api.validation_config import ValidationTaskConfig
from ray.util.annotations import DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    from ray.data import DataIterator
    from ray.train import Checkpoint
    from ray.train.v2.api.preemption import PreemptionInfo
    from ray.train.v2.api.reported_checkpoint import ReportedCheckpoint

logger = logging.getLogger(__name__)

# Severities accepted by `ray.train.annotate`, each rendered as its own layer on
# the Grafana train dashboard.
_ANNOTATION_SEVERITIES = frozenset({"info", "warning", "error"})

# Annotations are best-effort observability, and a failure here is typically
# persistent (e.g. metrics that cannot be JSON-serialized), so it is reported
# once per process rather than once per `ray.train.report` call.
_annotation_failure_reported = False


def _log_annotation_failure(event: str) -> None:
    """Log an annotation failure at most once per process, with its traceback."""
    global _annotation_failure_reported

    if _annotation_failure_reported:
        return
    _annotation_failure_reported = True

    logger.warning(
        "Failed to emit the %r annotation; continuing. "
        "Further annotation failures will not be logged.",
        event,
        exc_info=True,
    )


@PublicAPI(stability="stable")
@requires_train_worker(raise_in_tune_session=True)
def report(
    metrics: Dict[str, Any],
    checkpoint: Optional["Checkpoint"] = None,
    checkpoint_dir_name: Optional[str] = None,
    checkpoint_upload_mode: CheckpointUploadMode = CheckpointUploadMode.SYNC,
    delete_local_checkpoint_after_upload: Optional[bool] = None,
    checkpoint_upload_fn: Optional[Callable[["Checkpoint", str], "Checkpoint"]] = None,
    validation: Union[bool, ValidationTaskConfig] = False,
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
        validation: [Alpha] If True, triggers validation with default kwargs from validation_config.
            If a ValidationTaskConfig, validation is run using fn_kwargs merged with validation_config
            defaults, with fn_kwargs taking precedence on conflicts. If False, no validation.
    """
    if validation and not checkpoint:
        raise ValueError("Validation requires a checkpoint to be provided.")

    if delete_local_checkpoint_after_upload is None:
        delete_local_checkpoint_after_upload = (
            checkpoint_upload_mode.default_delete_local_checkpoint_after_upload()
        )

    if checkpoint:
        record_extra_usage_tag(
            TagKey.TRAIN_CHECKPOINT_MODE, checkpoint_upload_mode.value
        )
        if validation:
            record_extra_usage_tag(TagKey.TRAIN_ASYNCHRONOUS_VALIDATION, "1")

    # Emit a single rank-0 annotation marking this report on the Grafana train dashboards
    try:
        train_context = get_train_context()
        annotation = train_context.annotation
        if annotation is not None and train_context.get_world_rank() == 0:
            report_fields = {
                "metrics": json.dumps(metrics, default=str),
                "has_checkpoint": checkpoint is not None,
                "validation": bool(validation),
            }
            if checkpoint is not None and checkpoint_dir_name is not None:
                report_fields["checkpoint_dir_name"] = checkpoint_dir_name
            if isinstance(validation, ValidationTaskConfig):
                report_fields["validation_config"] = json.dumps(
                    {
                        "fn_kwargs": validation.fn_kwargs,
                        "timeout_s": validation.timeout_s,
                    },
                    default=str,
                )
            annotation.annotate(
                event=TRAIN_ANNOTATION_RAY_TRAIN_REPORT, **report_fields
            )
    except Exception:
        _log_annotation_failure(TRAIN_ANNOTATION_RAY_TRAIN_REPORT)

    get_train_fn_utils().report(
        metrics=metrics,
        checkpoint=checkpoint,
        checkpoint_dir_name=checkpoint_dir_name,
        checkpoint_upload_mode=checkpoint_upload_mode,
        delete_local_checkpoint_after_upload=delete_local_checkpoint_after_upload,
        checkpoint_upload_fn=checkpoint_upload_fn,
        validation=validation,
    )


@PublicAPI(stability="stable")
@requires_train_worker(raise_in_tune_session=True)
def get_context() -> TrainContext:
    """Get or create a singleton training context.

    The context is only available within a function passed to Ray Train.

    See the :class:`~ray.train.TrainContext` API reference to see available methods.
    """
    return get_train_fn_utils().get_context()


@PublicAPI(stability="alpha")
@requires_train_worker(raise_in_tune_session=True)
def get_preemption_info() -> Optional["PreemptionInfo"]:
    """Return the imminent preemption info for the current worker, or ``None``.

    Returns ``None`` until a node hosting one of the workers is being preempted
    (e.g. a spot instance reclaim). The recommended reaction is to save a
    just-in-time checkpoint and keep training: when the node is actually
    preempted, Ray Train restarts the run and resumes it from the latest
    checkpoint, retrying against ``FailureConfig.max_preemption_failures``.
    A run that returns cleanly always finishes, whether or not a preemption
    is in progress.

    Example:

        .. testcode::
            :skipif: True

            import ray.train

            def train_func(config):
                for step in range(config["total_steps"]):
                    if ray.train.get_preemption_info() is not None:
                        ray.train.report(metrics, checkpoint=checkpoint)
                    # ... normal training step (with your usual periodic
                    # checkpointing) ...

    Returns:
        A :class:`~ray.train.PreemptionInfo` with the affected node ids / world
        ranks and the reclaim deadline, or ``None`` if no preemption has been
        detected.
    """
    return get_train_fn_utils().get_preemption_info()


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
    timeout_s: Optional[float] = None,
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
        timeout_s: Timeout in seconds to collecting checkpoint and validation information.
            Defaults to None to wait indefinitely.

    Returns:
        List of ReportedCheckpoint objects that represent the checkpoints and
        corresponding metrics reported by the workers.
    """
    return get_train_fn_utils().get_all_reported_checkpoints(
        consistency_mode=consistency_mode, timeout_s=timeout_s
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
    train_fn_utils = get_train_fn_utils()
    return train_fn_utils.get_dataset_shard(
        DatasetShardMetadata(
            dataset_name=dataset_name,
            world_rank=train_fn_utils.get_context().get_world_rank(),
        )
    )


@DeveloperAPI
@requires_train_worker(raise_in_tune_session=True)
def annotate(
    message: str,
    severity: Literal["info", "warning", "error"] = "info",
    rank_zero_only: bool = True,
    **fields: Any,
) -> None:
    """Emit a custom annotation that can be visualized in Grafana.

    This helps mark moments of interest (e.g. an evaluation completing, a
    learning-rate change, or a phase transition) on the same timeline as your
    training metrics. Rendering it as a point annotation on the Train Grafana
    dashboard additionally requires the cluster operator to ship these log
    lines to a log backend registered as a Grafana datasource, and to set
    ``RAY_GRAFANA_ANNOTATION_DATASOURCE_UID`` and
    ``RAY_GRAFANA_ANNOTATION_STREAM_SELECTOR``.

    You control what appears on the dashboard tooltip: the ``message`` is shown
    as the annotation text and any ``**fields`` are shown together as a single
    JSON tag. All custom annotations share one dashboard layer (they are emitted
    under a fixed internal event name), colored by ``severity``.

    Setting ``RAY_TRAIN_ANNOTATIONS_ENABLED=0`` turns all Ray Train annotations
    off, in which case this call is a no-op.

    Example:

        .. testcode::
            :skipif: True

            import ray.train

            def train_func(config):
                for epoch in range(config["num_epochs"]):
                    # Do training...
                    ray.train.annotate(
                        message=f"Finished epoch {epoch}",
                        epoch=epoch,
                        loss=loss,
                    )

    Args:
        message: Human-readable description of the event, shown as the annotation
            text/tooltip in Grafana. Emitted as the ``message`` field.
        severity: Severity level for the event (``"info"``, ``"warning"``,
            ``"error"``). Defaults to ``"info"``. This drives the annotation
            color, since Grafana colors annotations per query/layer
            (see :class:`Annotation`).
        rank_zero_only: If ``True`` (the default), only the rank 0 worker emits
            the annotation, producing a single point on the dashboard. If
            ``False``, every worker emits its own annotation and carries
            ``ray_train_worker_world_rank`` for LogQL to distinguish ranks.
        **fields: Arbitrary additional key-value pairs to include in the emitted
            JSON payload (e.g. ``epoch=3``, ``loss=0.1``). These are serialized
            together into a single JSON string under the ``fields`` key, so they
            are displayed as one JSON tag on the dashboard annotation. Because
            ``fields`` parses as a single string label, individual keys within it
            are not filterable in LogQL; filter on the reserved fields
            (e.g. ``severity``) or the run/rank tags instead.
    """
    # Not an assert: under `python -O` an invalid severity would silently emit an
    # annotation that matches none of the dashboard layers, so it would never
    # appear and no error would surface anywhere.
    if severity not in _ANNOTATION_SEVERITIES:
        raise ValueError(
            f"Invalid annotation severity {severity!r}. "
            f"Expected one of {sorted(_ANNOTATION_SEVERITIES)}."
        )

    try:
        train_context = get_train_context()
        annotation = train_context.annotation
        if annotation is None:
            return
        if rank_zero_only and train_context.get_world_rank() != 0:
            return

        annotation_fields = {"message": message}
        if fields:
            annotation_fields["fields"] = json.dumps(fields, default=str)

        annotation.annotate(
            event=TRAIN_ANNOTATION_RAY_TRAIN_ANNOTATE,
            severity=severity,
            **annotation_fields,
        )
    except Exception:
        _log_annotation_failure(TRAIN_ANNOTATION_RAY_TRAIN_ANNOTATE)
