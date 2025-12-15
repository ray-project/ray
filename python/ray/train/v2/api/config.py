import logging
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Literal, Optional, Union

import pyarrow.fs

from ray.air.config import (
    FailureConfig as FailureConfigV1,
    ScalingConfig as ScalingConfigV1,
)
from ray.runtime_env import RuntimeEnv
from ray.train.v2._internal.constants import _DEPRECATED
from ray.train.v2._internal.execution.storage import StorageContext
from ray.train.v2._internal.migration_utils import (
    FAIL_FAST_DEPRECATION_MESSAGE,
    TRAINER_RESOURCES_DEPRECATION_MESSAGE,
)
from ray.train.v2._internal.util import date_str
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.train import UserCallback

logger = logging.getLogger(__name__)


@dataclass
class ScalingConfig(ScalingConfigV1):
    """Configuration for scaling training.

    Args:
        num_workers: The number of workers (Ray actors) to launch.
            Each worker will reserve 1 CPU by default. The number of CPUs
            reserved by each worker can be overridden with the
            ``resources_per_worker`` argument. If the number of workers is 0,
            the training function will run in local mode, meaning the training
            function runs in the same process.
        use_gpu: If True, training will be done on GPUs (1 per worker).
            Defaults to False. The number of GPUs reserved by each
            worker can be overridden with the ``resources_per_worker``
            argument.
        resources_per_worker: If specified, the resources
            defined in this Dict is reserved for each worker.
            Define the ``"CPU"`` and ``"GPU"`` keys (case-sensitive) to
            override the number of CPU or GPUs used by each worker.
        placement_strategy: The placement strategy to use for the
            placement group of the Ray actors. See :ref:`Placement Group
            Strategies <pgroup-strategy>` for the possible options.
        label_selector: A list of label selectors for Ray Train worker placement.
            If a single label selector is provided, it will be applied to all Ray Train workers.
            If a list is provided, it must be the same length as the max number of Ray Train workers.
        accelerator_type: [Experimental] If specified, Ray Train will launch the
            training coordinator and workers on the nodes with the specified type
            of accelerators.
            See :ref:`the available accelerator types <accelerator_types>`.
            Ensure that your cluster has instances with the specified accelerator type
            or is able to autoscale to fulfill the request. This field is required
            when `use_tpu` is True and `num_workers` is greater than 1.
        use_tpu: [Experimental] If True, training will be done on TPUs (1 TPU VM
            per worker). Defaults to False. The number of TPUs reserved by each
            worker can be overridden with the ``resources_per_worker``
            argument. This arg enables SPMD execution of the training workload.
        topology: [Experimental] If specified, Ray Train will launch the training
            coordinator and workers on nodes with the specified topology. Topology is
            auto-detected for TPUs and added as Ray node labels. This arg enables
            SPMD execution of the training workload. This field is required
            when `use_tpu` is True and `num_workers` is greater than 1.
    """

    trainer_resources: Optional[dict] = None
    use_tpu: Union[bool] = False
    topology: Optional[str] = None
    label_selector: Optional[Union[Dict[str, str], List[Dict[str, str]]]] = None

    def __post_init__(self):
        if self.trainer_resources is not None:
            raise DeprecationWarning(TRAINER_RESOURCES_DEPRECATION_MESSAGE)

        if self.use_gpu and self.use_tpu:
            raise ValueError("Cannot specify both `use_gpu=True` and `use_tpu=True`.")

        if not self.use_tpu and self.num_tpus_per_worker > 0:
            raise ValueError(
                "`use_tpu` is False but `TPU` was found in "
                "`resources_per_worker`. Either set `use_tpu` to True or "
                "remove `TPU` from `resources_per_worker."
            )

        if self.use_tpu and self.num_tpus_per_worker == 0:
            raise ValueError(
                "`use_tpu` is True but `TPU` is set to 0 in "
                "`resources_per_worker`. Either set `use_tpu` to False or "
                "request a positive number of `TPU` in "
                "`resources_per_worker."
            )

        if self.use_tpu and self.num_workers > 1:
            if not self.topology:
                raise ValueError(
                    "`topology` must be specified in ScalingConfig when `use_tpu=True` "
                    " and `num_workers` > 1."
                )
            if not self.accelerator_type:
                raise ValueError(
                    "`accelerator_type` must be specified in ScalingConfig when "
                    "`use_tpu=True` and `num_workers` > 1."
                )
            if self.label_selector:
                raise ValueError(
                    "Cannot set `label_selector` when `use_tpu=True` because "
                    "Ray Train automatically reserves a TPU slice with a predefined label."
                )

        if (
            isinstance(self.label_selector, list)
            and isinstance(self.num_workers, int)
            and len(self.label_selector) != self.num_workers
        ):
            raise ValueError(
                "If `label_selector` is a list, it must be the same length as `num_workers`."
            )

        if self.num_workers == 0:
            logger.info(
                "Running in local mode. The training function will run in the same process. "
                "If you are using it and running into issues please file a report at "
                "https://github.com/ray-project/ray/issues."
            )

        super().__post_init__()

    @property
    def _resources_per_worker_not_none(self):
        if self.resources_per_worker is None:
            if self.use_tpu:
                return {"TPU": 1}

        return super()._resources_per_worker_not_none

    @property
    def _trainer_resources_not_none(self):
        return {}

    @property
    def num_tpus_per_worker(self):
        """The number of TPUs to set per worker."""
        return self._resources_per_worker_not_none.get("TPU", 0)


@dataclass
@PublicAPI(stability="stable")
class CheckpointConfig:
    """Configuration for checkpointing.

    Default behavior is to persist all checkpoints reported with
    :meth:`ray.train.report` to disk. If ``num_to_keep`` is set,
    the default retention policy is to keep the most recent checkpoints.

    Args:
        num_to_keep: The maximum number of checkpoints to keep.
            If you report more checkpoints than this, the oldest
            (or lowest-scoring, if ``checkpoint_score_attribute`` is set)
            checkpoint will be deleted.
            If this is ``None`` then all checkpoints will be kept. Must be >= 1.
        checkpoint_score_attribute: The attribute that will be used to
            score checkpoints to determine which checkpoints should be kept.
            This attribute must be a key from the metrics dictionary
            attached to the checkpoint. This attribute must have a numerical value.
        checkpoint_score_order: Either "max" or "min".
            If "max"/"min", then checkpoints with highest/lowest values of
            the ``checkpoint_score_attribute`` will be kept. Defaults to "max".
        checkpoint_frequency: [Deprecated]
        checkpoint_at_end: [Deprecated]
    """

    num_to_keep: Optional[int] = None
    checkpoint_score_attribute: Optional[str] = None
    checkpoint_score_order: Literal["max", "min"] = "max"
    checkpoint_frequency: Union[Optional[int], Literal[_DEPRECATED]] = _DEPRECATED
    checkpoint_at_end: Union[Optional[bool], Literal[_DEPRECATED]] = _DEPRECATED

    def __post_init__(self):
        if self.checkpoint_frequency != _DEPRECATED:
            raise DeprecationWarning(
                "`checkpoint_frequency` is deprecated since it does not "
                "apply to user-defined training functions. "
                "Please remove this argument from your CheckpointConfig."
            )

        if self.checkpoint_at_end != _DEPRECATED:
            raise DeprecationWarning(
                "`checkpoint_at_end` is deprecated since it does not "
                "apply to user-defined training functions. "
                "Please remove this argument from your CheckpointConfig."
            )

        if self.num_to_keep is not None and self.num_to_keep <= 0:
            raise ValueError(
                f"Received invalid num_to_keep: {self.num_to_keep}. "
                "Must be None or an integer >= 1."
            )

        if self.checkpoint_score_order not in ("max", "min"):
            raise ValueError(
                f"Received invalid checkpoint_score_order: {self.checkpoint_score_order}. "
                "Must be 'max' or 'min'."
            )


@dataclass
class FailureConfig(FailureConfigV1):
    """Configuration related to failure handling of each training run.

    Args:
        max_failures: Tries to recover a run from training worker errors at least this many times.
            Will recover from the latest checkpoint if present.
            Setting to -1 will lead to infinite recovery retries.
            Setting to 0 will disable retries. Defaults to 0.
        controller_failure_limit: [DeveloperAPI] The maximum number of controller failures to tolerate.
            Setting to -1 will lead to infinite controller retries.
            Setting to 0 will disable controller retries. Defaults to -1.
    """

    fail_fast: Union[bool, str] = _DEPRECATED
    controller_failure_limit: int = -1

    def __post_init__(self):
        if self.fail_fast != _DEPRECATED:
            raise DeprecationWarning(FAIL_FAST_DEPRECATION_MESSAGE)


@dataclass
@PublicAPI(stability="stable")
class RunConfig:
    """Runtime configuration for training runs.

    Args:
        name: Name of the trial or experiment. If not provided, will be deduced
            from the Trainable.
        storage_path: Path where all results and checkpoints are persisted.
            Can be a local directory or a destination on cloud storage.
            For multi-node training/tuning runs, this must be set to a
            shared storage location (e.g., S3, NFS).
            This defaults to the local ``~/ray_results`` directory.
        storage_filesystem: A custom filesystem to use for storage.
            If this is provided, `storage_path` should be a path with its
            prefix stripped (e.g., `s3://bucket/path` -> `bucket/path`).
        failure_config: Failure mode configuration.
        checkpoint_config: Checkpointing configuration.
        callbacks: [DeveloperAPI] A list of callbacks that the Ray Train controller
            will invoke during training.
        worker_runtime_env: [DeveloperAPI] Runtime environment configuration
            for all Ray Train worker actors.
    """

    name: Optional[str] = None
    storage_path: Optional[str] = None
    storage_filesystem: Optional[pyarrow.fs.FileSystem] = None
    failure_config: Optional[FailureConfig] = None
    checkpoint_config: Optional[CheckpointConfig] = None
    callbacks: Optional[List["UserCallback"]] = None
    worker_runtime_env: Optional[Union[dict, RuntimeEnv]] = None

    sync_config: str = _DEPRECATED
    verbose: str = _DEPRECATED
    stop: str = _DEPRECATED
    progress_reporter: str = _DEPRECATED
    log_to_file: str = _DEPRECATED

    def __post_init__(self):
        from ray.train.constants import DEFAULT_STORAGE_PATH

        if self.storage_path is None:
            self.storage_path = DEFAULT_STORAGE_PATH

        if not self.failure_config:
            self.failure_config = FailureConfig()

        if not self.checkpoint_config:
            self.checkpoint_config = CheckpointConfig()

        if isinstance(self.storage_path, Path):
            self.storage_path = self.storage_path.as_posix()

        run_config_deprecation_message = (
            "`RunConfig({})` is deprecated. This configuration was a "
            "Ray Tune API that did not support Ray Train usage well, "
            "so we are dropping support going forward. "
            "If you heavily rely on these configurations, "
            "you can run Ray Train as a single Ray Tune trial. "
            "See this issue for more context: "
            "https://github.com/ray-project/ray/issues/49454"
        )

        unsupported_params = [
            "sync_config",
            "verbose",
            "stop",
            "progress_reporter",
            "log_to_file",
        ]
        for param in unsupported_params:
            if getattr(self, param) != _DEPRECATED:
                raise DeprecationWarning(run_config_deprecation_message.format(param))

        if not self.name:
            self.name = f"ray_train_run-{date_str()}"

        self.callbacks = self.callbacks or []
        self.worker_runtime_env = self.worker_runtime_env or {}

        from ray.train.v2.api.callback import RayTrainCallback

        if not all(isinstance(cb, RayTrainCallback) for cb in self.callbacks):
            raise ValueError(
                "All callbacks must be instances of `ray.train.UserCallback`. "
                "Passing in a Ray Tune callback is no longer supported. "
                "See this issue for more context: "
                "https://github.com/ray-project/ray/issues/49454"
            )

        if not isinstance(self.checkpoint_config, CheckpointConfig):
            raise ValueError(
                f"Invalid `CheckpointConfig` type: {self.checkpoint_config.__class__}. "
                "Use `ray.train.CheckpointConfig` instead. "
                "See this issue for more context: "
                "https://github.com/ray-project/ray/issues/49454"
            )

        if not isinstance(self.failure_config, FailureConfig):
            raise ValueError(
                f"Invalid `FailureConfig` type: {self.failure_config.__class__}. "
                "Use `ray.train.FailureConfig` instead. "
                "See this issue for more context: "
                "https://github.com/ray-project/ray/issues/49454"
            )

    @cached_property
    def storage_context(self) -> StorageContext:
        return StorageContext(
            storage_path=self.storage_path,
            experiment_dir_name=self.name,
            storage_filesystem=self.storage_filesystem,
        )
