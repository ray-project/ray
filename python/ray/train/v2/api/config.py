import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Union

import pyarrow.fs

from ray.air.config import (
    CheckpointConfig,
    FailureConfig as FailureConfigV1,
    ScalingConfig as ScalingConfigV1,
)
from ray.runtime_env import RuntimeEnv
from ray.train.v2._internal.constants import _DEPRECATED
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
            ``resources_per_worker`` argument.
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

    Example:

        .. testcode::

            from ray.train import ScalingConfig
            scaling_config = ScalingConfig(
                # Number of distributed workers.
                num_workers=2,
                # Turn on/off GPU.
                use_gpu=True,
            )

        .. testoutput::
            :hide:

            ...

    """

    trainer_resources: Optional[dict] = None
    use_tpu: Union[bool] = False
    topology: Optional[str] = None

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
        # TODO(justinvyu): Add link to migration guide.
        if self.fail_fast != _DEPRECATED:
            raise DeprecationWarning(FAIL_FAST_DEPRECATION_MESSAGE)


@dataclass
@PublicAPI(stability="stable")
class RunConfig:
    """Runtime configuration for training runs.

    Args:
        name: Name of the trial or experiment. If not provided, will be deduced
            from the Trainable.
        storage_path: [Beta] Path where all results and checkpoints are persisted.
            Can be a local directory or a destination on cloud storage.
            For multi-node training/tuning runs, this must be set to a
            shared storage location (e.g., S3, NFS).
            This defaults to the local ``~/ray_results`` directory.
        storage_filesystem: [Beta] A custom filesystem to use for storage.
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

        # TODO(justinvyu): Add link to migration guide.
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
