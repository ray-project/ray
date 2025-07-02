import logging
import os
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Union

from ray._private.thirdparty.tabulate.tabulate import tabulate
from ray.air.config import (
    CheckpointConfig,
    FailureConfig as FailureConfigV1,
    ScalingConfig as ScalingConfigV1,
    _repr_dataclass,
)
from ray.runtime_env import RuntimeEnv
from ray.train.v2._internal.constants import _DEPRECATED
from ray.train.v2._internal.migration_utils import (
    FAIL_FAST_DEPRECATION_MESSAGE,
    TRAINER_RESOURCES_DEPRECATION_MESSAGE,
)
from ray.train.v2._internal.util import date_str
from ray.util.annotations import PublicAPI, RayDeprecationWarning
from ray.widgets import Template

if TYPE_CHECKING:
    from ray.train import UserCallback

import pyarrow.fs

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
            or is able to autoscale to fulfill the request.

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

    def __post_init__(self):
        if self.trainer_resources is not None:
            raise DeprecationWarning(TRAINER_RESOURCES_DEPRECATION_MESSAGE)

        super().__post_init__()

    @property
    def _trainer_resources_not_none(self):
        return {}


@dataclass
class FailureConfig(FailureConfigV1):
    """Configuration related to failure handling of each training run.

    Args:
        max_failures: Tries to recover a run at least this many times.
            Will recover from the latest checkpoint if present.
            Setting to -1 will lead to infinite recovery retries.
            Setting to 0 will disable retries. Defaults to 0.
    """

    fail_fast: Union[bool, str] = _DEPRECATED

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

    # Deprecated
    local_dir: Optional[str] = None

    def __post_init__(self):
        from ray.train.constants import DEFAULT_STORAGE_PATH

        if self.local_dir is not None:
            raise DeprecationWarning(
                "The `RunConfig(local_dir)` argument is deprecated. "
                "You should set the `RunConfig(storage_path)` instead."
                "See the docs: https://docs.ray.io/en/latest/train/user-guides/"
                "persistent-storage.html#setting-the-local-staging-directory"
            )

        if self.storage_path is None:
            self.storage_path = DEFAULT_STORAGE_PATH

            # TODO(justinvyu): [Deprecated]
            ray_storage_uri: Optional[str] = os.environ.get("RAY_STORAGE")
            if ray_storage_uri is not None:
                logger.info(
                    "Using configured Ray Storage URI as the `storage_path`: "
                    f"{ray_storage_uri}"
                )
                warnings.warn(
                    "The `RAY_STORAGE` environment variable is deprecated. "
                    "Please use `RunConfig(storage_path)` instead.",
                    RayDeprecationWarning,
                    stacklevel=2,
                )
                self.storage_path = ray_storage_uri

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

    def __repr__(self):

        return _repr_dataclass(
            self,
            default_values={
                "failure_config": FailureConfig(),
                "checkpoint_config": CheckpointConfig(),
            },
        )

    def _repr_html_(self) -> str:
        reprs = []
        if self.failure_config is not None:
            reprs.append(
                Template("title_data_mini.html.j2").render(
                    title="Failure Config", data=self.failure_config._repr_html_()
                )
            )
        if self.checkpoint_config is not None:
            reprs.append(
                Template("title_data_mini.html.j2").render(
                    title="Checkpoint Config", data=self.checkpoint_config._repr_html_()
                )
            )

        # Create a divider between each displayed repr
        subconfigs = [Template("divider.html.j2").render()] * (2 * len(reprs) - 1)
        subconfigs[::2] = reprs

        settings = Template("scrollableTable.html.j2").render(
            table=tabulate(
                {
                    "Name": self.name,
                    "Local results directory": self.local_dir,
                    "Verbosity": self.verbose,
                    "Log to file": self.log_to_file,
                }.items(),
                tablefmt="html",
                headers=["Setting", "Value"],
                showindex=False,
            ),
            max_height="300px",
        )

        return Template("title_data.html.j2").render(
            title="RunConfig",
            data=Template("run_config.html.j2").render(
                subconfigs=subconfigs,
                settings=settings,
            ),
        )
