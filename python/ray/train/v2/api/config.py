from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, List, Mapping, Optional, Tuple, Union

from ray.air.config import FailureConfig as FailureConfigV1
from ray.air.config import RunConfig as RunConfigV1
from ray.air.config import ScalingConfig as ScalingConfigV1
from ray.train.v2._internal.constants import _DEPRECATED
from ray.train.v2._internal.execution.callback import Callback
from ray.train.v2._internal.util import date_str

if TYPE_CHECKING:
    from ray.train import SyncConfig
    from ray.tune.experimental.output import AirVerbosity
    from ray.tune.progress_reporter import ProgressReporter
    from ray.tune.stopper import Stopper
    from ray.tune.utils.log import Verbosity


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

    trainer_resources: Union[Optional[dict], str] = _DEPRECATED

    def __post_init__(self):
        if self.trainer_resources != _DEPRECATED:
            raise NotImplementedError(
                "`ScalingConfig(trainer_resources)` is deprecated. "
                "This parameter was an advanced configuration that specified "
                "resources for the Ray Train driver actor, which doesn't "
                "need to reserve logical resources because it doesn't perform "
                "any heavy computation. "
                "Only the `resources_per_worker` parameter is useful "
                "to specify resources for the training workers."
            )

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
            raise NotImplementedError("`FailureConfig(fail_fast)` is deprecated.")


@dataclass
class RunConfig(RunConfigV1):
    """Runtime configuration for training runs.

    Upon resuming from a training run checkpoint,
    Ray Train will automatically apply the RunConfig from
    the previously checkpointed run.

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
    """

    callbacks: Optional[List["Callback"]] = None
    sync_config: Union[Optional["SyncConfig"], str] = _DEPRECATED
    verbose: Union[Optional[Union[int, "AirVerbosity", "Verbosity"]], str] = _DEPRECATED
    stop: Union[
        Optional[Union[Mapping, "Stopper", Callable[[str, Mapping], bool]]], str
    ] = _DEPRECATED
    progress_reporter: Union[Optional["ProgressReporter"], str] = _DEPRECATED
    log_to_file: Union[bool, str, Tuple[str, str]] = _DEPRECATED

    def __post_init__(self):
        super().__post_init__()

        if self.callbacks is not None:
            raise NotImplementedError("`RunConfig(callbacks)` is not supported yet.")

        # TODO(justinvyu): Add link to migration guide.
        run_config_deprecation_message = (
            "`RunConfig({})` is deprecated. This configuration was a "
            "Ray Tune API that did not support Ray Train usage well, "
            "so we are dropping support going forward. "
            "If you heavily rely on these configurations, "
            "you can run Ray Train as a single Ray Tune trial."
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
                raise NotImplementedError(run_config_deprecation_message.format(param))

        if not self.name:
            self.name = f"ray_train_run-{date_str()}"
