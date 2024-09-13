from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, List, Mapping, Optional, Tuple, Union

from ray.air.config import FailureConfig as FailureConfigV1
from ray.air.config import RunConfig as RunConfigV1
from ray.air.config import SampleRange
from ray.air.config import ScalingConfig as ScalingConfigV1
from ray.train.v2._internal.constants import _UNSUPPORTED
from ray.train.v2._internal.execution.callback import Callback
from ray.train.v2._internal.util import date_str

if TYPE_CHECKING:
    from ray.train import SyncConfig
    from ray.tune.experimental.output import AirVerbosity
    from ray.tune.progress_reporter import ProgressReporter
    from ray.tune.stopper import Stopper
    from ray.tune.utils.log import Verbosity


_UNSUPPORTED_MESSAGE = "The '{}' argument is not supported yet."


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

    trainer_resources: Optional[dict] = _UNSUPPORTED
    placement_strategy: Union[str, SampleRange] = _UNSUPPORTED

    def __post_init__(self):
        super().__post_init__()

        unsupported_params = ["trainer_resources", "placement_strategy"]
        for param in unsupported_params:
            if getattr(self, param) != _UNSUPPORTED:
                raise NotImplementedError(_UNSUPPORTED_MESSAGE.format(param))


@dataclass
class FailureConfig(FailureConfigV1):
    """Configuration related to failure handling of each training run.

    Args:
        max_failures: Tries to recover a run at least this many times.
            Will recover from the latest checkpoint if present.
            Setting to -1 will lead to infinite recovery retries.
            Setting to 0 will disable retries. Defaults to 0.
    """

    fail_fast: Union[bool, str] = _UNSUPPORTED

    def __post_init__(self):
        if self.fail_fast != _UNSUPPORTED:
            raise NotImplementedError(_UNSUPPORTED_MESSAGE.format("fail_fast"))


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

    callbacks: Optional[List["Callback"]] = _UNSUPPORTED
    sync_config: Optional["SyncConfig"] = _UNSUPPORTED
    verbose: Optional[Union[int, "AirVerbosity", "Verbosity"]] = _UNSUPPORTED
    stop: Optional[
        Union[Mapping, "Stopper", Callable[[str, Mapping], bool]]
    ] = _UNSUPPORTED
    progress_reporter: Optional["ProgressReporter"] = _UNSUPPORTED
    log_to_file: Union[bool, str, Tuple[str, str]] = _UNSUPPORTED

    def __post_init__(self):
        super().__post_init__()

        unsupported_params = [
            "callbacks",
            "sync_config",
            "verbose",
            "stop",
            "progress_reporter",
            "log_to_file",
        ]
        for param in unsupported_params:
            if getattr(self, param) != _UNSUPPORTED:
                raise NotImplementedError(_UNSUPPORTED_MESSAGE.format(param))

        if not self.name:
            self.name = f"ray_train_results-{date_str()}"
