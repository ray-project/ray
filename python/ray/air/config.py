import logging
from collections import Counter, defaultdict
from dataclasses import _MISSING_TYPE, dataclass, fields
import os
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
)
import warnings

import pyarrow.fs

import ray
from ray._common.utils import RESOURCE_CONSTRAINT_PREFIX
from ray._private.thirdparty.tabulate.tabulate import tabulate
from ray.util.annotations import PublicAPI, RayDeprecationWarning
from ray.widgets import Template, make_table_html_repr

if TYPE_CHECKING:
    import ray.tune.progress_reporter
    from ray.tune.callback import Callback
    from ray.tune.execution.placement_groups import PlacementGroupFactory
    from ray.tune.experimental.output import AirVerbosity
    from ray.tune.search.sample import Domain
    from ray.tune.stopper import Stopper
    from ray.tune.utils.log import Verbosity


# Dict[str, List] is to support `tune.grid_search`:
# TODO(sumanthratna/matt): Upstream this to Tune.
SampleRange = Union["Domain", Dict[str, List]]


MAX = "max"
MIN = "min"
_DEPRECATED_VALUE = "DEPRECATED"


logger = logging.getLogger(__name__)


def _repr_dataclass(obj, *, default_values: Optional[Dict[str, Any]] = None) -> str:
    """A utility function to elegantly represent dataclasses.

    In contrast to the default dataclass `__repr__`, which shows all parameters, this
    function only shows parameters with non-default values.

    Args:
        obj: The dataclass to represent.
        default_values: An optional dictionary that maps field names to default values.
            Use this parameter to specify default values that are generated dynamically
            (e.g., in `__post_init__` or by a `default_factory`). If a default value
            isn't specified in `default_values`, then the default value is inferred from
            the `dataclass`.

    Returns:
        A representation of the dataclass.
    """
    if default_values is None:
        default_values = {}

    non_default_values = {}  # Maps field name to value.

    def equals(value, default_value):
        # We need to special case None because of a bug in pyarrow:
        # https://github.com/apache/arrow/issues/38535
        if value is None and default_value is None:
            return True
        if value is None or default_value is None:
            return False
        return value == default_value

    for field in fields(obj):
        value = getattr(obj, field.name)
        default_value = default_values.get(field.name, field.default)
        is_required = isinstance(field.default, _MISSING_TYPE)
        if is_required or not equals(value, default_value):
            non_default_values[field.name] = value

    string = f"{obj.__class__.__name__}("
    string += ", ".join(
        f"{name}={value!r}" for name, value in non_default_values.items()
    )
    string += ")"

    return string


@dataclass
@PublicAPI(stability="stable")
class ScalingConfig:
    """Configuration for scaling training.

    For more details, see :ref:`train_scaling_config`.

    Args:
        trainer_resources: Resources to allocate for the training coordinator.
            The training coordinator launches the worker group and executes
            the training function per worker, and this process does NOT require
            GPUs. The coordinator is always scheduled on the same node as the
            rank 0 worker, so one example use case is to set a minimum amount
            of resources (e.g. CPU memory) required by the rank 0 node.
            By default, this assigns 1 CPU to the training coordinator.
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
            Define the ``"CPU"`` key (case-sensitive) to
            override the number of CPUs used by each worker.
            This can also be used to request :ref:`custom resources <custom-resources>`.
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

        .. code-block:: python

            from ray.train import ScalingConfig
            scaling_config = ScalingConfig(
                # Number of distributed workers.
                num_workers=2,
                # Turn on/off GPU.
                use_gpu=True,
                # Assign extra CPU/GPU/custom resources per worker.
                resources_per_worker={"GPU": 1, "CPU": 1, "memory": 1e9, "custom": 1.0},
                # Try to schedule workers on different nodes.
                placement_strategy="SPREAD",
            )

    """

    trainer_resources: Optional[Union[Dict, SampleRange]] = None
    num_workers: Union[int, SampleRange] = 1
    use_gpu: Union[bool, SampleRange] = False
    resources_per_worker: Optional[Union[Dict, SampleRange]] = None
    placement_strategy: Union[str, SampleRange] = "PACK"
    accelerator_type: Optional[str] = None

    def __post_init__(self):
        if self.resources_per_worker:
            if not self.use_gpu and self.num_gpus_per_worker > 0:
                raise ValueError(
                    "`use_gpu` is False but `GPU` was found in "
                    "`resources_per_worker`. Either set `use_gpu` to True or "
                    "remove `GPU` from `resources_per_worker."
                )

            if self.use_gpu and self.num_gpus_per_worker == 0:
                raise ValueError(
                    "`use_gpu` is True but `GPU` is set to 0 in "
                    "`resources_per_worker`. Either set `use_gpu` to False or "
                    "request a positive number of `GPU` in "
                    "`resources_per_worker."
                )

    def __repr__(self):
        return _repr_dataclass(self)

    def _repr_html_(self) -> str:
        return make_table_html_repr(obj=self, title=type(self).__name__)

    def __eq__(self, o: "ScalingConfig") -> bool:
        if not isinstance(o, type(self)):
            return False
        return self.as_placement_group_factory() == o.as_placement_group_factory()

    @property
    def _resources_per_worker_not_none(self):
        if self.resources_per_worker is None:
            if self.use_gpu:
                # Note that we don't request any CPUs, which avoids possible
                # scheduling contention. Generally nodes have many more CPUs than
                # GPUs, so not requesting a CPU does not lead to oversubscription.
                resources_per_worker = {"GPU": 1}
            else:
                resources_per_worker = {"CPU": 1}
        else:
            resources_per_worker = {
                k: v for k, v in self.resources_per_worker.items() if v != 0
            }

        if self.use_gpu:
            resources_per_worker.setdefault("GPU", 1)

        if self.accelerator_type:
            accelerator = f"{RESOURCE_CONSTRAINT_PREFIX}{self.accelerator_type}"
            resources_per_worker.setdefault(accelerator, 0.001)
        return resources_per_worker

    @property
    def _trainer_resources_not_none(self):
        if self.trainer_resources is None:
            if self.num_workers:
                # For Google Colab, don't allocate resources to the base Trainer.
                # Colab only has 2 CPUs, and because of this resource scarcity,
                # we have to be careful on where we allocate resources. Since Colab
                # is not distributed, the concern about many parallel Ray Tune trials
                # leading to all Trainers being scheduled on the head node if we set
                # `trainer_resources` to 0 is no longer applicable.
                try:
                    import google.colab  # noqa: F401

                    trainer_num_cpus = 0
                except ImportError:
                    trainer_num_cpus = 1
            else:
                # If there are no additional workers, then always reserve 1 CPU for
                # the Trainer.
                trainer_num_cpus = 1

            trainer_resources = {"CPU": trainer_num_cpus}
        else:
            trainer_resources = {
                k: v for k, v in self.trainer_resources.items() if v != 0
            }

        return trainer_resources

    @property
    def total_resources(self):
        """Map of total resources required for the trainer."""
        total_resource_map = defaultdict(float, self._trainer_resources_not_none)
        for k, value in self._resources_per_worker_not_none.items():
            total_resource_map[k] += value * self.num_workers
        return dict(total_resource_map)

    @property
    def num_cpus_per_worker(self):
        """The number of CPUs to set per worker."""
        return self._resources_per_worker_not_none.get("CPU", 0)

    @property
    def num_gpus_per_worker(self):
        """The number of GPUs to set per worker."""
        return self._resources_per_worker_not_none.get("GPU", 0)

    @property
    def additional_resources_per_worker(self):
        """Resources per worker, not including CPU or GPU resources."""
        return {
            k: v
            for k, v in self._resources_per_worker_not_none.items()
            if k not in ["CPU", "GPU"]
        }

    def as_placement_group_factory(self) -> "PlacementGroupFactory":
        """Returns a PlacementGroupFactory to specify resources for Tune."""
        from ray.tune.execution.placement_groups import PlacementGroupFactory

        trainer_bundle = self._trainer_resources_not_none
        worker_bundle = self._resources_per_worker_not_none

        # Colocate Trainer and rank0 worker by merging their bundles
        # Note: This empty bundle is required so that the Tune actor manager schedules
        # the Trainable onto the combined bundle while taking none of its resources,
        # rather than a non-empty head bundle.
        combined_bundle = dict(Counter(trainer_bundle) + Counter(worker_bundle))
        bundles = [{}, combined_bundle] + [worker_bundle] * (self.num_workers - 1)
        return PlacementGroupFactory(bundles, strategy=self.placement_strategy)

    @classmethod
    def from_placement_group_factory(
        cls, pgf: "PlacementGroupFactory"
    ) -> "ScalingConfig":
        """Create a ScalingConfig from a Tune's PlacementGroupFactory

        Note that this is only needed for ResourceChangingScheduler, which
        modifies a trial's PlacementGroupFactory but doesn't propagate
        the changes to ScalingConfig. TrainTrainable needs to reconstruct
        a ScalingConfig from on the trial's PlacementGroupFactory.
        """

        # pgf.bundles = [{trainer + worker}, {worker}, ..., {worker}]
        num_workers = len(pgf.bundles)
        combined_resources = pgf.bundles[0]
        resources_per_worker = pgf.bundles[-1]
        use_gpu = bool(resources_per_worker.get("GPU", False))
        placement_strategy = pgf.strategy

        # In `as_placement_group_factory`, we merged the trainer resource into the
        # first worker resources bundle. We need to calculate the resources diff to
        # get the trainer resources.
        # Note: If there's only one worker, we won't be able to calculate the diff.
        # We'll have empty trainer bundle and assign all resources to the worker.
        trainer_resources = dict(
            Counter(combined_resources) - Counter(resources_per_worker)
        )

        return ScalingConfig(
            trainer_resources=trainer_resources,
            num_workers=num_workers,
            use_gpu=use_gpu,
            resources_per_worker=resources_per_worker,
            placement_strategy=placement_strategy,
        )


@dataclass
@PublicAPI(stability="stable")
class FailureConfig:
    """Configuration related to failure handling of each training/tuning run.

    Args:
        max_failures: Tries to recover a run at least this many times.
            Will recover from the latest checkpoint if present.
            Setting to -1 will lead to infinite recovery retries.
            Setting to 0 will disable retries. Defaults to 0.
        fail_fast: Whether to fail upon the first error.
            If fail_fast='raise' provided, the original error during training will be
            immediately raised. fail_fast='raise' can easily leak resources and
            should be used with caution.
    """

    max_failures: int = 0
    fail_fast: Union[bool, str] = False

    def __post_init__(self):
        # Same check as in TuneController
        if not (isinstance(self.fail_fast, bool) or self.fail_fast.upper() == "RAISE"):
            raise ValueError(
                "fail_fast must be one of {bool, 'raise'}. " f"Got {self.fail_fast}."
            )

        # Same check as in tune.run
        if self.fail_fast and self.max_failures != 0:
            raise ValueError(
                f"max_failures must be 0 if fail_fast={repr(self.fail_fast)}."
            )

    def __repr__(self):
        return _repr_dataclass(self)

    def _repr_html_(self):
        return Template("scrollableTable.html.j2").render(
            table=tabulate(
                {
                    "Setting": ["Max failures", "Fail fast"],
                    "Value": [self.max_failures, self.fail_fast],
                },
                tablefmt="html",
                showindex=False,
                headers="keys",
            ),
            max_height="none",
        )


@dataclass
@PublicAPI(stability="stable")
class CheckpointConfig:
    """Configurable parameters for defining the checkpointing strategy.

    Default behavior is to persist all checkpoints to disk. If
    ``num_to_keep`` is set, the default retention policy is to keep the
    checkpoints with maximum timestamp, i.e. the most recent checkpoints.

    Args:
        num_to_keep: The number of checkpoints to keep
            on disk for this run. If a checkpoint is persisted to disk after
            there are already this many checkpoints, then an existing
            checkpoint will be deleted. If this is ``None`` then checkpoints
            will not be deleted. Must be >= 1.
        checkpoint_score_attribute: The attribute that will be used to
            score checkpoints to determine which checkpoints should be kept
            on disk when there are greater than ``num_to_keep`` checkpoints.
            This attribute must be a key from the checkpoint
            dictionary which has a numerical value. Per default, the last
            checkpoints will be kept.
        checkpoint_score_order: Either "max" or "min".
            If "max", then checkpoints with highest values of
            ``checkpoint_score_attribute`` will be kept.
            If "min", then checkpoints with lowest values of
            ``checkpoint_score_attribute`` will be kept.
        checkpoint_frequency: Number of iterations between checkpoints. If 0
            this will disable checkpointing.
            Please note that most trainers will still save one checkpoint at
            the end of training.
            This attribute is only supported
            by trainers that don't take in custom training loops.
        checkpoint_at_end: If True, will save a checkpoint at the end of training.
            This attribute is only supported by trainers that don't take in
            custom training loops. Defaults to True for trainers that support it
            and False for generic function trainables.
        _checkpoint_keep_all_ranks: This experimental config is deprecated.
            This behavior is now controlled by reporting `checkpoint=None`
            in the workers that shouldn't persist a checkpoint.
            For example, if you only want the rank 0 worker to persist a checkpoint
            (e.g., in standard data parallel training), then you should save and
            report a checkpoint if `ray.train.get_context().get_world_rank() == 0`
            and `None` otherwise.
        _checkpoint_upload_from_workers: This experimental config is deprecated.
            Uploading checkpoint directly from the worker is now the default behavior.
    """

    num_to_keep: Optional[int] = None
    checkpoint_score_attribute: Optional[str] = None
    checkpoint_score_order: Optional[str] = MAX
    checkpoint_frequency: Optional[int] = 0
    checkpoint_at_end: Optional[bool] = None
    _checkpoint_keep_all_ranks: Optional[bool] = _DEPRECATED_VALUE
    _checkpoint_upload_from_workers: Optional[bool] = _DEPRECATED_VALUE

    def __post_init__(self):
        if self._checkpoint_keep_all_ranks != _DEPRECATED_VALUE:
            raise DeprecationWarning(
                "The experimental `_checkpoint_keep_all_ranks` config is deprecated. "
                "This behavior is now controlled by reporting `checkpoint=None` "
                "in the workers that shouldn't persist a checkpoint. "
                "For example, if you only want the rank 0 worker to persist a "
                "checkpoint (e.g., in standard data parallel training), "
                "then you should save and report a checkpoint if "
                "`ray.train.get_context().get_world_rank() == 0` "
                "and `None` otherwise."
            )

        if self._checkpoint_upload_from_workers != _DEPRECATED_VALUE:
            raise DeprecationWarning(
                "The experimental `_checkpoint_upload_from_workers` config is "
                "deprecated. Uploading checkpoint directly from the worker is "
                "now the default behavior."
            )

        if self.num_to_keep is not None and self.num_to_keep <= 0:
            raise ValueError(
                f"Received invalid num_to_keep: "
                f"{self.num_to_keep}. "
                f"Must be None or an integer >= 1."
            )
        if self.checkpoint_score_order not in (MAX, MIN):
            raise ValueError(
                f"checkpoint_score_order must be either " f'"{MAX}" or "{MIN}".'
            )

        if self.checkpoint_frequency < 0:
            raise ValueError(
                f"checkpoint_frequency must be >=0, got {self.checkpoint_frequency}"
            )

    def __repr__(self):
        return _repr_dataclass(self)

    def _repr_html_(self) -> str:
        if self.num_to_keep is None:
            num_to_keep_repr = "All"
        else:
            num_to_keep_repr = self.num_to_keep

        if self.checkpoint_score_attribute is None:
            checkpoint_score_attribute_repr = "Most recent"
        else:
            checkpoint_score_attribute_repr = self.checkpoint_score_attribute

        if self.checkpoint_at_end is None:
            checkpoint_at_end_repr = ""
        else:
            checkpoint_at_end_repr = self.checkpoint_at_end

        return Template("scrollableTable.html.j2").render(
            table=tabulate(
                {
                    "Setting": [
                        "Number of checkpoints to keep",
                        "Checkpoint score attribute",
                        "Checkpoint score order",
                        "Checkpoint frequency",
                        "Checkpoint at end",
                    ],
                    "Value": [
                        num_to_keep_repr,
                        checkpoint_score_attribute_repr,
                        self.checkpoint_score_order,
                        self.checkpoint_frequency,
                        checkpoint_at_end_repr,
                    ],
                },
                tablefmt="html",
                showindex=False,
                headers="keys",
            ),
            max_height="none",
        )

    @property
    def _tune_legacy_checkpoint_score_attr(self) -> Optional[str]:
        """Same as ``checkpoint_score_attr`` in ``tune.run``.

        Only used for Legacy API compatibility.
        """
        if self.checkpoint_score_attribute is None:
            return self.checkpoint_score_attribute
        prefix = ""
        if self.checkpoint_score_order == MIN:
            prefix = "min-"
        return f"{prefix}{self.checkpoint_score_attribute}"


@dataclass
@PublicAPI(stability="stable")
class RunConfig:
    """Runtime configuration for training and tuning runs.

    Upon resuming from a training or tuning run checkpoint,
    Ray Train/Tune will automatically apply the RunConfig from
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
        sync_config: Configuration object for syncing. See train.SyncConfig.
        verbose: 0, 1, or 2. Verbosity mode.
            0 = silent, 1 = default, 2 = verbose. Defaults to 1.
            If the ``RAY_AIR_NEW_OUTPUT=1`` environment variable is set,
            uses the old verbosity settings:
            0 = silent, 1 = only status updates, 2 = status and brief
            results, 3 = status and detailed results.
        stop: Stop conditions to consider. Refer to ray.tune.stopper.Stopper
            for more info. Stoppers should be serializable.
        callbacks: [DeveloperAPI] Callbacks to invoke.
            Refer to ray.tune.callback.Callback for more info.
            Callbacks should be serializable.
            Currently only stateless callbacks are supported for resumed runs.
            (any state of the callback will not be checkpointed by Tune
            and thus will not take effect in resumed runs).
        progress_reporter: [DeveloperAPI] Progress reporter for reporting
            intermediate experiment progress. Defaults to CLIReporter if
            running in command-line, or JupyterNotebookReporter if running in
            a Jupyter notebook.
        log_to_file: [DeveloperAPI] Log stdout and stderr to files in
            trial directories. If this is `False` (default), no files
            are written. If `true`, outputs are written to `trialdir/stdout`
            and `trialdir/stderr`, respectively. If this is a single string,
            this is interpreted as a file relative to the trialdir, to which
            both streams are written. If this is a Sequence (e.g. a Tuple),
            it has to have length 2 and the elements indicate the files to
            which stdout and stderr are written, respectively.

    """

    name: Optional[str] = None
    storage_path: Optional[str] = None
    storage_filesystem: Optional[pyarrow.fs.FileSystem] = None
    failure_config: Optional[FailureConfig] = None
    checkpoint_config: Optional[CheckpointConfig] = None
    sync_config: Optional["ray.train.SyncConfig"] = None
    verbose: Optional[Union[int, "AirVerbosity", "Verbosity"]] = None
    stop: Optional[Union[Mapping, "Stopper", Callable[[str, Mapping], bool]]] = None
    callbacks: Optional[List["Callback"]] = None
    progress_reporter: Optional["ray.tune.progress_reporter.ProgressReporter"] = None
    log_to_file: Union[bool, str, Tuple[str, str]] = False

    # Deprecated
    local_dir: Optional[str] = None

    def __post_init__(self):
        from ray.train import SyncConfig
        from ray.train.constants import DEFAULT_STORAGE_PATH
        from ray.tune.experimental.output import AirVerbosity, get_air_verbosity

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

        if not self.sync_config:
            self.sync_config = SyncConfig()

        if not self.checkpoint_config:
            self.checkpoint_config = CheckpointConfig()

        # Save the original verbose value to check for deprecations
        self._verbose = self.verbose
        if self.verbose is None:
            # Default `verbose` value. For new output engine,
            # this is AirVerbosity.DEFAULT.
            # For old output engine, this is Verbosity.V3_TRIAL_DETAILS
            # Todo (krfricke): Currently uses number to pass test_configs::test_repr
            self.verbose = get_air_verbosity(AirVerbosity.DEFAULT) or 3

        if isinstance(self.storage_path, Path):
            self.storage_path = self.storage_path.as_posix()

    def __repr__(self):
        from ray.train import SyncConfig

        return _repr_dataclass(
            self,
            default_values={
                "failure_config": FailureConfig(),
                "sync_config": SyncConfig(),
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
        if self.sync_config is not None:
            reprs.append(
                Template("title_data_mini.html.j2").render(
                    title="Sync Config", data=self.sync_config._repr_html_()
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
