from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Mapping, Optional, Union

from ray.air.constants import WILDCARD_KEY
from ray.tune.syncer import SyncConfig
from ray.tune.utils.log import Verbosity
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.data import Dataset
    from ray.tune.callback import Callback
    from ray.tune.stopper import Stopper
    from ray.tune.execution.placement_groups import PlacementGroupFactory

ScalingConfig = Dict[str, Any]


@dataclass
@PublicAPI(stability="alpha")
class ScalingConfigDataClass:
    """Configuration for scaling training.

    This is the schema for the scaling_config dict, and after beta, this will be the
    actual representation for Scaling config objects.

    trainer_resources: Resources to allocate for the trainer. If none is provided,
        will default to 1 CPU.
    num_workers: The number of workers (Ray actors) to launch.
        Each worker will reserve 1 CPU by default. The number of CPUs
        reserved by each worker can be overridden with the
        ``resources_per_worker`` argument.
    use_gpu: If True, training will be done on GPUs (1 per worker).
        Defaults to False. The number of GPUs reserved by each
        worker can be overridden with the ``resources_per_worker``
        argument.
    resources_per_worker: If specified, the resources
        defined in this Dict will be reserved for each worker. The
        ``CPU`` and ``GPU`` keys (case-sensitive) can be defined to
        override the number of CPU/GPUs used by each worker.
    placement_strategy: The placement strategy to use for the
        placement group of the Ray actors. See :ref:`Placement Group
        Strategies <pgroup-strategy>` for the possible options.
    """

    trainer_resources: Optional[Dict] = None
    num_workers: Optional[int] = None
    use_gpu: bool = False
    resources_per_worker: Optional[Dict] = None
    placement_strategy: str = "PACK"

    def __post_init__(self):
        self.resources_per_worker = (
            self.resources_per_worker if self.resources_per_worker else {}
        )
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

    @property
    def num_cpus_per_worker(self):
        """The number of CPUs to set per worker."""
        return self.resources_per_worker.get("CPU", 1)

    @property
    def num_gpus_per_worker(self):
        """The number of GPUs to set per worker."""
        return self.resources_per_worker.get("GPU", int(self.use_gpu))

    @property
    def additional_resources_per_worker(self):
        """Resources per worker, not including CPU or GPU resources."""
        return {
            k: v
            for k, v in self.resources_per_worker.items()
            if k not in ["CPU", "GPU"]
        }

    def as_placement_group_factory(self) -> "PlacementGroupFactory":
        """Returns a PlacementGroupFactory to specify resources for Tune."""
        from ray.tune.execution.placement_groups import PlacementGroupFactory

        trainer_resources = (
            self.trainer_resources if self.trainer_resources else {"CPU": 1}
        )
        trainer_bundle = [trainer_resources]
        worker_resources = {
            "CPU": self.num_cpus_per_worker,
            "GPU": self.num_gpus_per_worker,
        }
        worker_resources_extra = (
            {} if self.resources_per_worker is None else self.resources_per_worker
        )
        worker_bundles = [
            {**worker_resources, **worker_resources_extra}
            for _ in range(self.num_workers if self.num_workers else 0)
        ]
        bundles = trainer_bundle + worker_bundles
        return PlacementGroupFactory(bundles, strategy=self.placement_strategy)


@dataclass
@PublicAPI(stability="alpha")
class DatasetConfig:
    """Configuration for ingest of a single Dataset.

    These configs define how the Dataset should be read into the DataParallelTrainer.
    It configures the preprocessing, splitting, and ingest strategy per-dataset.

    DataParallelTrainers declare default DatasetConfigs for each dataset passed in the
    ``datasets`` argument. Users have the opportunity to selectively override these
    configs by passing the ``dataset_config`` argument. Trainers can also define user
    customizable values (e.g., XGBoostTrainer doesn't support streaming ingest).
    """

    # TODO(ekl) could we unify DataParallelTrainer and Trainer so the same data ingest
    # strategy applies to all Trainers?

    # Whether to fit preprocessors on this dataset. This can be set on at most one
    # dataset at a time.
    # True by default for the "train" dataset only.
    fit: Optional[bool] = None

    # Whether the dataset should be split across multiple workers.
    # True by default for the "train" dataset only.
    split: Optional[bool] = None

    # Whether to raise an error if the Dataset isn't provided by the user.
    # False by default.
    required: Optional[bool] = None

    # Whether to transform the dataset with the fitted preprocessor. This must be
    # enabled at least for the dataset that is fit.
    # True by default.
    transform: Optional[bool] = None

    # Whether the dataset should be streamed into memory using pipelined reads.
    # When enabled, get_dataset_shard() returns DatasetPipeline instead of Dataset.
    # The amount of memory to use is controlled by `stream_window_size`.
    # False by default.
    use_stream_api: Optional[bool] = None

    # Configure the streaming window size in bytes. A good value is something like
    # 20% of object store memory. If set to -1, then an infinite window size will be
    # used (similar to bulk ingest). This only has an effect if use_stream_api is set.
    # Set to 1.0 GiB by default.
    stream_window_size: Optional[float] = None

    # Whether to enable global shuffle (per pipeline window in streaming mode). Note
    # that this is an expensive all-to-all operation, and most likely you want to use
    # local shuffle instead. See https://docs.ray.io/en/master/data/faq.html
    # False by default.
    global_shuffle: Optional[bool] = None

    def fill_defaults(self) -> "DatasetConfig":
        """Return a copy of this config with all default values filled in."""
        return DatasetConfig(
            fit=self.fit or False,
            split=self.split or False,
            required=self.required or False,
            use_stream_api=self.use_stream_api or False,
            stream_window_size=self.stream_window_size
            if self.stream_window_size is not None
            else 1024 * 1024 * 1024,
            global_shuffle=self.global_shuffle or False,
            transform=self.transform if self.transform is not None else True,
        )

    @staticmethod
    def merge(
        a: Dict[str, "DatasetConfig"], b: Optional[Dict[str, "DatasetConfig"]]
    ) -> Dict[str, "DatasetConfig"]:
        """Merge two given DatasetConfigs, the second taking precedence.

        Raises:
            ValueError if validation fails on the merged configs.
        """
        has_wildcard = WILDCARD_KEY in a
        result = a.copy()
        if b is None:
            return result
        for key in b:
            if key in a:
                result[key] = a[key]._merge(b[key])
            elif has_wildcard:
                result[key] = a[WILDCARD_KEY]._merge(b[key])
            else:
                raise ValueError(
                    f"Invalid dataset config `{key}`. It must be one of `{list(a)}`."
                )
        return result

    @staticmethod
    def validated(
        config: Dict[str, "DatasetConfig"], datasets: Dict[str, "Dataset"]
    ) -> Dict[str, "DatasetConfig"]:
        """Validate the given config and datasets are usable.

        Returns dict of validated configs with defaults filled out.
        """
        has_wildcard = WILDCARD_KEY in config
        fittable = set()
        result = {k: v.fill_defaults() for k, v in config.items()}
        for k, v in result.items():
            if v.fit:
                fittable.add(k)
                if not v.transform:
                    raise ValueError(
                        f"Error configuring dataset `{k}`: cannot specify both "
                        "fit=True and transform=False."
                    )
            if v.required:
                if k not in datasets:
                    raise ValueError(
                        f"The required dataset `{k}` was not found in {datasets}."
                    )
        if len(fittable) > 1:
            raise ValueError(
                f"More than one dataset was specified to be fit: {fittable}"
            )
        if not has_wildcard:
            for k, v in datasets.items():
                if k not in result:
                    raise ValueError(
                        f"An unexpected dataset `{k}` was given. The list of expected "
                        f"datasets is `{list(result)}`."
                    )
        return result

    def _merge(self, other: "DatasetConfig") -> "DatasetConfig":
        """Merge the given DatasetConfig into this one."""
        new_config = DatasetConfig(
            fit=self.fit if other.fit is None else other.fit,
            split=self.split if other.split is None else other.split,
            required=self.required if other.required is None else other.required,
            transform=self.transform if other.transform is None else other.transform,
            use_stream_api=self.use_stream_api
            if other.use_stream_api is None
            else other.use_stream_api,
            stream_window_size=self.stream_window_size
            if other.stream_window_size is None
            else other.stream_window_size,
            global_shuffle=self.global_shuffle
            if other.global_shuffle is None
            else other.global_shuffle,
        )
        return new_config


@dataclass
@PublicAPI(stability="alpha")
class FailureConfig:
    """Configuration related to failure handling of each run/trial.

    Args:
        max_failures: Tries to recover a run at least this many times.
            Will recover from the latest checkpoint if present.
            Setting to -1 will lead to infinite recovery retries.
            Setting to 0 will disable retries. Defaults to 0.
    """

    max_failures: int = 0


@dataclass
@PublicAPI(stability="alpha")
class RunConfig:
    """Runtime configuration for individual trials that are run.

    This contains information that applies to individual runs of Trainable classes.
    This includes both running a Trainable by itself or running a hyperparameter
    tuning job on top of a Trainable (applies to each trial).

    At resume, Ray Tune will automatically apply the same run config so that resumed
    run uses the same run config as the original run.

    Args:
        name: Name of the trial or experiment. If not provided, will be deduced
            from the Trainable.
        local_dir: Local dir to save training results to.
            Defaults to ``~/ray_results``.
        stop: Stop conditions to consider. Refer to ray.tune.stopper.Stopper
            for more info. Stoppers should be serializable.
        callbacks: Callbacks to invoke.
            Refer to ray.tune.callback.Callback for more info.
            Callbacks should be serializable.
            Currently only stateless callbacks are supported for resumed runs.
            (any state of the callback will not be checkpointed by Tune
            and thus will not take effect in resumed runs).
        failure_config: The failure mode configuration.
        sync_config: Configuration object for syncing. See tune.SyncConfig.
        verbose: 0, 1, 2, or 3. Verbosity mode.
            0 = silent, 1 = only status updates, 2 = status and brief
            results, 3 = status and detailed results. Defaults to 2.
    """

    # TODO(xwjiang): Add more.
    name: Optional[str] = None
    local_dir: Optional[str] = None
    callbacks: Optional[List["Callback"]] = None
    stop: Optional[Union[Mapping, "Stopper", Callable[[str, Mapping], bool]]] = None
    failure_config: Optional[FailureConfig] = None
    sync_config: Optional[SyncConfig] = None
    verbose: Union[int, Verbosity] = Verbosity.V3_TRIAL_DETAILS
