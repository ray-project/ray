import copy
import os
from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Literal, Optional, Union

from ray.actor import ActorHandle
from ray.util.annotations import DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    from ray.data import DataIterator, Dataset, ExecutionOptions, NodeIdStr


@PublicAPI(stability="stable")
@dataclass(frozen=True)
class SplitConfig:
    """Per-dataset split behavior for Train datasets.

    Args:
        equal: Whether the dataset should be split equally across workers.
            When true, Ray Data may drop up to ``world_size - 1`` rows from
            the tail to keep worker shard lengths aligned.
        enable_shard_locality: Per-dataset override for shard locality.
            If unset, falls back to ``DataConfig(enable_shard_locality=...)``.
    """

    equal: bool = True
    enable_shard_locality: Optional[bool] = None


@PublicAPI(stability="stable")
class DataConfig:
    """Class responsible for configuring Train dataset preprocessing.

    For advanced use cases, this class can be subclassed and the `configure()` method
    overridden for custom data preprocessing.
    """

    def __init__(
        self,
        datasets_to_split: Union[
            Literal["all"], List[str], Dict[str, SplitConfig]
        ] = "all",
        unequal_split_datasets: Optional[List[str]] = None,
        execution_options: Optional[
            Union["ExecutionOptions", Dict[str, "ExecutionOptions"]]
        ] = None,
        enable_shard_locality: bool = True,
    ):
        """Construct a DataConfig.

        Args:
            datasets_to_split: Specifies which datasets should be split among workers.
                Can be set to "all", a list of dataset names, or a dict mapping
                dataset names to ``SplitConfig``. Dict-based configuration enables
                per-dataset control over equal splitting and shard locality while
                preserving the existing list-based API. Defaults to "all", i.e.
                split all datasets with the global defaults.
            unequal_split_datasets: Specifies which datasets should use unequal
                splitting (``equal=False``). By default, all datasets use equal
                splitting (``equal=True``) for backward compatibility. List datasets
                here that should NOT drop rows during splitting, such as evaluation
                or batch inference datasets. Only datasets that are also in
                ``datasets_to_split`` are affected. This is a legacy compatibility
                option; use ``datasets_to_split={name: SplitConfig(equal=False)}``
                for new code.
            execution_options: The execution options to pass to Ray Data. Can be either:
                1. A single ExecutionOptions object that is applied to all datasets.
                2. A dict mapping dataset names to ExecutionOptions. If a dataset name
                is not in the dict, it defaults to ``DataConfig.default_ingest_options()``.
                By default, the options are optimized for data ingest. When overriding,
                base your options off ``DataConfig.default_ingest_options()``.
            enable_shard_locality: If true, dataset sharding across Train workers will
                consider locality to minimize cross-node data transfer. Enabled by default.
        """
        from ray.data import ExecutionOptions

        self._dataset_split_configs: Dict[str, SplitConfig] = {}
        using_dict_split_config = isinstance(datasets_to_split, dict)
        if using_dict_split_config:
            invalid_dataset_names = [
                name for name in datasets_to_split if not isinstance(name, str)
            ]
            invalid_split_configs = [
                config
                for config in datasets_to_split.values()
                if not isinstance(config, SplitConfig)
            ]
            if invalid_dataset_names or invalid_split_configs:
                raise TypeError(
                    "`datasets_to_split` should be 'all', a list of strings of "
                    "dataset names, or a dict of dataset names to SplitConfig. "
                    f"Received value {datasets_to_split}."
                )
            self._datasets_to_split = list(datasets_to_split.keys())
            self._dataset_split_configs = dict(datasets_to_split)
        elif isinstance(datasets_to_split, list) or datasets_to_split == "all":
            self._datasets_to_split = datasets_to_split
        else:
            raise TypeError(
                "`datasets_to_split` should be 'all', a list of strings of "
                "dataset names, or a dict of dataset names to SplitConfig. Received "
                f"{type(datasets_to_split).__name__} with value {datasets_to_split}."
            )

        if unequal_split_datasets and using_dict_split_config:
            raise ValueError(
                "`unequal_split_datasets` cannot be combined with dict-based "
                "`datasets_to_split`. Use SplitConfig(equal=False) instead."
            )

        if unequal_split_datasets is None:
            self._unequal_split_datasets = []
        elif isinstance(unequal_split_datasets, list):
            self._unequal_split_datasets = unequal_split_datasets
        else:
            raise TypeError(
                "`unequal_split_datasets` should be a list of strings of "
                "dataset names or None. Received "
                f"{type(unequal_split_datasets).__name__} with value {unequal_split_datasets}."
            )

        default_execution_options = DataConfig.default_ingest_options()

        if isinstance(execution_options, ExecutionOptions):
            default_execution_options = execution_options
        # If None, all datasets will use the default ingest options.
        self._execution_options: Dict[str, "ExecutionOptions"] = defaultdict(
            lambda: copy.deepcopy(default_execution_options)
        )
        if isinstance(execution_options, dict):
            self._execution_options.update(execution_options)

        self._enable_shard_locality = enable_shard_locality

        self._num_train_cpus = 0.0
        self._num_train_gpus = 0.0

    def _get_dataset_split_config(self, dataset_name: str) -> SplitConfig:
        """Return the effective split config for a dataset."""
        split_config = self._dataset_split_configs.get(dataset_name)
        if split_config is not None:
            enable_shard_locality = split_config.enable_shard_locality
            if enable_shard_locality is None:
                enable_shard_locality = self._enable_shard_locality
            return SplitConfig(
                equal=split_config.equal,
                enable_shard_locality=enable_shard_locality,
            )

        return SplitConfig(
            equal=dataset_name not in set(self._unequal_split_datasets),
            enable_shard_locality=self._enable_shard_locality,
        )

    def set_train_total_resources(self, num_train_cpus: float, num_train_gpus: float):
        """Set the total number of CPUs and GPUs used by training.

        If CPU or GPU resource limits are not set, they will be set to the
        total cluster resources minus the resources used by training.
        """
        # TODO: We may also include other resources besides CPU and GPU.
        self._num_train_cpus = num_train_cpus
        self._num_train_gpus = num_train_gpus

    def _get_execution_options(self, dataset_name: str) -> "ExecutionOptions":
        """Return a copy of the configured execution options for a given dataset name."""
        return copy.deepcopy(self._execution_options[dataset_name])

    @DeveloperAPI
    def configure(
        self,
        datasets: Dict[str, "Dataset"],
        world_size: int,
        worker_handles: Optional[List[ActorHandle]],
        worker_node_ids: Optional[List["NodeIdStr"]],
        **kwargs,
    ) -> List[Dict[str, "DataIterator"]]:
        """Configure how Train datasets should be assigned to workers.

        Args:
            datasets: The datasets dict passed to Train by the user.
            world_size: The number of Train workers in total.
            worker_handles: The actor handles of the Train workers.
            worker_node_ids: The node ids of the Train workers.
            kwargs: Forwards compatibility placeholder.

        Returns:
            A list of dataset splits for each worker. The size of the list must be
            equal to `world_size`. Each element of the list contains the assigned
            `DataIterator` instances by name for the worker.
        """
        from ray.data._internal.execution.interfaces.execution_options import (
            ExecutionResources,
        )

        output = [{} for _ in range(world_size)]

        for dataset_name, dataset in datasets.items():
            if dataset.name is None:
                dataset.set_name(dataset_name)

        if self._datasets_to_split == "all":
            datasets_to_split = set(datasets.keys())
        else:
            datasets_to_split = set(self._datasets_to_split)

        for name, ds in datasets.items():
            execution_options = self._get_execution_options(name)

            if execution_options.is_resource_limits_default():
                if not self._scaling_policy_reserves_train_resources():
                    execution_options.exclude_resources = (
                        execution_options.exclude_resources.add(
                            ExecutionResources(
                                cpu=self._num_train_cpus, gpu=self._num_train_gpus
                            )
                        )
                    )

            ds = ds.copy(ds)
            ds.context.execution_options = execution_options

            if name in datasets_to_split:
                split_config = self._get_dataset_split_config(name)
                locality_hints = (
                    worker_node_ids if split_config.enable_shard_locality else None
                )
                for i, split in enumerate(
                    ds.streaming_split(
                        world_size,
                        equal=split_config.equal,
                        locality_hints=locality_hints,
                    )
                ):
                    output[i][name] = split
            else:
                for i in range(world_size):
                    output[i][name] = ds.iterator()

        return output

    @staticmethod
    def _is_v2_autoscaler() -> bool:
        """Check if Ray Data is set to use the V2 cluster autoscaler."""
        from ray.data._internal.cluster_autoscaler import (
            CLUSTER_AUTOSCALER_ENV_KEY,
            DEFAULT_CLUSTER_AUTOSCALER_VERSION,
            ClusterAutoscalerVersion,
        )

        return (
            os.environ.get(
                CLUSTER_AUTOSCALER_ENV_KEY, DEFAULT_CLUSTER_AUTOSCALER_VERSION
            )
            == ClusterAutoscalerVersion.V2
        )

    @classmethod
    def _scaling_policy_reserves_train_resources(cls) -> bool:
        """True iff Ray Train V2's ScalingPolicy will register training resources
        with the AutoscalingCoordinator for this run.

        Only the combination of Ray Train V2 AND the V2 cluster autoscaler wires
        this registration end-to-end: the V2 ScalingPolicy registers training
        resources with the AutoscalingCoordinator, which adjusts each executor's
        share accordingly. Under Ray Train V1 there is no scaling policy, and
        under the V1 cluster autoscaler the coordinator is not consulted.
        In either of those cases, DataConfig must reserve training resources via
        exclude_resources instead to avoid Ray Data over-booking the cluster.
        """
        from ray.train.v2._internal.constants import is_v2_enabled

        return is_v2_enabled() and cls._is_v2_autoscaler()

    @staticmethod
    def default_ingest_options() -> "ExecutionOptions":
        """The default Ray Data options used for data ingest.

        By default, configurations are carried over from what is already set
        in DataContext.
        """
        from ray.data import ExecutionOptions
        from ray.data.context import DataContext

        ctx = DataContext.get_current()
        return ExecutionOptions(
            resource_limits=ctx.execution_options.resource_limits,
            exclude_resources=ctx.execution_options.exclude_resources,
            preserve_order=ctx.execution_options.preserve_order,
            verbose_progress=ctx.execution_options.verbose_progress,
        )
