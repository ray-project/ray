import copy
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Literal, Optional, Union

from ray.actor import ActorHandle
from ray.util.annotations import DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    from ray.data import DataIterator, Dataset, ExecutionOptions, NodeIdStr
    from ray.data.checkpoint.interfaces import DatasetCheckpointConfig


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
        dataset_checkpoint_configs: Optional[
            Dict[str, "DatasetCheckpointConfig"]
        ] = None,
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
            execution_options: Optional Ray Data execution options. When set, they are
                applied to dataset shards. When ``None`` (the default), Train applies
                :meth:`default_ingest_options` to each dataset shard. Can be either:

                1. A single ExecutionOptions object applied to all datasets.
                2. A dict mapping dataset names to ExecutionOptions for per-dataset
                   overrides. Datasets not present in the dict use
                   :meth:`default_ingest_options`.

                NOTE: For exclude_resources and resource_limits, those options only affect
                Ray Data *after* train performs its cluster resource reservation.
                So if you specify exclude_resources, it will exclude the resources
                from data's reservation, *not* train's reservation.
            enable_shard_locality: If true, dataset sharding across Train workers will
                consider locality to minimize cross-node data transfer. Enabled by default.
            dataset_checkpoint_configs: [Experimental] A dictionary of dataset names to
                :class:`~ray.train.DatasetCheckpointConfig`. Providing the configs
                enables checkpointing the data iterator state and mid-epoch
                resumption for the specified datasets. Only supported with
                Ray Train V2.
        """
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

        self._user_execution_options = execution_options
        self._enable_shard_locality = enable_shard_locality
        self.dataset_checkpoint_configs: Dict[str, "DatasetCheckpointConfig"] = (
            dataset_checkpoint_configs or {}
        )

        # TODO: [unsharded-data-ckpt] Checkpointing only supports sharded datasets
        # for now because _supports_checkpointing only returns True for
        # datasets ending in a `streaming_split` operator.
        if self._datasets_to_split != "all":
            unsharded_datasets = set(self.dataset_checkpoint_configs.keys()) - set(
                self._datasets_to_split
            )
            if unsharded_datasets:
                raise NotImplementedError(
                    "Dataset checkpointing is not currently supported for unsharded datasets. "
                    f"Please add {unsharded_datasets} to `DataConfig.datasets_to_split` "
                    "or remove the `DataConfig.dataset_checkpoint_configs` key for "
                    "these unsharded datasets. "
                )

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

    def _get_user_execution_options(
        self, dataset_name: str
    ) -> Optional["ExecutionOptions"]:
        """Return user-provided execution options for a dataset, if any."""
        if self._user_execution_options is None:
            return None
        if isinstance(self._user_execution_options, dict):
            if dataset_name not in self._user_execution_options:
                return None
            return self._user_execution_options[dataset_name]
        return self._user_execution_options

    def _resolve_execution_options(self, dataset_name: str) -> "ExecutionOptions":
        """Return a deep copy of the effective execution options for a dataset shard.

        Returns a deep copy so callers (including subclasses that override
        ``configure``) can mutate the result without aliasing the driver
        ``DataContext`` or the user-supplied ``ExecutionOptions`` object.
        """
        return copy.deepcopy(
            self._get_user_execution_options(dataset_name)
            or self.default_ingest_options()
        )

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
            **kwargs: Forwards compatibility placeholder.

        Returns:
            A list of dataset splits for each worker. The size of the list must be
            equal to `world_size`. Each element of the list contains the assigned
            `DataIterator` instances by name for the worker.
        """
        output = [{} for _ in range(world_size)]

        for dataset_name, dataset in datasets.items():
            if dataset.name is None:
                dataset.set_name(dataset_name)

        if self._datasets_to_split == "all":
            datasets_to_split = set(datasets.keys())
        else:
            datasets_to_split = set(self._datasets_to_split)

        for name, ds in datasets.items():
            ds = ds.copy(ds)
            ds.context.execution_options = self._resolve_execution_options(name)

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
    def default_ingest_options() -> "ExecutionOptions":
        """The default Ray Data options used for data ingest.

        By default, configurations are carried over from what is already set
        in DataContext.

        Returns:
            ``ExecutionOptions`` with ``resource_limits``, ``exclude_resources``,
            ``preserve_order``, and ``verbose_progress`` copied from the current
            ``DataContext``.
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
