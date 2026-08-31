import copy
from typing import TYPE_CHECKING, Dict, List, Literal, Optional, Union

from ray.actor import ActorHandle
from ray.util.annotations import DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    from ray.data import DataIterator, Dataset, ExecutionOptions, NodeIdStr


@PublicAPI(stability="stable")
class DataConfig:
    """Class responsible for configuring Train dataset preprocessing.

    For advanced use cases, this class can be subclassed and the `configure()` method
    overridden for custom data preprocessing.
    """

    def __init__(
        self,
        datasets_to_split: Union[Literal["all"], List[str]] = "all",
        execution_options: Optional[
            Union["ExecutionOptions", Dict[str, "ExecutionOptions"]]
        ] = None,
        enable_shard_locality: bool = True,
    ):
        """Construct a DataConfig.

        Args:
            datasets_to_split: Specifies which datasets should be split among workers.
                Can be set to "all" or a list of dataset names. Defaults to "all",
                i.e. split all datasets.
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
        """
        if isinstance(datasets_to_split, list) or datasets_to_split == "all":
            self._datasets_to_split = datasets_to_split
        else:
            raise TypeError(
                "`datasets_to_split` should be a 'all' or a list of strings of "
                "dataset names. Received "
                f"{type(datasets_to_split).__name__} with value {datasets_to_split}."
            )

        self._user_execution_options = execution_options
        self._enable_shard_locality = enable_shard_locality

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

        locality_hints = worker_node_ids if self._enable_shard_locality else None
        for name, ds in datasets.items():
            ds = ds.copy(ds)
            ds.context.execution_options = self._resolve_execution_options(name)

            if name in datasets_to_split:
                for i, split in enumerate(
                    ds.streaming_split(
                        world_size, equal=True, locality_hints=locality_hints
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
