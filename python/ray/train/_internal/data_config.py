import copy
from collections import defaultdict
from typing import Dict, List, Literal, Optional, Union

import ray
from ray.actor import ActorHandle
from ray.data import DataIterator, Dataset, ExecutionOptions, NodeIdStr
from ray.data._internal.execution.interfaces.execution_options import ExecutionResources
from ray.util.annotations import DeveloperAPI, PublicAPI


@PublicAPI(stability="stable")
class DataConfig:
    """Class responsible for configuring Train dataset preprocessing.

    For advanced use cases, this class can be subclassed and the `configure()` method
    overriden for custom data preprocessing.
    """

    def __init__(
        self,
        datasets_to_split: Union[Literal["all"], List[str]] = "all",
        execution_options: Optional[
            Union[ExecutionOptions, Dict[str, ExecutionOptions]]
        ] = None,
        enable_shard_locality: bool = True,
    ):
        """Construct a DataConfig.

        Args:
            datasets_to_split: Specifies which datasets should be split among workers.
                Can be set to "all" or a list of dataset names. Defaults to "all",
                i.e. split all datasets.
            execution_options: The execution options to pass to Ray Data. Can be either:
                1. A single ExecutionOptions object that is applied to all datasets.
                2. A dict mapping dataset names to ExecutionOptions. If a dataset name
                is not in the dict, it defaults to ``DataConfig.default_ingest_options()``.
                By default, the options are optimized for data ingest. When overriding,
                base your options off ``DataConfig.default_ingest_options()``.
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

        default_execution_options = DataConfig.default_ingest_options()
        if isinstance(execution_options, ExecutionOptions):
            default_execution_options = execution_options
        # If None, all datasets will use the default ingest options.
        self._execution_options: Dict[str, ExecutionOptions] = defaultdict(
            lambda: copy.deepcopy(default_execution_options)
        )
        if isinstance(execution_options, dict):
            self._execution_options.update(execution_options)

        self._enable_shard_locality = enable_shard_locality

        self._num_train_cpus = 0.0
        self._num_train_gpus = 0.0

    def set_train_total_resources(self, num_train_cpus: float, num_train_gpus: float):
        """Set the total number of CPUs and GPUs used by training.

        If CPU or GPU resource limits are not set, they will be set to the
        total cluster resources minus the resources used by training.
        """
        # TODO: We may also include other resources besides CPU and GPU.
        self._num_train_cpus = num_train_cpus
        self._num_train_gpus = num_train_gpus

    def _get_execution_options(self, dataset_name: str) -> ExecutionOptions:
        """Return a copy of the configured execution options for a given dataset name."""
        return copy.deepcopy(self._execution_options[dataset_name])

    @DeveloperAPI
    def configure(
        self,
        datasets: Dict[str, Dataset],
        world_size: int,
        worker_handles: Optional[List[ActorHandle]],
        worker_node_ids: Optional[List[NodeIdStr]],
        **kwargs,
    ) -> List[Dict[str, DataIterator]]:
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
            execution_options = self._get_execution_options(name)

            if execution_options.is_resource_limits_default():
                # If "resource_limits" is not overriden by the user,
                # add training-reserved resources to Data's exclude_resources.
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
    def default_ingest_options() -> ExecutionOptions:
        """The default Ray Data options used for data ingest.

        By default, configurations are carried over from what is already set
        in DataContext.
        """
        ctx = ray.data.DataContext.get_current()
        return ExecutionOptions(
            # TODO(hchen): Re-enable `locality_with_output` by default after fixing
            # https://github.com/ray-project/ray/issues/40607
            locality_with_output=ctx.execution_options.locality_with_output,
            resource_limits=ctx.execution_options.resource_limits,
            exclude_resources=ctx.execution_options.exclude_resources,
            preserve_order=ctx.execution_options.preserve_order,
            verbose_progress=ctx.execution_options.verbose_progress,
        )
