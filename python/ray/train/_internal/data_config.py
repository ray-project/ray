from typing import Dict, List, Literal, Optional, Union

import ray
from ray.actor import ActorHandle
from ray.data import DataIterator, Dataset, ExecutionOptions, NodeIdStr
from ray.data.preprocessor import Preprocessor

# TODO(justinvyu): Fix the circular import error
from ray.train.constants import TRAIN_DATASET_KEY  # noqa
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
        execution_options: Optional[ExecutionOptions] = None,
    ):
        """Construct a DataConfig.

        Args:
            datasets_to_split: Specifies which datasets should be split among workers.
                Can be set to "all" or a list of dataset names. Defaults to "all",
                i.e. split all datasets.
            execution_options: The execution options to pass to Ray Data. By default,
                the options will be optimized for data ingest. When overriding this,
                base your options off of `DataConfig.default_ingest_options()`.
        """
        if isinstance(datasets_to_split, list) or datasets_to_split == "all":
            self._datasets_to_split = datasets_to_split
        else:
            raise TypeError(
                "`datasets_to_split` should be a 'all' or a list of strings of "
                "dataset names. Received "
                f"{type(datasets_to_split).__name__} with value {datasets_to_split}."
            )

        self._execution_options: ExecutionOptions = (
            execution_options or DataConfig.default_ingest_options()
        )

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

        if self._datasets_to_split == "all":
            datasets_to_split = set(datasets.keys())
        else:
            datasets_to_split = set(self._datasets_to_split)

        for name, ds in datasets.items():
            ds = ds.copy(ds)
            ds.context.execution_options = self._execution_options
            if name in datasets_to_split:
                for i, split in enumerate(
                    ds.streaming_split(
                        world_size, equal=True, locality_hints=worker_node_ids
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

        By default, output locality is enabled, which means that Ray Data will try to
        place tasks on the node the data is consumed. The remaining configurations are
        carried over from what is already set in DataContext.
        """
        ctx = ray.data.DataContext.get_current()
        return ExecutionOptions(
            locality_with_output=True,
            resource_limits=ctx.execution_options.resource_limits,
            preserve_order=ctx.execution_options.preserve_order,
            verbose_progress=ctx.execution_options.verbose_progress,
        )

    def _legacy_preprocessing(
        self, datasets: Dict[str, Dataset], preprocessor: Optional[Preprocessor]
    ) -> Dict[str, Dataset]:
        """Legacy hook for backwards compatiblity.

        This will be removed in the future.
        """
        return datasets  # No-op for non-legacy configs.
