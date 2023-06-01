from typing import Optional, Dict, List

from ray.actor import ActorHandle
from ray.train.constants import TRAIN_DATASET_KEY
from ray.train._internal.dataset_spec import DataParallelIngestSpec
from ray.util.annotations import PublicAPI, DeveloperAPI
from ray.air.config import DatasetConfig
from ray.data import (
    Dataset,
    DataIterator,
    ExecutionOptions,
    ExecutionResources,
    NodeIdStr,
)
from ray.data.preprocessor import Preprocessor


@PublicAPI
class DataConfig:
    """Class responsible for configuring Train dataset preprocessing.

    For advanced use cases, this class can be subclassed and the `configure()` method
    overriden for custom data preprocessing.
    """

    def __init__(
        self,
        datasets_to_split: Optional[List[str]] = None,
        execution_options: Optional[ExecutionOptions] = None,
    ):
        """Construct a DataConfig.

        Args:
            datasets_to_split: The list of dataset names to split between workers.
                By default, only the "train" dataset will be split.
            execution_options: The execution options to pass to Ray Data. By default,
                the options will be optimized for data ingest. When overriding this,
                base your options off of `DataConfig.default_ingest_options()`.
        """
        self._datasets_to_split: List[str] = (
            datasets_to_split if datasets_to_split is not None else [TRAIN_DATASET_KEY]
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
        output = [{} for i in range(world_size)]

        for name, ds in datasets.items():
            ds = ds.copy(ds)
            ds.context.execution_options = self._execution_options
            if name in self._datasets_to_split:
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

        We enable output locality, which means that Ray Data will try to place tasks on
        the node the data will be consumed. We also set the object store memory limit
        to a fixed smaller value, to avoid using too much memory per Train worker.
        """
        return ExecutionOptions(
            locality_with_output=True,
            resource_limits=ExecutionResources(object_store_memory=2e9),
        )

    def _legacy_preprocessing(
        self, datasets: Dict[str, Dataset], preprocessor: Optional[Preprocessor]
    ) -> Dict[str, Dataset]:
        """Legacy hook for backwards compatiblity.

        This will be removed in the future.
        """
        return datasets  # No-op for non-legacy configs.


class _LegacyDataConfigWrapper(DataConfig):
    """Backwards-compatibility wrapper for the legacy dict-based datasets config.

    This will be removed in the future.
    """

    def __init__(
        self,
        cls_config: Dict[str, DatasetConfig],
        user_config: Dict[str, DatasetConfig],
        datasets: Dict[str, Dataset],
    ):
        self._dataset_config = DatasetConfig.validated(
            DatasetConfig.merge(cls_config, user_config), datasets
        )
        self._ingest_spec = DataParallelIngestSpec(
            dataset_config=self._dataset_config,
        )

    def configure(
        self,
        datasets: Dict[str, Dataset],
        world_size: int,
        worker_handles: Optional[List[ActorHandle]],
        worker_node_ids: Optional[List[NodeIdStr]],
        **kwargs,
    ) -> Dict[int, Dict[str, DataIterator]]:
        return self._ingest_spec.get_dataset_shards(worker_handles)

    def _legacy_preprocessing(
        self, datasets: Dict[str, Dataset], preprocessor: Optional[Preprocessor]
    ) -> Dict[str, Dataset]:
        return self._ingest_spec.preprocess_datasets(preprocessor, datasets)
