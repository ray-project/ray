from typing import Optional, Dict, List, TYPE_CHECKING


if TYPE_CHECKING:
    from ray.air.config import DatasetConfig
    from ray.data import (
        Dataset,
        DataIterator,
        ExecutionOptions,
        ExecutionResources,
        NodeIdStr,
    )
    from ray.data.preprocessor import Preprocessor


def default_ingest_options() -> ExecutionOptions:
    """The default Ray Data options used for data ingest.

    We enable output locality, which means that Ray Data will try to place tasks on
    the node the data will be consumed. We also set the object store memory limit to a
    fixed smaller value, to avoid using too much memory per Train worker.
    """

    return ExecutionOptions(
        locality_with_output=True,
        resource_limits=ExecutionResources(object_store_memory=2e9),
    )


class DataConfig:
    def __init__(
        self,
        datasets_to_split: Optional[List[str]] = None,
        execution_options: ExecutionOptions = default_ingest_options,
    ):
        self._datasets_to_split = datasets_to_split or []
        self._execution_options = execution_options

    def configure(
        self,
        datasets: Dict[str, Dataset],
        world_size: int,
        worker_node_ids: Optional[List[NodeIdStr]],
    ) -> Dict[int, Dict[str, DataIterator]]:
        output = {i: {} for i in range(world_size)}

        for name, ds in datasets.items():
            ds = ds.copy(ds)
            ds.context.execution_options = self._execution_options
            if name in self._datasets_to_splits:
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


class LegacyDataConfigWrapper(DataConfig):
    def __init__(self, config: Dict[str, DatasetConfig]):
        self._config = config

    def _legacy_preprocessing(
        self, datasets: Dict[str, Dataset], preprocessor: Optional[Preprocessor]
    ) -> Dict[str, Dataset]:
        ...
