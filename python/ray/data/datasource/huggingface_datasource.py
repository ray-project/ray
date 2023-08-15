from typing import TYPE_CHECKING, Iterable, List, Optional, Union

from ray.data._internal.dataset_logger import DatasetLogger
from ray.data._internal.util import _check_pyarrow_version
from ray.data.block import Block, BlockMetadata
from ray.data.datasource import Datasource, Reader, ReadTask
from ray.util.annotations import DeveloperAPI

logger = DatasetLogger(__name__)

if TYPE_CHECKING:
    import datasets


@DeveloperAPI
class HuggingFaceDatasource(Datasource):
    """TODO(scott)

    Examples:
        # >>> import ray
    """

    def create_reader(
        self,
        dataset: Union["datasets.Dataset", "datasets.IterableDataset"],
    ) -> "_HuggingFaceDatasourceReader":
        return _HuggingFaceDatasourceReader(dataset)


class _HuggingFaceDatasourceReader(Reader):
    def __init__(
        self,
        dataset: Union["datasets.Dataset", "datasets.IterableDataset"],
        batch_size: int = 4096 * 64,
    ):
        self._dataset = dataset
        self._batch_size = batch_size

    def estimate_inmemory_data_size(self) -> Optional[int]:
        return self._dataset.dataset_size

    def get_read_tasks(
        self,
        parallelism: int,
    ) -> List[ReadTask]:
        _check_pyarrow_version()
        import pyarrow

        # try:
        #     from datasets.distributed import split_dataset_by_node
        # except ModuleNotFoundError as e:
        #     print(e)
        #     logger.get_logger().warning(
        #         "To read large Hugging Face Datasets efficiently, please install "
        #         "HuggingFace datasets>=2.9.0`."
        #     )
        def _read_shard(dataset: "datasets.IterableDataset") -> Iterable[Block]:
            for batch in dataset.with_format("arrow").iter(batch_size=self._batch_size):
                block = pyarrow.Table.from_pydict(batch)
                yield block

        schema = None  # self._dataset.features
        read_tasks: List[ReadTask] = []
        # TODO(scott): figure out how to properly shard HF dataset, so we can
        # use multiple ReadTasks.
        parallelism = 1
        for i in range(parallelism):
            # ds_shard = hf_dataset_shards[i]
            meta = BlockMetadata(
                num_rows=None,
                size_bytes=None,
                schema=schema,
                input_files=None,
                exec_stats=None,
            )
            read_tasks.append(
                ReadTask(
                    lambda shard=self._dataset: _read_shard(shard),
                    meta,
                )
            )

        return read_tasks
